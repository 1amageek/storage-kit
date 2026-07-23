import StorageKit

/// Owns one logical SQLite range iterator across copied iterator values.
/// Actor isolation protects the cursor state, while `advancing` rejects actor
/// reentrancy during an awaited SQLite operation instead of opening two cursors.
actor SQLiteRangeIteratorState {
    private var transaction: SQLiteStorageTransaction?
    private var plan: SQLiteRangeScanPlan?
    private var pendingError: StorageError?
    private var started = false
    private var advancing = false
    private var emitted = 0
    private var done = false
    private let lifetime = SQLiteRangeCursorLifetime()

    init(
        transaction: SQLiteStorageTransaction,
        plan: SQLiteRangeScanPlan
    ) {
        self.transaction = transaction
        self.plan = plan
    }

    init(error: StorageError) {
        self.pendingError = error
    }

    func next() async throws -> (Bytes, Bytes)? {
        guard !advancing else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .sqlite,
                message: "Concurrent next calls on one SQLite range iterator are not supported"
            )
        }
        advancing = true
        defer { advancing = false }
        return try await advance()
    }

    private func advance() async throws -> (Bytes, Bytes)? {
        guard !done else { return nil }
        if let pendingError {
            finish()
            throw pendingError
        }
        guard let transaction, let plan else {
            finish()
            return nil
        }

        do {
            if !started {
                started = true
                let opened = try await transaction.openRange(plan: plan)
                guard let first = opened.first else {
                    finish()
                    return nil
                }
                emitted = 1
                if plan.limit > 0, emitted >= plan.limit {
                    transaction.abandonRange(
                        registrationIdentifier:
                            opened.registrationIdentifier,
                        cursorIdentifier: opened.cursorIdentifier
                    )
                    finish()
                    return first
                }
                lifetime.install(
                    SQLiteRangeCursorLifetime.Payload(
                        transaction: transaction,
                        registrationIdentifier:
                            opened.registrationIdentifier,
                        cursorIdentifier: opened.cursorIdentifier
                    )
                )
                return first
            }

            guard let payload = lifetime.current() else {
                finish()
                return nil
            }
            let row = try await transaction.nextRange(
                registrationIdentifier: payload.registrationIdentifier,
                cursorIdentifier: payload.cursorIdentifier
            )
            if row != nil {
                emitted += 1
            }
            if let row, plan.limit > 0, emitted >= plan.limit {
                transaction.abandonRange(
                    registrationIdentifier:
                        payload.registrationIdentifier,
                    cursorIdentifier: payload.cursorIdentifier
                )
                lifetime.disarm()
                finish()
                return row
            }
            if row == nil {
                lifetime.disarm()
                finish()
            }
            return row
        } catch {
            if let payload = lifetime.current() {
                transaction.abandonRange(
                    registrationIdentifier:
                        payload.registrationIdentifier,
                    cursorIdentifier: payload.cursorIdentifier
                )
                lifetime.disarm()
            }
            finish()
            throw error
        }
    }

    func finish() {
        if let payload = lifetime.current() {
            payload.transaction.abandonRange(
                registrationIdentifier: payload.registrationIdentifier,
                cursorIdentifier: payload.cursorIdentifier
            )
            lifetime.disarm()
        }
        transaction = nil
        plan = nil
        pendingError = nil
        emitted = 0
        done = true
    }
}
