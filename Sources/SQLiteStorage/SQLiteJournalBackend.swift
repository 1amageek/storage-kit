import DatabaseTypes
import StorageKit
import Synchronization

/// Native snapshot access without a retained transaction or mutation flush.
struct SQLiteJournalBackend: TransactionJournalBackendAccess {
    let rootIdentifier: UInt64
    let identifier: UInt64
    let coordinator: SQLiteTransactionCoordinator
    let connection: SQLiteConnectionHandle
    let lifetime: SQLiteStorageLifetime
    let failure: Failure

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        do {
            try requireActive()
            return try await coordinator.readValue(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                writes: [],
                key: key,
                maximumByteCount: maximumByteCount
            )
        } catch {
            record(error, operation: .read)
            throw error
        }
    }

    private func record(_ error: any Error, operation: StorageOperation) {
        if let storage = error as? StorageError,
           storage.isPointReadValueTooLarge || storage.code == .resourceUnavailable { return }
        let storage = error as? StorageError ?? StorageError(
            code: .backendFailure, operation: operation, backend: .sqlite,
            message: "SQLite journal snapshot read failed", underlyingDescription: String(describing: error)
        )
        failure.record(storage)
    }

    final class Failure: Sendable {
        private let state = Mutex<StorageError?>(nil)
        var error: StorageError? { state.withLock { $0 } }
        func record(_ error: StorageError) {
            state.withLock { if $0 == nil { $0 = error } }
        }
    }

    func rangeCursor(
        in range: TransactionJournalRange,
        reverse: Bool,
        snapshot: Bool,
        pageSize: Int,
        maximumByteCount: Int
    ) -> KeyValueCursor {
        KeyValueCursor(consuming: Rows(
            backend: self,
            plan: SQLiteRangeScanPlan(
                begin: Self.boundary(range.lower, lower: true),
                end: Self.boundary(range.upper, lower: false),
                limit: 0,
                reverse: reverse,
                maximumByteCount: maximumByteCount
            )
        ))
    }

    private static func boundary(
        _ boundary: TransactionJournalBoundary,
        lower: Bool
    ) -> SQLRangeBoundary? {
        switch boundary {
        case .unbounded: nil
        case .inclusive(let key): .direct(op: lower ? ">=" : "<=", key: key)
        case .exclusive(let key): .direct(op: lower ? ">" : "<", key: key)
        }
    }

    private func requireActive() throws {
        guard !lifetime.isClosed else {
            throw StorageError(
                code: .invalidOperation,
                operation: .read,
                backend: .sqlite,
                message: "SQLite storage engine is closed"
            )
        }
    }

    private struct Rows: TransactionRangeResult {
        let backend: SQLiteJournalBackend
        let plan: SQLiteRangeScanPlan

        func makeCursor() -> Cursor {
            Cursor(backend: backend, plan: plan)
        }

        struct Cursor: TransactionRangeCursor {
            let backend: SQLiteJournalBackend
            let plan: SQLiteRangeScanPlan
            private let lifetime: CursorLifetime
            private var started = false
            private var done = false

            init(backend: SQLiteJournalBackend, plan: SQLiteRangeScanPlan) {
                self.backend = backend
                self.plan = plan
                self.lifetime = CursorLifetime(connection: backend.connection)
            }

            mutating func next() async throws -> (ByteString, ByteString)? {
                guard !done else { return nil }
                do {
                    try backend.requireActive()
                    let row: (ByteString, ByteString)?
                    if !started {
                        started = true
                        let opened = try await backend.coordinator.openRange(
                            rootIdentifier: backend.rootIdentifier,
                            transactionIdentifier: backend.identifier,
                            writes: [],
                            plan: plan
                        )
                        lifetime.install(opened.cursorIdentifier)
                        row = opened.first
                    } else if let identifier = lifetime.identifier {
                        row = try await backend.coordinator.nextRange(
                            rootIdentifier: backend.rootIdentifier,
                            transactionIdentifier: backend.identifier,
                            cursorIdentifier: identifier,
                            maximumByteCount: plan.maximumByteCount
                        )
                    } else {
                        row = nil
                    }
                    if row == nil { close() }
                    return row
                } catch {
                    close()
                    backend.record(error, operation: .rangeRead)
                    throw error
                }
            }

            mutating func finish(isolation actor: isolated (any Actor)?) async throws {
                close()
            }

            private mutating func close() {
                lifetime.close()
                done = true
            }
        }
    }

    private final class CursorLifetime: Sendable {
        let connection: SQLiteConnectionHandle
        let state = Mutex<UInt64?>(nil)

        init(connection: SQLiteConnectionHandle) {
            self.connection = connection
        }

        var identifier: UInt64? { state.withLock { $0 } }

        func install(_ identifier: UInt64) {
            state.withLock { $0 = identifier }
        }

        func close() {
            let identifier = state.withLock { identifier in
                let current = identifier
                identifier = nil
                return current
            }
            if let identifier { connection.closeRangeCursor(identifier: identifier) }
        }

        deinit { close() }
    }
}
