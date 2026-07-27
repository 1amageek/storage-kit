import DatabaseTypes
import StorageKit

/// Owns the FIFO lease for one SQLite connection and enforces the native
/// transaction/savepoint stack. No lock is held across a suspension point.
actor SQLiteTransactionCoordinator {
    private struct LeaseWaiter {
        let identifier: UInt64
        let continuation: CheckedContinuation<Void, any Error>
    }

    private let connection: SQLiteConnectionHandle
    private let lifetime: SQLiteStorageLifetime

    private var activeRootIdentifier: UInt64?
    private var transactionStack: [UInt64] = []
    private var waitingOrder: [UInt64] = []
    private var waitingHead: Int = 0
    private var waitingContinuations:
        [UInt64: [LeaseWaiter]] = [:]
    private var nextWaiterIdentifier: UInt64 = 1
    private var terminalTransactionIdentifiers: Set<UInt64> = []
    private var savepointBeginCount: UInt64 = 0
    private var savepointReleaseCount: UInt64 = 0
    private var savepointRollbackCount: UInt64 = 0
    private var fatalCleanupError: StorageError?

    init(
        connection: SQLiteConnectionHandle,
        lifetime: SQLiteStorageLifetime
    ) {
        self.connection = connection
        self.lifetime = lifetime
    }

    func beginRoot(identifier: UInt64) async throws {
        try await ensureRoot(identifier: identifier)
    }

    func beginChild(
        rootIdentifier: UInt64,
        parentIdentifier: UInt64,
        childIdentifier: UInt64,
        parentWrites: [SQLiteWriteOperation]
    ) async throws {
        try await ensureRoot(identifier: rootIdentifier)
        try requireActiveRoot(rootIdentifier)
        guard !terminalTransactionIdentifiers.contains(childIdentifier) else {
            throw invalidState(
                "Nested SQLite transaction is already terminal",
                operation: .beginTransaction
            )
        }
        if transactionStack.last == childIdentifier {
            guard parentWrites.isEmpty else {
                throw invalidState(
                    "Parent writes arrived after its child savepoint started",
                    operation: .write
                )
            }
            return
        }
        guard transactionStack.last == parentIdentifier else {
            throw invalidState(
                "SQLite nested transactions must start in strict LIFO order",
                operation: .beginTransaction
            )
        }

        try apply(parentWrites)
        try connection.execute(
            "SAVEPOINT \(savepointName(childIdentifier))",
            operation: .beginTransaction
        )
        savepointBeginCount &+= 1
        transactionStack.append(childIdentifier)
    }

    func readValue(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        writes: [SQLiteWriteOperation],
        key: ByteString
    ) async throws -> ByteString? {
        try await ensureRoot(identifier: rootIdentifier)
        try requireTopTransaction(
            rootIdentifier: rootIdentifier,
            transactionIdentifier: transactionIdentifier,
            operation: .read
        )
        try apply(writes)
        return try connection.get(key: key)
    }

    func readKey(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        writes: [SQLiteWriteOperation],
        plan: SQLiteKeySelectionPlan
    ) async throws -> ByteString? {
        try await ensureRoot(identifier: rootIdentifier)
        try requireTopTransaction(
            rootIdentifier: rootIdentifier,
            transactionIdentifier: transactionIdentifier,
            operation: .rangeRead
        )
        try apply(writes)
        return try connection.getKey(plan: plan)
    }

    func openRange(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        writes: [SQLiteWriteOperation],
        plan: SQLiteRangeScanPlan
    ) async throws -> (cursorIdentifier: UInt64, first: (ByteString, ByteString)?) {
        try await ensureRoot(identifier: rootIdentifier)
        try requireTopTransaction(
            rootIdentifier: rootIdentifier,
            transactionIdentifier: transactionIdentifier,
            operation: .rangeRead
        )
        try apply(writes)
        let cursorIdentifier = try connection.openRangeCursor(
            ownerTransactionIdentifier: transactionIdentifier,
            begin: plan.begin,
            end: plan.end,
            limit: plan.limit,
            reverse: plan.reverse
        )
        do {
            let first = try connection.nextRangeCursor(
                identifier: cursorIdentifier
            )
            return (cursorIdentifier, first)
        } catch {
            connection.closeRangeCursor(identifier: cursorIdentifier)
            throw error
        }
    }

    func nextRange(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        cursorIdentifier: UInt64
    ) throws -> (ByteString, ByteString)? {
        try requireTopTransaction(
            rootIdentifier: rootIdentifier,
            transactionIdentifier: transactionIdentifier,
            operation: .rangeRead
        )
        return try connection.nextRangeCursor(identifier: cursorIdentifier)
    }

    func compact(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        writes: [SQLiteWriteOperation],
        maximumWorkUnits: UInt64
    ) async throws -> SQLiteIncrementalCompactionMetrics {
        try await ensureRoot(identifier: rootIdentifier)
        try requireTopTransaction(
            rootIdentifier: rootIdentifier,
            transactionIdentifier: transactionIdentifier,
            operation: .execute
        )
        try apply(writes)

        let autoVacuumMode = try connection.pragmaInt64("auto_vacuum")
        guard autoVacuumMode == 2 else {
            throw DatabaseStorageCompactionError.unsupportedConfiguration(
                feature: "sqlite.auto_vacuum.incremental",
                actualValue: autoVacuumMode
            )
        }
        return try connection.runIncrementalVacuum(
            maximumPages: maximumWorkUnits
        )
    }

    func commitRoot(
        identifier: UInt64,
        writes: [SQLiteWriteOperation]
    ) async -> StorageError? {
        do {
            try await ensureRoot(identifier: identifier)
            try requireRootTerminalPosition(identifier, operation: .commit)
            connection.closeRangeCursors(
                ownerTransactionIdentifier: identifier
            )
            try apply(writes)
            try connection.execute("COMMIT", operation: .commit)
            finishActiveRoot(identifier: identifier)
            return nil
        } catch {
            let commitError = map(error, operation: .commit)
            let rollbackError = rollbackActiveRootIfNeeded(identifier: identifier)
            let terminalError: StorageError
            if let rollbackError {
                terminalError = StorageError(
                    code: .backendFailure,
                    operation: .rollback,
                    backend: .sqlite,
                    message: "SQLite commit failed and rollback also failed",
                    underlyingDescription:
                        "commit=\(commitError); rollback=\(rollbackError)"
                )
                fatalCleanupError = terminalError
            } else {
                terminalError = commitError
            }
            finishActiveRoot(identifier: identifier)
            return terminalError
        }
    }

    func cancelRoot(identifier: UInt64) -> StorageError? {
        if activeRootIdentifier == identifier {
            let rollbackError = rollbackActiveRootIfNeeded(
                identifier: identifier
            )
            if let rollbackError {
                fatalCleanupError = rollbackError
            }
            finishActiveRoot(identifier: identifier)
            return rollbackError
        }

        if let continuations = waitingContinuations.removeValue(
            forKey: identifier
        ) {
            terminalTransactionIdentifiers.insert(identifier)
            let error = cancelledError(operation: .cancel)
            for waiter in continuations {
                waiter.continuation.resume(throwing: error)
            }
            return nil
        }

        terminalTransactionIdentifiers.insert(identifier)
        return nil
    }

    /// Rolls back a root transaction whose owner was released without an
    /// explicit terminal operation. A rollback failure poisons this coordinator
    /// so later work cannot be reported as successful on an uncertain database.
    func abandonRoot(identifier: UInt64) {
        if activeRootIdentifier == identifier {
            let rollbackError = rollbackActiveRootIfNeeded(
                identifier: identifier
            )
            if let rollbackError {
                fatalCleanupError = rollbackError
            }
            finishActiveRoot(identifier: identifier)
            return
        }

        if let continuations = waitingContinuations.removeValue(
            forKey: identifier
        ) {
            terminalTransactionIdentifiers.insert(identifier)
            let error = cancelledError(operation: .cancel)
            for waiter in continuations {
                waiter.continuation.resume(throwing: error)
            }
            return
        }

        terminalTransactionIdentifiers.insert(identifier)
    }

    func commitChild(
        rootIdentifier: UInt64,
        childIdentifier: UInt64,
        writes: [SQLiteWriteOperation]
    ) -> SQLiteChildTerminalOutcome {
        do {
            try requireTopTransaction(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: childIdentifier,
                operation: .commit
            )
            connection.closeRangeCursors(
                ownerTransactionIdentifier: childIdentifier
            )
            try apply(writes)
            try connection.execute(
                "RELEASE SAVEPOINT \(savepointName(childIdentifier))",
                operation: .commit
            )
            savepointReleaseCount &+= 1
            transactionStack.removeLast()
            terminalTransactionIdentifiers.insert(childIdentifier)
            return SQLiteChildTerminalOutcome(
                error: nil,
                parentResumed: true
            )
        } catch {
            let commitError = map(error, operation: .commit)
            return finishFailedChild(
                rootIdentifier: rootIdentifier,
                childIdentifier: childIdentifier,
                originalError: commitError
            )
        }
    }

    func cancelChild(
        rootIdentifier: UInt64,
        parentIdentifier: UInt64,
        childIdentifier: UInt64
    ) -> SQLiteChildTerminalOutcome {
        if terminalTransactionIdentifiers.contains(childIdentifier) {
            return SQLiteChildTerminalOutcome(
                error: nil,
                parentResumed: true
            )
        }

        guard activeRootIdentifier == rootIdentifier else {
            terminalTransactionIdentifiers.insert(childIdentifier)
            return SQLiteChildTerminalOutcome(
                error: nil,
                parentResumed: true
            )
        }
        if transactionStack.last == parentIdentifier {
            terminalTransactionIdentifiers.insert(childIdentifier)
            return SQLiteChildTerminalOutcome(
                error: nil,
                parentResumed: true
            )
        }
        guard transactionStack.last == childIdentifier else {
            return SQLiteChildTerminalOutcome(
                error: invalidState(
                    "SQLite nested transactions must cancel in strict LIFO order",
                    operation: .cancel
                ),
                parentResumed: false
            )
        }

        connection.closeRangeCursors(
            ownerTransactionIdentifier: childIdentifier
        )
        do {
            try rollbackSavepoint(identifier: childIdentifier)
            transactionStack.removeLast()
            terminalTransactionIdentifiers.insert(childIdentifier)
            return SQLiteChildTerminalOutcome(
                error: nil,
                parentResumed: true
            )
        } catch {
            return SQLiteChildTerminalOutcome(
                error: map(error, operation: .rollback),
                parentResumed: false
            )
        }
    }

    func shutdown() {
        let error = closedError(operation: .close)
        for continuations in waitingContinuations.values {
            for waiter in continuations {
                waiter.continuation.resume(throwing: error)
            }
        }
        waitingContinuations.removeAll(keepingCapacity: false)
        waitingOrder.removeAll(keepingCapacity: false)
        waitingHead = 0
        for identifier in transactionStack {
            terminalTransactionIdentifiers.insert(identifier)
        }
        transactionStack.removeAll(keepingCapacity: false)
        activeRootIdentifier = nil
    }

    var leaseInstrumentation: SQLiteLeaseInstrumentation {
        SQLiteLeaseInstrumentation(
            hasActiveRoot: activeRootIdentifier != nil,
            waitingRootCount: waitingContinuations.count,
            savepointBeginCount: savepointBeginCount,
            savepointReleaseCount: savepointReleaseCount,
            savepointRollbackCount: savepointRollbackCount
        )
    }

    func retireTerminalIdentifier(_ identifier: UInt64) {
        terminalTransactionIdentifiers.remove(identifier)
    }

    private func ensureRoot(identifier: UInt64) async throws {
        if let fatalCleanupError {
            throw fatalCleanupError
        }
        guard !lifetime.isClosed else {
            throw closedError(operation: .beginTransaction)
        }
        guard !terminalTransactionIdentifiers.contains(identifier) else {
            throw invalidState(
                "SQLite transaction is already terminal",
                operation: .beginTransaction
            )
        }
        if activeRootIdentifier == identifier {
            return
        }
        if activeRootIdentifier == nil, waitingContinuations.isEmpty {
            try startRoot(identifier: identifier)
            return
        }

        let waiterIdentifier = nextWaiterIdentifier
        let (nextWaiterIdentifier, waiterOverflow) =
            waiterIdentifier.addingReportingOverflow(1)
        guard !waiterOverflow else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .beginTransaction,
                backend: .sqlite,
                message: "SQLite lease waiter identifier space is exhausted"
            )
        }
        self.nextWaiterIdentifier = nextWaiterIdentifier

        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, any Error>) in
                if waitingContinuations[identifier] == nil {
                    waitingOrder.append(identifier)
                    waitingContinuations[identifier] = []
                }
                waitingContinuations[identifier]?.append(
                    LeaseWaiter(
                        identifier: waiterIdentifier,
                        continuation: continuation
                    )
                )
            }
        } onCancel: {
            Task {
                await self.cancelWaiter(
                    rootIdentifier: identifier,
                    waiterIdentifier: waiterIdentifier
                )
            }
        }

        guard !Task.isCancelled else {
            throw cancelledError(operation: .beginTransaction)
        }
        guard activeRootIdentifier == identifier else {
            throw invalidState(
                "SQLite transaction did not acquire its FIFO lease",
                operation: .beginTransaction
            )
        }
    }

    private func startRoot(identifier: UInt64) throws {
        if let fatalCleanupError {
            terminalTransactionIdentifiers.insert(identifier)
            throw fatalCleanupError
        }
        guard !lifetime.isClosed else {
            throw closedError(operation: .beginTransaction)
        }
        do {
            try connection.execute(
                "BEGIN IMMEDIATE",
                operation: .beginTransaction
            )
            activeRootIdentifier = identifier
            transactionStack = [identifier]
        } catch {
            terminalTransactionIdentifiers.insert(identifier)
            throw map(error, operation: .beginTransaction)
        }
    }

    private func startNextWaitingRoot() {
        while waitingHead < waitingOrder.count {
            let identifier = waitingOrder[waitingHead]
            waitingHead += 1
            guard let continuations = waitingContinuations.removeValue(
                forKey: identifier
            ) else {
                continue
            }
            do {
                try startRoot(identifier: identifier)
                compactWaitingOrderIfNeeded()
                for waiter in continuations {
                    waiter.continuation.resume()
                }
                return
            } catch {
                for waiter in continuations {
                    waiter.continuation.resume(throwing: error)
                }
            }
        }
        waitingOrder.removeAll(keepingCapacity: true)
        waitingHead = 0
    }

    private func compactWaitingOrderIfNeeded() {
        guard waitingHead >= 1_024,
              waitingHead >= waitingOrder.count / 2 else {
            return
        }
        waitingOrder.removeFirst(waitingHead)
        waitingHead = 0
    }

    private func cancelWaiter(
        rootIdentifier: UInt64,
        waiterIdentifier: UInt64
    ) {
        guard var waiters = waitingContinuations[rootIdentifier],
              let index = waiters.firstIndex(where: {
                  $0.identifier == waiterIdentifier
              }) else {
            return
        }
        let waiter = waiters.remove(at: index)
        if waiters.isEmpty {
            waitingContinuations[rootIdentifier] = nil
        } else {
            waitingContinuations[rootIdentifier] = waiters
        }
        waiter.continuation.resume(
            throwing: cancelledError(operation: .beginTransaction)
        )
    }

    private func apply(_ writes: [SQLiteWriteOperation]) throws {
        for write in writes {
            switch write {
            case .set(let key, let value):
                try connection.insertOrReplace(key: key, value: value)
            case .clear(let key):
                try connection.delete(key: key)
            case .clearRange(let begin, let end):
                try connection.deleteRange(begin: begin, end: end)
            case .atomic(let key, let parameter, let mutationType):
                switch try mutationType.apply(
                    to: connection.get(key: key),
                    param: parameter
                ) {
                case .set(let value):
                    try connection.insertOrReplace(key: key, value: value)
                case .clear:
                    try connection.delete(key: key)
                case .unchanged:
                    break
                }
            }
        }
    }

    private func requireActiveRoot(_ identifier: UInt64) throws {
        guard activeRootIdentifier == identifier else {
            throw invalidState(
                "SQLite transaction does not own the active connection lease",
                operation: .beginTransaction
            )
        }
    }

    private func requireTopTransaction(
        rootIdentifier: UInt64,
        transactionIdentifier: UInt64,
        operation: StorageOperation
    ) throws {
        try requireActiveRoot(rootIdentifier)
        guard !terminalTransactionIdentifiers.contains(transactionIdentifier),
              transactionStack.last == transactionIdentifier else {
            throw invalidState(
                "SQLite transaction is suspended by a nested transaction or is terminal",
                operation: operation
            )
        }
    }

    private func requireRootTerminalPosition(
        _ identifier: UInt64,
        operation: StorageOperation
    ) throws {
        try requireActiveRoot(identifier)
        guard transactionStack == [identifier] else {
            throw invalidState(
                "SQLite root transaction cannot finish while a nested savepoint is active",
                operation: operation
            )
        }
    }

    private func finishFailedChild(
        rootIdentifier: UInt64,
        childIdentifier: UInt64,
        originalError: StorageError
    ) -> SQLiteChildTerminalOutcome {
        guard activeRootIdentifier == rootIdentifier,
              transactionStack.last == childIdentifier else {
            return SQLiteChildTerminalOutcome(
                error: originalError,
                parentResumed: false
            )
        }
        connection.closeRangeCursors(
            ownerTransactionIdentifier: childIdentifier
        )
        do {
            try rollbackSavepoint(identifier: childIdentifier)
            transactionStack.removeLast()
            terminalTransactionIdentifiers.insert(childIdentifier)
            return SQLiteChildTerminalOutcome(
                error: originalError,
                parentResumed: true
            )
        } catch {
            let rollbackError = map(error, operation: .rollback)
            return SQLiteChildTerminalOutcome(
                error: StorageError(
                    code: .backendFailure,
                    operation: .rollback,
                    backend: .sqlite,
                    message: "SQLite nested commit failed and savepoint rollback also failed",
                    underlyingDescription:
                        "commit=\(originalError); rollback=\(rollbackError)"
                ),
                parentResumed: false
            )
        }
    }

    private func rollbackSavepoint(identifier: UInt64) throws {
        let name = savepointName(identifier)
        try connection.execute(
            "ROLLBACK TO SAVEPOINT \(name)",
            operation: .rollback
        )
        savepointRollbackCount &+= 1
        try connection.execute(
            "RELEASE SAVEPOINT \(name)",
            operation: .rollback
        )
        savepointReleaseCount &+= 1
    }

    private func rollbackActiveRootIfNeeded(
        identifier: UInt64
    ) -> StorageError? {
        guard activeRootIdentifier == identifier else {
            return nil
        }
        for transactionIdentifier in transactionStack {
            connection.closeRangeCursors(
                ownerTransactionIdentifier: transactionIdentifier
            )
        }
        do {
            try connection.execute("ROLLBACK", operation: .rollback)
            return nil
        } catch {
            return map(error, operation: .rollback)
        }
    }

    private func finishActiveRoot(identifier: UInt64) {
        guard activeRootIdentifier == identifier else {
            terminalTransactionIdentifiers.insert(identifier)
            return
        }
        for transactionIdentifier in transactionStack {
            terminalTransactionIdentifiers.insert(transactionIdentifier)
        }
        terminalTransactionIdentifiers.insert(identifier)
        transactionStack.removeAll(keepingCapacity: false)
        activeRootIdentifier = nil
        startNextWaitingRoot()
    }

    private func savepointName(_ identifier: UInt64) -> String {
        "storagekit_\(identifier)"
    }

    private func map(
        _ error: any Error,
        operation: StorageOperation
    ) -> StorageError {
        if let storageError = error as? StorageError {
            if storageError.backend == .sqlite {
                return storageError
            }
            return StorageError(
                code: storageError.code,
                operation: operation,
                backend: .sqlite,
                message: storageError.message,
                backendCode: storageError.backendCode,
                underlyingDescription: storageError.underlyingDescription
            )
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .sqlite,
            message: "SQLite transaction coordinator failed",
            underlyingDescription: String(describing: error)
        )
    }

    private func invalidState(
        _ message: String,
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .sqlite,
            message: message
        )
    }

    private func cancelledError(operation: StorageOperation) -> StorageError {
        StorageError(
            code: .transactionCancelled,
            operation: operation,
            backend: .sqlite,
            message: "SQLite transaction was cancelled before acquiring its lease"
        )
    }

    private func closedError(operation: StorageOperation) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .sqlite,
            message: "SQLite storage engine is closed"
        )
    }
}
