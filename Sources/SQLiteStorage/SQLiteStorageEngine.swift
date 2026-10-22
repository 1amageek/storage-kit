import StorageKit
import Synchronization

/// SQLite `StorageEngine` with lazily acquired FIFO transaction leases.
///
/// Synchronous mutations are buffered without acquiring SQLite. The first
/// async read, range iteration, maintenance operation, or commit enters the
/// selected connection's coordinator through terminal cleanup. The default
/// single connection uses `BEGIN IMMEDIATE`; explicit parallel WAL connections
/// use deferred transactions and report retryable snapshot upgrade conflicts.
/// Nested transactions use native savepoints and strict LIFO ordering.
public final class SQLiteStorageEngine: StorageEngine, Sendable {
    public struct Configuration: Sendable {
        public var path: String

        /// Open handles and maximum simultaneous native root transactions.
        /// Counts above one require a file-backed WAL database and SQLite 3.51.3+.
        public var connectionCount: Int

        /// SQLite-level wait budget for acquiring the database write lock,
        /// applied as `PRAGMA busy_timeout`.
        ///
        /// This absorbs only micro-contention between connections (typically
        /// separate engines or processes sharing one database file). Beyond
        /// the budget the engine still fails fast with the typed
        /// `StorageError.transactionBusy`, whose `retryDisposition == .safe`
        /// lets transaction runners replay with asynchronous backoff.
        /// The value is deliberately small: the coordinator executes SQLite's
        /// synchronous C calls, so this bounds how long one of those calls may
        /// block. Set `0` to fail immediately on any contention.
        public var busyTimeoutMilliseconds: Int32

        public init(path: String, busyTimeoutMilliseconds: Int32 = 100, connectionCount: Int = 1) {
            self.path = path
            self.connectionCount = connectionCount
            self.busyTimeoutMilliseconds = busyTimeoutMilliseconds
        }

        public static func file(
            _ path: String,
            busyTimeoutMilliseconds: Int32 = 100,
            connectionCount: Int = 1
        ) -> Configuration {
            Configuration(
                path: path,
                busyTimeoutMilliseconds: busyTimeoutMilliseconds,
                connectionCount: connectionCount
            )
        }

        public static var inMemory: Configuration {
            Configuration(path: ":memory:")
        }
    }

    public typealias TransactionType = SQLiteStorageTransaction

    private struct EngineState: Sendable {
        var nextTransactionIdentifier: UInt64 = 1
        var nextConnectionIndex = 0
    }

    private struct TransactionConnection: Sendable {
        let handle: SQLiteConnectionHandle
        let coordinator: SQLiteTransactionCoordinator
    }

    private let connections: [TransactionConnection]
    private let lifetime: SQLiteStorageLifetime
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let state = Mutex(EngineState())
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration) throws {
        guard configuration.connectionCount > 0 else {
            throw StorageError(code: .invalidOperation, operation: .open, backend: .sqlite,
                message: "SQLite connectionCount must be positive")
        }
        let lifetime = SQLiteStorageLifetime()
        let parallel = configuration.connectionCount > 1
        guard !parallel || !configuration.path.hasPrefix("file:") else {
            throw StorageError(code: .unsupportedOperation, operation: .open, backend: .sqlite,
                message: "Concurrent SQLite transactions require an ordinary file path without URI options")
        }
        var connections: [TransactionConnection] = []
        do {
            var path = configuration.path
            while connections.count < configuration.connectionCount {
                let handle = try SQLiteConnectionHandle(path: path,
                    busyTimeoutMilliseconds: configuration.busyTimeoutMilliseconds,
                    allowsConcurrentTransactions: parallel)
                path = handle.databasePath
                connections.append(TransactionConnection(handle: handle,
                    coordinator: SQLiteTransactionCoordinator(connection: handle, lifetime: lifetime,
                        allowsConcurrentTransactions: parallel)))
            }
        } catch {
            for connection in connections { connection.handle.close() }
            throw error
        }
        self.connections = connections
        self.lifetime = lifetime
        let domain = StorageTransactionDomain()
        self.transactionDomain = domain
        self.directoryAccess = KeyValueDirectoryCatalog(transactionDomain: domain, backend: .sqlite)
    }

    public func createTransaction() throws -> SQLiteStorageTransaction {
        try storageLifecycle.withActiveAdmission(
            backend: .sqlite,
            operation: .beginTransaction
        ) {
            let identifier = try allocateTransactionIdentifier()

            let contextLease = ActiveTransactionContext
                .acquireCurrentTransaction()
            if let contextLease,
               let existing = contextLease.transaction
                as? SQLiteStorageTransaction,
               existing.transactionDomain === transactionDomain {
                do {
                    return try existing.makeChild(
                        identifier: identifier,
                        contextLease: contextLease
                    )
                } catch {
                    contextLease.release()
                    throw error
                }
            }
            contextLease?.release()

            let selected = state.withLock { state in
                let selected = connections[state.nextConnectionIndex]
                state.nextConnectionIndex = (state.nextConnectionIndex + 1) % connections.count
                return selected
            }
            return SQLiteStorageTransaction(
                identifier: identifier,
                coordinator: selected.coordinator,
                connection: selected.handle,
                lifetime: lifetime,
                transactionDomain: transactionDomain
            )
        }
    }

    public func requestShutdown() {
        transactionDomain.requestShutdown()
        let lifetime = lifetime
        let connections = connections
        storageLifecycle.requestShutdown(
            prepare: {
                _ = lifetime.close()
            },
            cleanup: {
                for connection in connections { await connection.coordinator.shutdown() }
                for connection in connections { connection.handle.close() }
            }
        )
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }

    deinit {
        requestShutdown()
    }

    var rangeInstrumentation: SQLiteRangeInstrumentation {
        var total = SQLiteRangeInstrumentation(prepareCount: 0, stepCount: 0,
            payloadCopyCount: 0, finalizeCount: 0, openCursorCount: 0)
        for connection in connections {
            let value = connection.handle.rangeInstrumentation
            total = SQLiteRangeInstrumentation(prepareCount: total.prepareCount &+ value.prepareCount,
                stepCount: total.stepCount &+ value.stepCount,
                payloadCopyCount: total.payloadCopyCount &+ value.payloadCopyCount,
                finalizeCount: total.finalizeCount &+ value.finalizeCount,
                openCursorCount: total.openCursorCount + value.openCursorCount)
        }
        return total
    }

    var leaseInstrumentation: SQLiteLeaseInstrumentation {
        get async {
            var total = SQLiteLeaseInstrumentation(hasActiveRoot: false, waitingRootCount: 0,
                savepointBeginCount: 0, savepointReleaseCount: 0, savepointRollbackCount: 0)
            for connection in connections {
                let value = await connection.coordinator.leaseInstrumentation
                total = SQLiteLeaseInstrumentation(hasActiveRoot: total.hasActiveRoot || value.hasActiveRoot,
                    waitingRootCount: total.waitingRootCount + value.waitingRootCount,
                    savepointBeginCount: total.savepointBeginCount &+ value.savepointBeginCount,
                    savepointReleaseCount: total.savepointReleaseCount &+ value.savepointReleaseCount,
                    savepointRollbackCount: total.savepointRollbackCount &+ value.savepointRollbackCount)
            }
            return total
        }
    }

    private func allocateTransactionIdentifier() throws -> UInt64 {
        try state.withLock { state in
            let identifier = state.nextTransactionIdentifier
            let (next, overflow) = identifier.addingReportingOverflow(1)
            guard !overflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .beginTransaction,
                    backend: .sqlite,
                    message: "SQLite transaction identifier space is exhausted"
                )
            }
            state.nextTransactionIdentifier = next
            return identifier
        }
    }

    private static func closedError(
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .sqlite,
            message: "SQLite storage engine is closed"
        )
    }
}
