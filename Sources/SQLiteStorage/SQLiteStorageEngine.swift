import StorageKit
import Synchronization

/// SQLite `StorageEngine` with lazily acquired FIFO transaction leases.
///
/// Synchronous mutations are buffered without acquiring SQLite. The first
/// async read, range iteration, maintenance operation, or commit enters the
/// coordinator actor, which owns `BEGIN IMMEDIATE` through terminal cleanup.
/// Nested transactions use native savepoints and strict LIFO ordering.
public final class SQLiteStorageEngine: StorageEngine, Sendable {
    public struct Configuration: Sendable {
        public var path: String

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

        public init(path: String, busyTimeoutMilliseconds: Int32 = 100) {
            self.path = path
            self.busyTimeoutMilliseconds = busyTimeoutMilliseconds
        }

        public static func file(
            _ path: String,
            busyTimeoutMilliseconds: Int32 = 100
        ) -> Configuration {
            Configuration(
                path: path,
                busyTimeoutMilliseconds: busyTimeoutMilliseconds
            )
        }

        public static var inMemory: Configuration {
            Configuration(path: ":memory:")
        }
    }

    public typealias TransactionType = SQLiteStorageTransaction

    private struct EngineState: Sendable {
        var nextTransactionIdentifier: UInt64 = 1
    }

    private let connection: SQLiteConnectionHandle
    private let lifetime: SQLiteStorageLifetime
    private let coordinator: SQLiteTransactionCoordinator
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let state = Mutex(EngineState())
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration) throws {
        let connection = try SQLiteConnectionHandle(
            path: configuration.path,
            busyTimeoutMilliseconds: configuration.busyTimeoutMilliseconds
        )
        let lifetime = SQLiteStorageLifetime()
        let domain = StorageTransactionDomain()
        self.connection = connection
        self.lifetime = lifetime
        self.transactionDomain = domain
        self.directoryAccess = KeyValueDirectoryCatalog(
            transactionDomain: domain,
            backend: .sqlite
        )
        self.coordinator = SQLiteTransactionCoordinator(
            connection: connection,
            lifetime: lifetime
        )
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

            return SQLiteStorageTransaction(
                identifier: identifier,
                coordinator: coordinator,
                connection: connection,
                lifetime: lifetime,
                transactionDomain: transactionDomain
            )
        }
    }

    public func requestShutdown() {
        transactionDomain.requestShutdown()
        let lifetime = lifetime
        let coordinator = coordinator
        let connection = connection
        storageLifecycle.requestShutdown(
            prepare: {
                _ = lifetime.close()
            },
            cleanup: {
                await coordinator.shutdown()
                connection.close()
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
        connection.rangeInstrumentation
    }

    var leaseInstrumentation: SQLiteLeaseInstrumentation {
        get async {
            await coordinator.leaseInstrumentation
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
