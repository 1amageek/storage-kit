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

        public init(path: String) {
            self.path = path
        }

        public static func file(_ path: String) -> Configuration {
            Configuration(path: path)
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
    private let transactionDomain = StorageTransactionDomain()
    private let state = Mutex(EngineState())
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration) throws {
        let connection = try SQLiteConnectionHandle(path: configuration.path)
        let lifetime = SQLiteStorageLifetime()
        self.connection = connection
        self.lifetime = lifetime
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

            if let existing = ActiveTransactionScope.current
                as? SQLiteStorageTransaction,
               existing.transactionDomain === transactionDomain {
                return try existing.makeChild(identifier: identifier)
            }

            return SQLiteStorageTransaction(
                identifier: identifier,
                coordinator: coordinator,
                connection: connection,
                lifetime: lifetime,
                transactionDomain: transactionDomain
            )
        }
    }

    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let transaction = try createTransaction()
        try await ActiveTransactionScope.withActiveTransaction(
            transaction
        ) { _ in
            do {
                try await operation(transaction)
                try await transaction.commit()
            } catch {
                let operationError = error
                do {
                    try await transaction.cancel()
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                }
                throw operationError
            }
        }
    }

    public func requestShutdown() {
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
