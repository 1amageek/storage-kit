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
    private let state = Mutex(EngineState())

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
        guard !lifetime.isClosed else {
            throw Self.closedError(operation: .beginTransaction)
        }
        let identifier = try allocateTransactionIdentifier()

        if let existing = ActiveTransactionScope.current
            as? SQLiteStorageTransaction,
           existing.belongs(to: lifetime) {
            return try existing.makeChild(identifier: identifier)
        }

        return SQLiteStorageTransaction(
            identifier: identifier,
            coordinator: coordinator,
            connection: connection,
            lifetime: lifetime
        )
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let transaction = try createTransaction()
        return try await ActiveTransactionScope.$current.withValue(transaction) {
            do {
                let result = try await operation(transaction)
                try await transaction.commit()
                return result
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

    /// Closes the connection and wakes every queued lease waiter with a typed
    /// failure. Closing is idempotent.
    public func close() {
        guard lifetime.close() else { return }
        connection.close()
        let coordinator = coordinator
        Task {
            await coordinator.shutdown()
        }
    }

    public func shutdown() {
        close()
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
