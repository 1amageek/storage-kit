import StorageKit
import FoundationDB
import Synchronization

/// FoundationDB backend StorageEngine implementation.
///
/// Wraps FDB's `DatabaseProtocol` and provides StorageKit's unified interface.
/// Transaction retry is owned by higher-level runners. This engine only creates
/// transactions and classifies backend errors.
///
/// ## Usage
/// ```swift
/// // Default cluster (handles FDBClient initialization internally)
/// let engine = try await FDBStorageEngine(configuration: .init())
///
/// // Specific database instance
/// let engine = try await FDBStorageEngine(configuration: .init(database: db))
/// ```
public final class FDBStorageEngine: StorageEngine, Sendable {

    public struct Configuration: Sendable {
        let database: (any DatabaseProtocol)?
        let commitRequestLimit: CommitRequestLimit

        /// Use the default cluster. FDB client library is initialized automatically.
        public init(
            commitRequestLimit: CommitRequestLimit = .default
        ) {
            self.database = nil
            self.commitRequestLimit = commitRequestLimit
        }

        /// Use a specific database instance.
        public init(
            database: any DatabaseProtocol,
            commitRequestLimit: CommitRequestLimit = .default
        ) {
            self.database = database
            self.commitRequestLimit = commitRequestLimit
        }
    }

    /// Serializes FoundationDB client startup to prevent TOCTOU races.
    ///
    /// `FDBClient.initialize()` throws if called twice. Without serialization,
    /// concurrent `init(configuration:)` calls could both observe `isInitialized == false`
    /// and race into `initialize()`.
    private static let clientStartup = FoundationDBClientStartup()

    private actor FoundationDBClientStartup {
        private var initialized = false

        func ensureInitialized() async throws {
            guard !initialized else { return }
            try await FDBClient.initialize()
            initialized = true
        }
    }

    public typealias TransactionType = FDBStorageTransaction

    private let database: Mutex<(any DatabaseProtocol)?>
    private let transactionDomain = StorageTransactionDomain()
    private let commitRequestLimit: CommitRequestLimit
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration) async throws {
        let database: any DatabaseProtocol
        if let configuredDatabase = configuration.database {
            database = configuredDatabase
        } else {
            if !FDBClient.isInitialized {
                try await Self.clientStartup.ensureInitialized()
            }
            database = try FDBClient.openDatabase()
        }
        self.database = Mutex(database)
        self.commitRequestLimit = configuration.commitRequestLimit
    }

    public func createTransaction() throws -> FDBStorageTransaction {
        try storageLifecycle.withActiveAdmission(
            backend: .foundationDB,
            operation: .beginTransaction
        ) {
            let database = try retainedDatabase(operation: .beginTransaction)
            do {
                let fdbTx = try database.createTransaction()
                return try FDBStorageTransaction(
                    fdbTx,
                    database: database,
                    transactionDomain: transactionDomain,
                    commitRequestLimit: commitRequestLimit
                )
            } catch let error as FDBError {
                throw FDBStorageTransaction.convertFDBError(error, operation: .beginTransaction)
            } catch {
                throw FDBStorageTransaction.convertBackendError(error, operation: .beginTransaction)
            }
        }
    }

    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let tx = try createTransaction()
        try await ActiveTransactionScope.withActiveTransaction(
            tx
        ) { _ in
            do {
                try await operation(tx)
            } catch let error as FDBError {
                let converted = FDBStorageTransaction.convertFDBError(
                    error,
                    operation: .execute
                )
                try await cancel(tx, preserving: converted)
                throw converted
            } catch {
                try await cancel(tx, preserving: error)
                throw error
            }

            do {
                try await tx.commit()
            } catch {
                if let storageError = error as? StorageError,
                   storageError.code == .commitUnknownResult {
                    throw storageError
                }
                try await cancel(tx, preserving: error)
                throw error
            }
        }
    }

    private func cancel(
        _ transaction: FDBStorageTransaction,
        preserving operationError: any Error
    ) async throws {
        do {
            try await transaction.cancel()
        } catch {
            throw StorageTransactionCleanupError(
                operationError: operationError,
                cancellationError: error
            )
        }
    }

    public var namespaceResolver: any NamespaceResolver {
        NamespaceRegistry(
            transactionDomain: transactionDomain
        )
    }

    public var namespaceCatalog: (any NamespaceCatalog)? {
        NamespaceRegistry(
            transactionDomain: transactionDomain
        )
    }

    public func requestShutdown() {
        storageLifecycle.requestShutdown { [self] in
            releaseDatabase()
        }
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }

    deinit {
        requestShutdown()
    }

    private func retainedDatabase(
        operation: StorageOperation
    ) throws -> any DatabaseProtocol {
        try database.withLock { database in
            guard let database else {
                throw StorageError(
                    code: .invalidOperation,
                    operation: operation,
                    backend: .foundationDB,
                    message: "Storage engine shutdown has been requested"
                )
            }
            return database
        }
    }

    private func releaseDatabase() {
        let releasedDatabase = database.withLock { database in
            let releasedDatabase = database
            database = nil
            return releasedDatabase
        }
        withExtendedLifetime(releasedDatabase) {}
    }
}
