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
/// // Explicit cluster file with defaults applied to every transaction
/// let isolatedEngine = try await FDBStorageEngine(
///     configuration: .init(
///         clusterFilePath: "/run/storage/fdb.cluster",
///         transactionOptions: [.timeout(milliseconds: 30_000)]
///     )
/// )
///
/// // Specific database instance
/// let engine = try await FDBStorageEngine(configuration: .init(database: db))
/// ```
public final class FDBStorageEngine: StorageEngine, Sendable {

    public struct Configuration: Sendable {
        let database: (any DatabaseProtocol)?
        let clusterFilePath: String?
        let transactionOptions: [TransactionOption]
        let commitRequestLimit: CommitRequestLimit

        /// Use the default cluster. FDB client library is initialized automatically.
        public init(
            transactionOptions: [TransactionOption] = [],
            commitRequestLimit: CommitRequestLimit = .default
        ) {
            self.database = nil
            self.clusterFilePath = nil
            self.transactionOptions = transactionOptions
            self.commitRequestLimit = commitRequestLimit
        }

        /// Use the cluster selected by an explicit cluster file.
        ///
        /// An empty path is rejected by `FDBStorageEngine.init(configuration:)`.
        /// The adapter never searches for a cluster file when this initializer is used.
        public init(
            clusterFilePath: String,
            transactionOptions: [TransactionOption] = [],
            commitRequestLimit: CommitRequestLimit = .default
        ) {
            self.database = nil
            self.clusterFilePath = clusterFilePath
            self.transactionOptions = transactionOptions
            self.commitRequestLimit = commitRequestLimit
        }

        /// Use a specific database instance.
        public init(
            database: any DatabaseProtocol,
            transactionOptions: [TransactionOption] = [],
            commitRequestLimit: CommitRequestLimit = .default
        ) {
            self.database = database
            self.clusterFilePath = nil
            self.transactionOptions = transactionOptions
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
        private var startupTask: Task<Void, any Error>?

        func ensureInitialized() async throws {
            if FDBClient.isInitialized { return }
            if let startupTask {
                return try await startupTask.value
            }
            let startupTask = Task {
                try await FDBClient.initialize()
            }
            self.startupTask = startupTask
            try await startupTask.value
        }
    }

    public typealias TransactionType = FDBStorageTransaction

    private let database: Mutex<(any DatabaseProtocol)?>
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let transactionOptions: [TransactionOption]
    private let commitRequestLimit: CommitRequestLimit
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration) async throws {
        let database: any DatabaseProtocol
        if let configuredDatabase = configuration.database {
            database = configuredDatabase
        } else {
            if let clusterFilePath = configuration.clusterFilePath,
               clusterFilePath.isEmpty {
                throw StorageError(
                    code: .invalidOperation,
                    operation: .open,
                    backend: .foundationDB,
                    message: "FoundationDB cluster file path must not be empty"
                )
            }
            if !FDBClient.isInitialized {
                try await Self.clientStartup.ensureInitialized()
            }
            do {
                database = try FDBClient.openDatabase(
                    clusterFilePath: configuration.clusterFilePath
                )
            } catch let error as FDBError {
                throw FDBStorageTransaction.convertFDBError(
                    error,
                    operation: .open
                )
            } catch {
                throw FDBStorageTransaction.convertBackendError(
                    error,
                    operation: .open
                )
            }
        }
        let domain = StorageTransactionDomain()
        self.database = Mutex(database)
        self.transactionDomain = domain
        self.directoryAccess = FDBDirectoryAccess(transactionDomain: domain)
        self.transactionOptions = configuration.transactionOptions
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
                let transaction = try FDBStorageTransaction(
                    fdbTx,
                    database: database,
                    transactionDomain: transactionDomain,
                    commitRequestLimit: commitRequestLimit
                )
                for option in transactionOptions {
                    try transaction.setOption(forOption: option)
                }
                return transaction
            } catch let error as FDBError {
                throw FDBStorageTransaction.convertFDBError(error, operation: .beginTransaction)
            } catch let error as StorageError {
                throw StorageError(
                    code: error.code,
                    operation: .beginTransaction,
                    backend: .foundationDB,
                    message: error.message,
                    backendCode: error.backendCode,
                    underlyingDescription: error.underlyingDescription,
                    byteLimitViolation: error.byteLimitViolation
                )
            } catch {
                throw FDBStorageTransaction.convertBackendError(error, operation: .beginTransaction)
            }
        }
    }

    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let tx = try createTransaction()
        defer { transactionDomain.leases.releaseIntents(for: tx) }
        try await ActiveTransactionContext.withActiveTransaction(
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

    public func requestShutdown() {
        transactionDomain.leases.requestShutdown()
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
