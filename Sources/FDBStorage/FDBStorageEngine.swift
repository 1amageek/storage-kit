import StorageKit
import FoundationDB

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

    public let database: any DatabaseProtocol
    private let transactionDomain = FoundationDBTransactionDomain()
    private let commitRequestLimit: CommitRequestLimit

    public init(configuration: Configuration) async throws {
        if !FDBClient.isInitialized {
            try await Self.clientStartup.ensureInitialized()
        }
        self.database = try configuration.database ?? FDBClient.openDatabase()
        self.commitRequestLimit = configuration.commitRequestLimit
    }

    public func createTransaction() throws -> FDBStorageTransaction {
        do {
            let fdbTx = try database.createTransaction()
            return try FDBStorageTransaction(
                fdbTx,
                transactionDomain: transactionDomain,
                commitRequestLimit: commitRequestLimit
            )
        } catch let error as FDBError {
            throw FDBStorageTransaction.convertFDBError(error, operation: .beginTransaction)
        } catch {
            throw FDBStorageTransaction.convertBackendError(error, operation: .beginTransaction)
        }
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let tx = try createTransaction()
        return try await ActiveTransactionScope.$current.withValue(tx) {
            let result: T
            do {
                result = try await operation(tx)
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
                return result
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

    public var directoryService: any DirectoryService {
        FDBDirectoryService(
            database: database,
            transactionDomain: transactionDomain
        )
    }
}
