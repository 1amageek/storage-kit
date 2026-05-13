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
        nonisolated(unsafe) let database: (any DatabaseProtocol)?

        /// Use the default cluster. FDB client library is initialized automatically.
        public init() {
            self.database = nil
        }

        /// Use a specific database instance.
        public init(database: any DatabaseProtocol) {
            self.database = database
        }
    }

    /// Serializes FDB client library initialization to prevent TOCTOU races.
    ///
    /// `FDBClient.initialize()` throws if called twice. Without serialization,
    /// concurrent `init(configuration:)` calls could both observe `isInitialized == false`
    /// and race into `initialize()`.
    private static let initGuard = InitializationGuard()

    private actor InitializationGuard {
        private var initialized = false

        func ensureInitialized() async throws {
            guard !initialized else { return }
            try await FDBClient.initialize()
            initialized = true
        }
    }

    public typealias TransactionType = FDBStorageTransaction

    nonisolated(unsafe) public let database: any DatabaseProtocol

    public init(configuration: Configuration) async throws {
        if !FDBClient.isInitialized {
            try await Self.initGuard.ensureInitialized()
        }
        self.database = try configuration.database ?? FDBClient.openDatabase()
    }

    public func createTransaction() throws -> FDBStorageTransaction {
        do {
            let fdbTx = try database.createTransaction()
            return FDBStorageTransaction(fdbTx)
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
            do {
                let result = try await operation(tx)
                try await tx.commit()
                return result
            } catch let error as FDBError {
                tx.cancel()
                throw FDBStorageTransaction.convertFDBError(error, operation: .commit)
            } catch {
                tx.cancel()
                throw error
            }
        }
    }

    public var directoryService: any DirectoryService {
        FDBDirectoryService(database: database)
    }
}
