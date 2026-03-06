import StorageKit
import FoundationDB

/// FoundationDB backend StorageEngine implementation.
///
/// Wraps FDB's `DatabaseProtocol` and provides StorageKit's unified interface.
/// Retry logic is based on FDB's `isRetryable` error classification.
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

    public typealias TransactionType = FDBStorageTransaction

    nonisolated(unsafe) public let database: any DatabaseProtocol

    public init(configuration: Configuration) async throws {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }
        self.database = try configuration.database ?? FDBClient.openDatabase()
    }

    public func createTransaction() throws -> FDBStorageTransaction {
        let fdbTx = try database.createTransaction()
        return FDBStorageTransaction(fdbTx)
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let maxRetries = 100
        for attempt in 0..<maxRetries {
            let tx = try createTransaction()
            do {
                let result = try await operation(tx)
                try await tx.commit()
                return result
            } catch let error as StorageError where error.isRetryable {
                // StorageError retryable (converted from FDBError in commit/other ops)
                tx.cancel()
                if attempt < maxRetries - 1 { continue }
                throw StorageError.backendError("Max retries exceeded: \(error)")
            } catch let error as FDBError where error.isRetryable {
                // Raw FDBError that escaped conversion (e.g. from user code calling fdbTransaction directly)
                tx.cancel()
                if attempt < maxRetries - 1 { continue }
                throw StorageError.backendError("Max retries exceeded: \(error.description)")
            } catch {
                tx.cancel()
                throw error
            }
        }
        throw StorageError.transactionTooOld
    }

    public var directoryService: any DirectoryService {
        FDBDirectoryService(database: database)
    }
}
