import StorageKit
import FoundationDB

/// FoundationDB backend StorageEngine implementation.
///
/// Wraps FDB's `DatabaseProtocol` and provides StorageKit's unified interface.
/// Retry logic is based on FDB's `isRetryable` error classification.
public final class FDBStorageEngine: StorageEngine, @unchecked Sendable {
    public typealias TransactionType = FDBStorageTransaction

    public let database: any DatabaseProtocol

    public init(database: any DatabaseProtocol) {
        self.database = database
    }

    /// Initialize the FDB client library (once per process).
    public static func initialize() async throws {
        try await FDBClient.initialize()
    }

    /// Connect to the default cluster and create an FDBStorageEngine.
    public static func open() async throws -> FDBStorageEngine {
        let db = try FDBClient.openDatabase()
        return FDBStorageEngine(database: db)
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
