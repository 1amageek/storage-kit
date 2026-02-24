import StorageKit
import FoundationDB

/// FoundationDB backend StorageEngine implementation.
///
/// Wraps FDB's `DatabaseProtocol` and provides StorageKit's unified interface.
/// Retry logic is based on FDB's `isRetryable` error classification.
public final class FDBStorageEngine: StorageEngine, @unchecked Sendable {
    public typealias TransactionType = FDBStorageTransaction

    private let database: any DatabaseProtocol

    public init(database: any DatabaseProtocol) {
        self.database = database
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
            } catch let error as FDBError where error.isRetryable {
                tx.cancel()
                if attempt < maxRetries - 1 { continue }
                throw StorageError.backendError(error.description)
            } catch let error as StorageError {
                tx.cancel()
                throw error
            } catch {
                tx.cancel()
                throw StorageError.backendError("\(error)")
            }
        }
        throw StorageError.transactionTooOld
    }
}
