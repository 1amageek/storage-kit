import DatabaseTypes
import StorageKit
import FoundationDB

/// Persistent namespace registry backed by FoundationDB's directory layer.
///
/// FDB's DirectoryLayer uses the High Contention Allocator (HCA)
/// to dynamically assign short prefixes.
final class NamespaceRegistry:
    NamespaceResolver,
    NamespaceCatalog,
    Sendable {

    private let database: any DatabaseProtocol
    private let transactionDomain: StorageTransactionDomain

    init(
        database: any DatabaseProtocol,
        transactionDomain: StorageTransactionDomain
    ) {
        self.database = database
        self.transactionDomain = transactionDomain
    }

    func resolveOrCreate(
        path: [String],
        transaction: any StorageKit.TransactionAccess
    ) async throws -> StorageKit.Subspace {
        try await withFDBTransaction(
            transaction,
            writes: true,
            operation: .write
        ) { fdbTransaction in
            let directoryLayer = DirectoryLayer(database: self.database)
            let directory = try await directoryLayer.createOrOpen(
                path: path,
                transaction: fdbTransaction
            )
            return StorageKit.Subspace(
                prefix: ByteString(directory.subspace.prefix)
            )
        }
    }

    func resolveExisting(
        path: [String],
        transaction: any StorageKit.TransactionAccess
    ) async throws -> StorageKit.Subspace {
        try await withFDBTransaction(
            transaction,
            writes: false,
            operation: .read
        ) { fdbTransaction in
            let directoryLayer = DirectoryLayer(database: self.database)
            let directory = try await directoryLayer.open(
                path: path,
                transaction: fdbTransaction
            )
            return StorageKit.Subspace(
                prefix: ByteString(directory.subspace.prefix)
            )
        }
    }

    func listNamespaces(
        path: [String],
        transaction: any StorageKit.TransactionAccess
    ) async throws -> [String] {
        try await withFDBTransaction(
            transaction,
            writes: false,
            operation: .read
        ) { fdbTransaction in
            let directoryLayer = DirectoryLayer(database: self.database)
            return try await directoryLayer.list(
                path: path,
                transaction: fdbTransaction
            )
        }
    }

    func removeNamespace(
        path: [String],
        transaction: any StorageKit.TransactionAccess
    ) async throws {
        try await withFDBTransaction(
            transaction,
            writes: true,
            operation: .delete
        ) { fdbTransaction in
            let directoryLayer = DirectoryLayer(database: self.database)
            try await directoryLayer.remove(
                path: path,
                transaction: fdbTransaction
            )
        }
    }

    func namespaceExists(
        path: [String],
        transaction: any StorageKit.TransactionAccess
    ) async throws -> Bool {
        try await withFDBTransaction(
            transaction,
            writes: false,
            operation: .read
        ) { fdbTransaction in
            let directoryLayer = DirectoryLayer(database: self.database)
            return try await directoryLayer.exists(
                path: path,
                transaction: fdbTransaction
            )
        }
    }

    private func withFDBTransaction<T: Sendable>(
        _ transaction: any StorageKit.TransactionAccess,
        writes: Bool,
        operation: StorageOperation,
        _ body: (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        guard let transaction = transaction as? FDBStorageTransaction else {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .foundationDB,
                message: "FoundationDB directory operations require an FDB storage transaction"
            )
        }
        return try await transaction.withNamespaceOperation(
            transactionDomain: transactionDomain,
            writes: writes,
            operation: operation
        ) { fdbTransaction in
            do {
                return try await body(fdbTransaction)
            } catch let error as FDBError {
                throw FDBStorageTransaction.convertFDBError(
                    error,
                    operation: operation
                )
            }
        }
    }
}
