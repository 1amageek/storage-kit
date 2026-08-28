import StorageKit

// FIXME(INCOMPLETE_IMPLEMENTATION): FoundationDB Directory access is not
// implemented yet. Production call path: `FDBStorageEngine.directoryAccess`
// and `StorageEngine.leasePartition`. Every operation fails with
// `unsupportedOperation` until the native Directory Layer binding
// (PROGRESS K-60) lands; no caller may treat this adapter as providing
// Directories or Partitions before that binding passes the shared
// Directory conformance case.
final class FDBDirectoryAccess: DirectoryAccess, Sendable {
    let transactionDomain: StorageTransactionDomain
    let backend: StorageBackend = .foundationDB

    init(transactionDomain: StorageTransactionDomain) {
        self.transactionDomain = transactionDomain
    }

    private func unsupported(_ operation: StorageOperation) -> StorageError {
        StorageError.unsupportedOperation(
            "FoundationDB Directory access is not implemented",
            operation: operation,
            backend: .foundationDB
        )
    }

    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        throw unsupported(.open)
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        throw unsupported(.initialize)
    }

    func openDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        throw unsupported(.open)
    }

    func openOrCreateDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        throw unsupported(.write)
    }

    func openPartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Partition? {
        throw unsupported(.open)
    }

    func openOrCreatePartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Partition {
        throw unsupported(.write)
    }

    func listDirectories(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [String] {
        throw unsupported(.rangeRead)
    }

    func listPartitions(
        in parent: Directory,
        after: PartitionID?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [PartitionID] {
        throw unsupported(.rangeRead)
    }

    func moveChild(
        _ child: StorageAddressStep,
        in source: Directory,
        to newChild: StorageAddressStep,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        throw unsupported(.write)
    }

    func removeChild(
        _ child: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        throw unsupported(.delete)
    }
}
