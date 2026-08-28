import DatabaseTypes

/// Storage placement authority of one engine: the eight semantic Directory
/// operations plus root bootstrap through the layout-marker state machine.
///
/// Read operations take `TransactionReadAccess` and never create; write
/// operations are atomic with the caller's transaction. Every operation checks
/// that the transaction, the parent Directory, and the catalog share one
/// `StorageTransactionDomain` before performing any I/O.
public protocol DirectoryAccess: AnyObject, Sendable {
    var transactionDomain: StorageTransactionDomain { get }

    /// Backend reported in every typed failure this catalog produces.
    var backend: StorageBackend { get }

    /// Operation 1 applied to the root: `nil` when the store is uninitialized.
    func openRoot(
        transaction: any TransactionReadAccess
    ) async throws -> Directory?

    /// Operation 2 applied to the root: initializes layout V1 when absent.
    func openOrInitializeRoot(
        transaction: any TransactionAccess
    ) async throws -> Directory

    /// Operation 1: opens a child Directory; `nil` when absent.
    func openDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory?

    /// Operation 2: opens or creates a child Directory.
    func openOrCreateDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory

    /// Operation 3: opens a child Partition; `nil` when absent.
    func openPartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Partition?

    /// Operation 4: opens or creates a child Partition.
    func openOrCreatePartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Partition

    /// Operation 5: one page of child Directory names ordered by encoded key.
    func listDirectories(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [String]

    /// Operation 6: one page of child Partition identifiers ordered by encoded key.
    func listPartitions(
        in parent: Directory,
        after: PartitionID?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [PartitionID]

    /// Operation 7: atomically renames a child Directory.
    func moveChild(
        _ child: StorageAddressStep,
        in source: Directory,
        to newChild: StorageAddressStep,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory

    /// Operation 8: removes an empty, unleased child.
    func removeChild(
        _ child: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws
}

extension DirectoryAccess {
    /// Walks `path` below `parent` with operation 1; `nil` when any step is absent.
    public func openDirectory(
        at path: DirectoryPath,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        var current = parent
        for component in path.components {
            guard let next = try await openDirectory(
                component,
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = next
        }
        return current
    }

    /// Walks `path` below `parent` with operation 2.
    public func openOrCreateDirectory(
        at path: DirectoryPath,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        var current = parent
        for component in path.components {
            current = try await openOrCreateDirectory(
                component,
                in: current,
                transaction: transaction
            )
        }
        return current
    }

    /// Resolves an absolute address from the root with read operations only.
    ///
    /// A Partition step resolves to the Partition's root Directory.
    public func openDirectory(
        at address: StorageAddress,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        guard var current = try await openRoot(transaction: transaction) else {
            return nil
        }
        for step in address.steps {
            switch step {
            case .directory(let name):
                guard let next = try await openDirectory(
                    name,
                    in: current,
                    transaction: transaction
                ) else {
                    return nil
                }
                current = next
            case .partition(let id):
                guard let next = try await openPartition(
                    id,
                    in: current,
                    transaction: transaction
                ) else {
                    return nil
                }
                current = next.root
            }
        }
        return current
    }
}
