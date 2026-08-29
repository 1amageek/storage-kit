import DatabaseTypes

/// Storage placement authority of one engine: the five semantic Directory
/// operations plus root bootstrap through the layout-marker state machine.
///
/// Read operations take `TransactionReadAccess` and never create; write
/// operations are atomic with the caller's transaction. Every operation checks
/// that the transaction, the parent Directory, and the catalog share one
/// `StorageTransactionDomain` before performing any I/O.
///
/// A node is a prefix plus a layer tag. One name namespace exists per parent:
/// a name identifies exactly one node and the tag is a property of that node.
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

    /// Operation 1: opens an existing child; `nil` when absent.
    ///
    /// A non-`nil` `expected` tag is verified against the stored tag and fails
    /// with `directoryLayerMismatch` when they differ. `nil` performs no
    /// verification; the stored tag is always carried on the result.
    func open(
        _ name: String,
        expecting expected: LayerTag?,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory?

    /// Operation 2: opens or creates a child with `layer`.
    ///
    /// An existing node whose stored tag differs from `layer` fails with
    /// `directoryLayerMismatch`; nothing is created.
    func openOrCreate(
        _ name: String,
        layer: LayerTag,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory

    /// Operation 3: one page of child entries ordered by encoded key.
    func listChildren(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [DirectoryEntry]

    /// Operation 4: atomically moves one node, Partitions included.
    ///
    /// Source and destination must lie in the same Directory Layer; a move that
    /// would cross a Partition boundary fails with `partitionBoundaryViolation`.
    func move(
        _ name: String,
        in source: Directory,
        to newName: String,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory

    /// Operation 5: atomic recursive removal of a child and its whole subtree.
    func remove(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws
}

extension DirectoryAccess {
    /// Operation 1 restricted to plain Directories.
    public func openDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try await open(name, expecting: .default, in: parent, transaction: transaction)
    }

    /// Operation 2 restricted to plain Directories.
    public func openOrCreateDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try await openOrCreate(name, layer: .default, in: parent, transaction: transaction)
    }

    /// Operation 1 restricted to Partitions.
    public func openPartition(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Partition? {
        guard let node = try await open(
            name,
            expecting: .partition,
            in: parent,
            transaction: transaction
        ) else {
            return nil
        }
        return try requirePartition(node, operation: .open)
    }

    /// Operation 2 restricted to Partitions.
    public func openOrCreatePartition(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Partition {
        let node = try await openOrCreate(
            name,
            layer: .partition,
            in: parent,
            transaction: transaction
        )
        return try requirePartition(node, operation: .write)
    }

    /// Walks `path` below `parent` with operation 1; `nil` when any step is
    /// absent. Intermediate steps may be Directories or Partitions.
    public func openDirectory(
        at path: DirectoryPath,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        var current = parent
        for component in path.components {
            guard let next = try await open(
                component,
                expecting: nil,
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = next
        }
        return current
    }

    /// Walks `path` below `parent` with operation 2, creating plain
    /// Directories for every absent component.
    public func openOrCreateDirectory(
        at path: DirectoryPath,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        var current = parent
        for component in path.components {
            current = try await openOrCreate(
                component,
                layer: .default,
                in: current,
                transaction: transaction
            )
        }
        return current
    }

    /// Resolves an absolute address from the root with read operations only.
    public func openDirectory(
        at address: StorageAddress,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        guard var current = try await openRoot(transaction: transaction) else {
            return nil
        }
        for component in address.components {
            guard let next = try await open(
                component,
                expecting: nil,
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = next
        }
        return current
    }

    private func requirePartition(
        _ node: Directory,
        operation: StorageOperation
    ) throws -> Partition {
        guard let partition = Partition(node) else {
            throw StorageError.directoryLayerMismatch(
                "Node is not tagged as a Partition",
                operation: operation,
                backend: backend
            )
        }
        return partition
    }
}
