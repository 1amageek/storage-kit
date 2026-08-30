import DatabaseTypes

/// Storage placement authority of one engine: the five semantic Directory
/// operations plus root bootstrap from the root's own allocation authority.
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

    /// Operation 2 applied to the root: initializes the root layout when absent.
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

    /// Rejects, before any I/O, a mutation this backend's configured
    /// transaction semantics cannot carry.
    ///
    /// Both the catalog writes (operations 2, 4, 5 and `openOrInitializeRoot`)
    /// and a Partition write binding depend on the same detection: a row this
    /// transaction only read must conflict with a concurrent transaction that
    /// writes it. A backend whose configured isolation cannot produce that
    /// conflict narrows the capability here with `unsupportedOperation` instead
    /// of writing under semantics that admit a child below a removed parent or
    /// data inside a removed Partition.
    ///
    /// Reads are never gated: opening, listing, and leasing stay available at
    /// every isolation level. The default admits every operation.
    func admitMutation(_ operation: StorageOperation) throws
}

extension DirectoryAccess {
    public func admitMutation(_ operation: StorageOperation) throws {}

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

extension DirectoryAccess {
    /// Resolves `partition` in `transaction` and fails when the live node is a
    /// different generation than the one the caller holds.
    ///
    /// This is the single staleness test behind every Partition lease. The
    /// prefix allocator never reuses a number, so a Partition removed and
    /// recreated at the same address always carries a different
    /// `keyspacePrefix`; comparing prefixes therefore separates the two
    /// generations without any record of what else is running.
    ///
    /// The walk runs in `transaction`, which puts the resolution into that
    /// transaction's read set. A removal committing concurrently then makes
    /// `transaction` conflict rather than letting it write into a Partition
    /// that no longer exists.
    package func requirePartitionGeneration(
        _ partition: Partition,
        operation: StorageOperation,
        transaction: any TransactionReadAccess
    ) async throws {
        let current = try await openDirectory(
            at: partition.root.address,
            transaction: transaction
        )
        guard let current,
              current.layer.isPartition,
              current.keyspacePrefix == partition.keyspacePrefix
        else {
            throw StorageError.staleLease(
                "Partition no longer exists at its address with the same keyspace",
                operation: operation,
                backend: backend
            )
        }
    }
}
