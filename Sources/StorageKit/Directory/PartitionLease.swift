/// Exclusive, noncopyable handle that binds one Partition for data access.
///
/// A lease is issued only by `StorageEngine.leasePartition`, which resolves the
/// Partition in the caller's transaction. It does not stop anyone from removing
/// that Partition: whether such a removal is admissible is a database-operation
/// decision made above StorageKit. What the lease guarantees is that stale work
/// fails instead of landing somewhere else — every binding re-resolves the
/// Partition in its own transaction and fails with `staleLease` when the live
/// node is a different generation.
///
/// Data access happens only inside `withReadAccess` or `withWriteAccess`; the
/// bound access and every cursor it produced stop working when the closure
/// returns or the lease is released.
public struct PartitionLease: ~Copyable, Sendable {
    public let partition: Partition
    private let directoryAccess: any DirectoryAccess
    private let registration: LeaseRegistration
    private let bounds: PartitionKeyBounds

    init(
        partition: Partition,
        directoryAccess: any DirectoryAccess,
        registration: LeaseRegistration,
        bounds: PartitionKeyBounds
    ) {
        self.partition = partition
        self.directoryAccess = directoryAccess
        self.registration = registration
        self.bounds = bounds
    }

    public var isActive: Bool {
        registration.isActive
    }

    /// Releases the lease immediately instead of at end of lifetime.
    public consuming func release() {
        registration.release()
    }

    /// Binds read access to the Partition inside `transaction`.
    public nonisolated(nonsending) func withReadAccess<R>(
        _ transaction: any TransactionReadAccess,
        _ body: (borrowing BoundReadAccess) async throws -> R
    ) async throws -> R {
        try await requireBinding(of: transaction, operation: .read)
        let scope = PartitionBindingScope()
        defer { scope.close() }
        let access = BoundReadAccess(
            partition: partition,
            transaction: transaction,
            bounds: bounds,
            registration: registration,
            scope: scope
        )
        return try await body(access)
    }

    /// Binds read and write access to the Partition inside `transaction`.
    public nonisolated(nonsending) func withWriteAccess<R>(
        _ transaction: any TransactionAccess,
        _ body: (borrowing BoundWriteAccess) async throws -> R
    ) async throws -> R {
        try await requireBinding(of: transaction, operation: .write)
        let scope = PartitionBindingScope()
        defer { scope.close() }
        let access = BoundWriteAccess(
            reads: BoundReadAccess(
                partition: partition,
                transaction: transaction,
                bounds: bounds,
                registration: registration,
                scope: scope
            ),
            transaction: transaction
        )
        return try await body(access)
    }

    /// Validates the binding: same domain, lease still held, and the Partition
    /// still the generation this lease was issued for.
    ///
    /// The generation walk runs on every bind, including a bind to the
    /// transaction that issued the lease. Exempting that transaction would
    /// require the lease to recognize it, and the object identity a lease could
    /// remember is reusable once the transaction is deallocated, so the
    /// exemption would eventually skip validation for a different transaction.
    ///
    /// A bind therefore costs one address walk: the root read plus one read per
    /// address component. Hold a binding for the span of the work it covers
    /// rather than opening one per key.
    private func requireBinding(
        of transaction: any TransactionReadAccess,
        operation: StorageOperation
    ) async throws {
        guard transaction.transactionDomain === partition.domain else {
            throw StorageError.storageDomainMismatch(
                "Transaction belongs to a different storage engine than the leased Partition",
                operation: operation,
                backend: bounds.backend
            )
        }
        try registration.requireActive(operation: operation, backend: bounds.backend)
        try await directoryAccess.requirePartitionGeneration(
            partition,
            operation: operation,
            transaction: transaction
        )
    }

    deinit {
        registration.release()
    }
}
