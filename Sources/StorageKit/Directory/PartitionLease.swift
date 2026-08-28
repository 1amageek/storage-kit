/// Exclusive, noncopyable handle that binds one Partition for data access.
///
/// A lease is issued only by `StorageEngine.leasePartition` after the
/// Partition was re-validated in the caller's transaction. While the lease is
/// active, operations 7 and 8 over the Partition or any ancestor are rejected
/// with `directoryLeased`. Data access happens only inside `withReadAccess`
/// or `withWriteAccess`; the bound access and every cursor it produced stop
/// working when the closure returns or the lease is released.
public struct PartitionLease: ~Copyable, Sendable {
    public let partition: Partition
    private let registration: LeaseRegistration
    private let bounds: PartitionKeyBounds

    init(
        partition: Partition,
        registration: LeaseRegistration,
        bounds: PartitionKeyBounds
    ) {
        self.partition = partition
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
        try requireBinding(of: transaction, operation: .read)
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
        try requireBinding(of: transaction, operation: .write)
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

    private func requireBinding(
        of transaction: any TransactionReadAccess,
        operation: StorageOperation
    ) throws {
        guard transaction.transactionDomain === partition.domain else {
            throw StorageError.storageDomainMismatch(
                "Transaction belongs to a different storage engine than the leased Partition",
                operation: operation,
                backend: bounds.backend
            )
        }
        try registration.requireActive(operation: operation, backend: bounds.backend)
    }

    deinit {
        registration.release()
    }
}
