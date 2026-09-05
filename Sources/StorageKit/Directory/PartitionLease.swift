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
/// Data access happens only inside `withReadAccess` or `withWriteAccess`. The
/// bound access is borrowed for the closure, and the binding owns every cursor
/// it issued: closing it completes that cursor's backend cleanup, so a cursor
/// the caller kept is already finished rather than merely refused.
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
    ///
    /// A read binding promises the leased generation for the span of the
    /// closure, not for the instant of its generation walk. A backend whose
    /// reads each take a fresh snapshot cannot keep that promise: the
    /// Partition can be removed after the walk, and the closure's reads then
    /// return nothing, reporting a removed Partition as an empty one. Such a
    /// backend refuses the binding here, before any I/O.
    ///
    /// The binding closes on both paths, and closing completes the cleanup of
    /// every cursor it issued. A cleanup failure is reported as
    /// `PartitionBindingCleanupError`, which also carries the closure's failure
    /// when there was one.
    public nonisolated(nonsending) func withReadAccess<R>(
        _ transaction: any TransactionReadAccess,
        _ body: (borrowing BoundReadAccess) async throws -> R
    ) async throws -> R {
        try await requireBinding(of: transaction, operation: .read)
        let scope = PartitionBindingScope()
        let access = BoundReadAccess(
            partition: partition,
            transaction: transaction,
            bounds: bounds,
            registration: registration,
            scope: scope
        )
        let outcome: Result<R, any Error>
        do {
            outcome = .success(try await body(access))
        } catch {
            outcome = .failure(error)
        }
        return try await Self.complete(outcome, closing: scope)
    }

    /// Binds read and write access to the Partition inside `transaction`.
    ///
    /// A write binding asks for more than a read binding: a write bound to
    /// this Partition's generation must conflict with that Partition's
    /// concurrent removal, which is the same read-then-write detection the
    /// catalog depends on, because the generation walk only reads the node the
    /// removal writes. A backend whose configured transaction semantics cannot
    /// produce that conflict refuses the write binding here, before any I/O,
    /// so a write is never admitted into a Partition the store may already
    /// have removed.
    public nonisolated(nonsending) func withWriteAccess<R>(
        _ transaction: any TransactionAccess,
        _ body: (borrowing BoundWriteAccess) async throws -> R
    ) async throws -> R {
        try await requireBinding(of: transaction, operation: .write)
        let scope = PartitionBindingScope()
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
        let outcome: Result<R, any Error>
        do {
            outcome = .success(try await body(access))
        } catch {
            outcome = .failure(error)
        }
        return try await Self.complete(outcome, closing: scope)
    }

    /// Closes `scope` and combines its cleanup failures with `outcome`.
    ///
    /// A value produced over storage whose cleanup failed is not a result, so a
    /// cleanup failure replaces a successful value as well as accompanying a
    /// failed one.
    private static func complete<R>(
        _ outcome: Result<R, any Error>,
        closing scope: PartitionBindingScope
    ) async throws -> R {
        let cleanupErrors = await scope.close()
        switch outcome {
        case .success(let value):
            guard cleanupErrors.isEmpty else {
                throw PartitionBindingCleanupError(
                    operationError: nil,
                    cursorCleanupErrors: cleanupErrors
                )
            }
            return value
        case .failure(let error):
            guard cleanupErrors.isEmpty else {
                throw PartitionBindingCleanupError(
                    operationError: error,
                    cursorCleanupErrors: cleanupErrors
                )
            }
            throw error
        }
    }

    /// Validates the binding: same domain, lease still held, the backend able
    /// to carry `operation`, and the Partition still the generation this lease
    /// was issued for.
    ///
    /// The three stages run in that order because each is a precondition of
    /// the next being meaningful. What this process already knows costs
    /// nothing and settles a caller error as itself rather than as the
    /// backend's refusal; the backend's admission is synchronous and decides
    /// whether the binding can exist at all; only then is a round trip worth
    /// spending on the generation.
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
        try requireLocalAuthority(of: transaction, operation: operation)
        try directoryAccess.admit(operation)
        try await requirePartitionGeneration(
            partition,
            operation: operation,
            access: directoryAccess,
            transaction: transaction
        )
    }

    /// The part of a binding this process settles without the backend: the
    /// transaction belongs to the Partition's engine, and the lease is still
    /// held.
    private func requireLocalAuthority(
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
