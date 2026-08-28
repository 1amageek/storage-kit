/// Identifies the storage engine instance that owns a transaction.
///
/// Active transaction reuse, Directory operations, and Partition leases are
/// valid only when every participant belongs to this exact domain. Reference
/// identity intentionally defines equality.
public final class StorageTransactionDomain: Sendable {
    /// Process-local Partition lease and subtree-intent registry of this domain.
    package let leases: PartitionLeaseRegistry

    public init() {
        self.leases = PartitionLeaseRegistry()
    }
}
