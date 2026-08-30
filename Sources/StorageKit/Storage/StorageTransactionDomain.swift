import Synchronization

/// Identifies the storage engine instance that owns a transaction.
///
/// Active transaction reuse, Directory operations, and Partition leases are
/// valid only when every participant belongs to this exact domain. Reference
/// identity intentionally defines equality.
///
/// The domain also owns the shutdown gate on lease issuance. That gate is
/// process-local because it describes this engine instance's own lifecycle,
/// not the state of the store: whether a Partition still exists is answered by
/// the store itself, in the caller's transaction, at issuance and at every
/// bind.
public final class StorageTransactionDomain: Sendable {
    private let shutdownRequested = Mutex(false)

    public init() {}

    /// Rejects every later lease issuance.
    ///
    /// Leases that were already issued stay valid until they are released, so
    /// in-flight bindings finish deterministically instead of failing midway.
    package func requestShutdown() {
        shutdownRequested.withLock { $0 = true }
    }

    /// Fails when lease issuance has been closed by `requestShutdown()`.
    package func requireLeaseIssuance(backend: StorageBackend) throws {
        guard !shutdownRequested.withLock({ $0 }) else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .open,
                backend: backend,
                message: "Partition leases cannot be issued after shutdown was requested"
            )
        }
    }
}
