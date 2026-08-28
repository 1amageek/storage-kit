import Synchronization

/// Registry-backed identity of one Partition lease.
///
/// The registration is the shared, `Sendable` core behind a noncopyable
/// `PartitionLease` and the bound access values and cursors it produces.
/// Release is idempotent and happens exactly once against the registry.
package final class LeaseRegistration: Sendable {
    package let id: UInt64
    package let address: StorageAddress
    private let registry: PartitionLeaseRegistry
    private let active = Mutex(true)

    init(id: UInt64, address: StorageAddress, registry: PartitionLeaseRegistry) {
        self.id = id
        self.address = address
        self.registry = registry
    }

    package var isActive: Bool {
        active.withLock { $0 }
    }

    /// Returns `true` when this call performed the release.
    @discardableResult
    package func release() -> Bool {
        let wasActive = active.withLock { state in
            let was = state
            state = false
            return was
        }
        if wasActive {
            registry.release(registrationID: id)
        }
        return wasActive
    }

    package func requireActive(
        operation: StorageOperation,
        backend: StorageBackend
    ) throws {
        guard isActive else {
            throw StorageError.staleLease(
                "Partition lease has been released",
                operation: operation,
                backend: backend
            )
        }
    }

    deinit {
        release()
    }
}
