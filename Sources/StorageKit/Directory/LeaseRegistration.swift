import Synchronization

/// Shared, `Sendable` identity of one Partition lease.
///
/// The registration is the core behind a noncopyable `PartitionLease` and the
/// bound access values and cursors it produces, so a cursor that outlived its
/// binding observes the release. Release is idempotent.
package final class LeaseRegistration: Sendable {
    package let address: StorageAddress
    private let active = Mutex(true)

    init(address: StorageAddress) {
        self.address = address
    }

    package var isActive: Bool {
        active.withLock { $0 }
    }

    /// Returns `true` when this call performed the release.
    @discardableResult
    package func release() -> Bool {
        active.withLock { state in
            let was = state
            state = false
            return was
        }
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
}
