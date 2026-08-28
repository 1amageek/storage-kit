import Synchronization

/// Lifetime of one `withReadAccess` / `withWriteAccess` binding.
///
/// Cursors produced inside the binding capture the scope and fail on advance
/// once it has closed, so no cursor outlives the bound access it came from.
final class PartitionBindingScope: Sendable {
    private let open = Mutex(true)

    init() {}

    var isOpen: Bool {
        open.withLock { $0 }
    }

    func close() {
        open.withLock { $0 = false }
    }

    func requireOpen(
        operation: StorageOperation,
        backend: StorageBackend
    ) throws {
        guard isOpen else {
            throw StorageError.staleLease(
                "Partition binding scope has ended",
                operation: operation,
                backend: backend
            )
        }
    }
}
