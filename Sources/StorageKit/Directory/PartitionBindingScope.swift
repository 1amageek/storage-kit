import Synchronization

/// Lifetime of one `withReadAccess` / `withWriteAccess` binding, and owner of
/// every cursor that binding issued.
///
/// Cursors produced inside the binding capture the scope and fail on advance
/// once it has closed, so no cursor outlives the bound access it came from.
/// Invalidation alone would leave the backend iterator, an advance still in
/// flight, and the native transaction retention outstanding, so the scope also
/// holds each cursor it issued and completes that cursor's cleanup when it
/// closes. Nothing an issued cursor can still do survives the binding.
final class PartitionBindingScope: Sendable {
    private struct State {
        var isOpen = true
        var cursors: [KeyValueCursor] = []
    }

    private let state = Mutex(State())

    init() {}

    var isOpen: Bool {
        state.withLock { $0.isOpen }
    }

    /// Takes ownership of a cursor issued inside this binding.
    ///
    /// A scope that has already closed adopts nothing: the cursor is dropped
    /// unopened, which is the abandonment path its own deinit is written for.
    func adopt(
        _ cursor: KeyValueCursor,
        operation: StorageOperation,
        backend: StorageBackend
    ) throws {
        try state.withLock { state in
            guard state.isOpen else {
                throw StorageError.staleLease(
                    "Partition binding scope has ended",
                    operation: operation,
                    backend: backend
                )
            }
            state.cursors.append(cursor)
        }
    }

    /// Ends the binding and completes the cleanup of every cursor it issued.
    ///
    /// Cursors are finished newest first, so a cursor opened over state a later
    /// one depends on is released last. Cleanup runs for every cursor even
    /// after one of them fails, and runs outside the lock because it awaits
    /// backend work. The returned failures are the ones this call caused; a
    /// cursor already terminal, or one another caller is finishing, is awaited
    /// without restating a failure that caller already received.
    func close() async -> [any Error] {
        let cursors = state.withLock { state -> [KeyValueCursor] in
            state.isOpen = false
            defer { state.cursors = [] }
            return state.cursors
        }

        var failures: [any Error] = []
        for var cursor in cursors.reversed() {
            do {
                try await cursor.completeCleanup()
            } catch {
                failures.append(error)
            }
        }
        return failures
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
