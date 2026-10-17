import Synchronization

/// Process-local validity of one unpublished node generation or path transition.
/// No transaction or journal is retained by a Directory value.
package final class DirectoryProvenance: Sendable {
    private enum State: Sendable {
        case unpublished
        case published
        case invalid
    }

    private let state = Mutex(State.unpublished)

    package init() {}

    package func publish() {
        state.withLock { state in
            if case .unpublished = state { state = .published }
        }
    }

    package func invalidate() {
        state.withLock { state in
            if case .unpublished = state { state = .invalid }
        }
    }

    package func requireValid(operation: StorageOperation, backend: StorageBackend) throws {
        let invalid = state.withLock { state in
            if case .invalid = state { return true }
            return false
        }
        if invalid {
            throw StorageError.staleLease(
                "Directory generation or path was discarded by its transaction",
                operation: operation,
                backend: backend
            )
        }
    }
}
