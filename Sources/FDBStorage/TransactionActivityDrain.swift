import Synchronization

/// Completion for activity that was already admitted when cancellation began.
final class TransactionActivityDrain: Sendable {
    private struct State: Sendable {
        var isResolved = false
        var waiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    func wait() async {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.isResolved {
                    continuation.resume()
                } else {
                    state.waiters.append(continuation)
                }
            }
        }
    }

    func resolveIfPending() {
        let waiters = state.withLock { state -> [
            CheckedContinuation<Void, Never>
        ]? in
            guard !state.isResolved else {
                return nil
            }
            state.isResolved = true
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        guard let waiters else { return }
        for waiter in waiters {
            waiter.resume()
        }
    }
}
