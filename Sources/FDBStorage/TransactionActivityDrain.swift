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
            let isResolved = state.withLock { state -> Bool in
                if state.isResolved {
                    return true
                }
                state.waiters.append(continuation)
                return false
            }
            if isResolved {
                continuation.resume()
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
