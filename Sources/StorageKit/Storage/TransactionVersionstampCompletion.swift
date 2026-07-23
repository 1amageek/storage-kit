import Synchronization

/// Single-assignment state shared by versionstamp requests for one transaction.
package final class TransactionVersionstampCompletion: Sendable {
    private struct State: Sendable {
        var result: Result<TransactionVersionstamp, StorageError>?
        var waiters: [
            CheckedContinuation<
                Result<TransactionVersionstamp, StorageError>,
                Never
            >
        ] = []
    }

    private let state: Mutex<State>

    package init(
        resolved result: Result<TransactionVersionstamp, StorageError>? = nil
    ) {
        self.state = Mutex(State(result: result))
    }

    package func wait() async
        -> Result<TransactionVersionstamp, StorageError> {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if let result = state.result {
                    continuation.resume(returning: result)
                } else {
                    state.waiters.append(continuation)
                }
            }
        }
    }

    package func resolveIfPending(
        _ result: Result<TransactionVersionstamp, StorageError>
    ) {
        let waiters = state.withLock { state
            -> [
                CheckedContinuation<
                    Result<TransactionVersionstamp, StorageError>,
                    Never
                >
            ]? in
            guard state.result == nil else {
                return nil
            }
            state.result = result
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        guard let waiters else { return }
        for waiter in waiters {
            waiter.resume(returning: result)
        }
    }
}
