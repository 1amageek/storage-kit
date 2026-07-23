import Synchronization

/// Single-assignment state shared by versionstamp requests for one transaction.
package final class TransactionVersionstampCompletion: Sendable {
    private final class Waiter: Sendable {
        let continuation: CheckedContinuation<Void, Never>
        let next: Waiter?

        init(
            continuation: CheckedContinuation<Void, Never>,
            next: Waiter?
        ) {
            self.continuation = continuation
            self.next = next
        }
    }

    private struct State: Sendable {
        var result: Result<TransactionVersionstamp, StorageError>?
        var firstWaiter: Waiter?
    }

    private let state: Mutex<State>

    package init(
        resolved result: Result<TransactionVersionstamp, StorageError>? = nil
    ) {
        self.state = Mutex(
            State(result: result, firstWaiter: nil)
        )
    }

    package func wait() async throws(StorageError)
        -> TransactionVersionstamp {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.result != nil {
                    continuation.resume()
                } else {
                    state.firstWaiter = Waiter(
                        continuation: continuation,
                        next: state.firstWaiter
                    )
                }
            }
        }
        return try state.withLock { state throws(StorageError) in
            guard let result = state.result else {
                preconditionFailure(
                    "Versionstamp completion resumed before resolution"
                )
            }
            return try result.get()
        }
    }

    package func resolveIfPending(
        _ result: Result<TransactionVersionstamp, StorageError>
    ) {
        let firstWaiter = state.withLock { state -> Waiter? in
            guard state.result == nil else {
                return nil
            }
            state.result = result
            let firstWaiter = state.firstWaiter
            state.firstWaiter = nil
            return firstWaiter
        }
        var waiter = firstWaiter
        while let currentWaiter = waiter {
            currentWaiter.continuation.resume()
            waiter = currentWaiter.next
        }
    }
}
