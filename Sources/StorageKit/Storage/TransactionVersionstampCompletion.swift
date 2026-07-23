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

    private enum Resolution: Sendable {
        case pending
        case succeeded(TransactionVersionstamp)
        case failed(StorageError)
    }

    private struct State: Sendable {
        var resolution: Resolution
        var firstWaiter: Waiter?
    }

    private let state: Mutex<State>

    package init() {
        self.state = Mutex(
            State(resolution: .pending, firstWaiter: nil)
        )
    }

    package init(failure: StorageError) {
        self.state = Mutex(
            State(resolution: .failed(failure), firstWaiter: nil)
        )
    }

    package func wait() async throws(StorageError)
        -> TransactionVersionstamp {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                switch state.resolution {
                case .pending:
                    state.firstWaiter = Waiter(
                        continuation: continuation,
                        next: state.firstWaiter
                    )
                case .succeeded, .failed:
                    continuation.resume()
                }
            }
        }
        return try state.withLock { state throws(StorageError) in
            switch state.resolution {
            case .succeeded(let versionstamp):
                return versionstamp
            case .failed(let error):
                throw error
            case .pending:
                preconditionFailure(
                    "Versionstamp completion resumed before resolution"
                )
            }
        }
    }

    package func succeed(_ versionstamp: TransactionVersionstamp) {
        resolveIfPending(.succeeded(versionstamp))
    }

    package func fail(_ error: StorageError) {
        resolveIfPending(.failed(error))
    }

    private func resolveIfPending(_ resolution: Resolution) {
        let firstWaiter = state.withLock { state -> Waiter? in
            guard case .pending = state.resolution else {
                return nil
            }
            state.resolution = resolution
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
