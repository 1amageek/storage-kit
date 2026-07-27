import Synchronization

/// A single-assignment completion shared by every caller that observes the
/// same transaction lifecycle operation.
public final class TransactionOperationCompletion: Sendable {
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
        case succeeded
        case failed(StorageError)
    }

    private struct State: Sendable {
        var resolution: Resolution = .pending
        var firstWaiter: Waiter?
    }

    private let state = Mutex(State())

    public init() {}

    public func wait() async throws(StorageError) {
        await withCheckedContinuation { continuation in
            let isResolved = state.withLock { state -> Bool in
                switch state.resolution {
                case .pending:
                    state.firstWaiter = Waiter(
                        continuation: continuation,
                        next: state.firstWaiter
                    )
                    return false
                case .succeeded, .failed:
                    return true
                }
            }
            if isResolved {
                continuation.resume()
            }
        }
        try state.withLock { state throws(StorageError) in
            switch state.resolution {
            case .succeeded:
                return
            case .failed(let error):
                throw error
            case .pending:
                preconditionFailure(
                    "Transaction completion resumed before resolution"
                )
            }
        }
    }

    public func succeed() {
        resolve(.succeeded)
    }

    public func fail(_ error: StorageError) {
        resolve(.failed(error))
    }

    private func resolve(_ resolution: Resolution) {
        let firstWaiter = state.withLock { state -> Waiter? in
            guard case .pending = state.resolution else {
                preconditionFailure(
                    "Transaction completion resolved more than once"
                )
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
