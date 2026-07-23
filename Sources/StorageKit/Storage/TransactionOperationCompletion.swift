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

    public func resolve(_ result: Result<Void, StorageError>) {
        let firstWaiter = state.withLock { state -> Waiter? in
            guard case .pending = state.resolution else {
                preconditionFailure(
                    "Transaction completion resolved more than once"
                )
            }
            switch result {
            case .success:
                state.resolution = .succeeded
            case .failure(let error):
                state.resolution = .failed(error)
            }
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
