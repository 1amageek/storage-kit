/// A single-assignment completion shared by every caller that observes the
/// same transaction lifecycle operation.
public actor TransactionOperationCompletion {
    private enum State: Sendable {
        case pending
        case succeeded
        case failed(StorageError)
    }

    private var state = State.pending
    private var waiters: [
        CheckedContinuation<Result<Void, StorageError>, Never>
    ] = []

    public init() {}

    public func wait() async -> Result<Void, StorageError> {
        switch state {
        case .succeeded:
            return .success(())
        case .failed(let error):
            return .failure(error)
        case .pending:
            return await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
        }
    }

    public func resolve(_ result: Result<Void, StorageError>) {
        guard case .pending = state else {
            preconditionFailure("Transaction completion resolved more than once")
        }
        switch result {
        case .success:
            state = .succeeded
        case .failure(let error):
            state = .failed(error)
        }
        let pendingWaiters = waiters
        waiters.removeAll(keepingCapacity: false)
        for waiter in pendingWaiters {
            waiter.resume(returning: result)
        }
    }
}
