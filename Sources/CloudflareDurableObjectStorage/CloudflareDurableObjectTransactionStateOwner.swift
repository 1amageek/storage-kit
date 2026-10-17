import Synchronization

/// Shared host state retained independently of journal cursor authority.
final class CloudflareDurableObjectTransactionStateOwner: Sendable {
    private let state = Mutex(CloudflareDurableObjectTransactionState())

    func withLock<Result>(
        _ body: (inout sending CloudflareDurableObjectTransactionState) throws -> sending Result
    ) rethrows -> sending Result {
        try state.withLock(body)
    }
}
