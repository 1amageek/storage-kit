import Testing

@testable import FDBStorage

@Suite("FoundationDB transaction activity drain")
struct TransactionActivityDrainTests {
    @Test
    func pendingAndLateWaitersCompleteAfterResolution() async {
        let drain = TransactionActivityDrain()
        let pendingWaiters = (0..<16).map { _ in
            Task {
                await drain.wait()
            }
        }

        await Task.yield()
        drain.resolveIfPending()

        for waiter in pendingWaiters {
            await waiter.value
        }
        await drain.wait()
    }

    @Test
    func repeatedResolutionIsIdempotent() async {
        let drain = TransactionActivityDrain()

        drain.resolveIfPending()
        drain.resolveIfPending()

        await drain.wait()
    }
}
