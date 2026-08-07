import Testing
@testable import StorageKit

@Suite("Active Transaction Context Tests")
struct ActiveTransactionContextTests {
    @Test("Inherited task lookup closes with the dynamic operation")
    func inheritedTaskCannotReuseClosedTransaction() async throws {
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let releaseChild = TransactionContextTestGate()

        let inheritedTask = await ActiveTransactionContext
            .withActiveTransaction(transaction) { _ in
                #expect(ActiveTransactionContext.isActive)
                return Task {
                    await releaseChild.wait()
                    return ActiveTransactionContext.isActive
                }
            }

        #expect(!ActiveTransactionContext.isActive)
        await releaseChild.release()
        #expect(await inheritedTask.value == false)

        try await transaction.cancel()
        await engine.shutdown()
    }

    @Test("Owner waits for acquired nested transaction leases")
    func ownerDrainsNestedTransactionLeasesBeforeReturning() async throws {
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let leaseAcquired = TransactionContextTestGate()
        let releaseLease = TransactionContextTestGate()
        let observation = TransactionContextTestObservation()

        let owner = Task {
            await ActiveTransactionContext.withActiveTransaction(
                transaction
            ) { _ in
                Task {
                    let lease = ActiveTransactionContext
                        .acquireCurrentTransaction()
                    #expect(lease != nil)
                    await leaseAcquired.release()
                    await releaseLease.wait()
                    lease?.release()
                }
                await leaseAcquired.wait()
            }
            await observation.markOwnerReturned()
        }

        await leaseAcquired.wait()
        #expect(await observation.ownerReturned == false)
        await releaseLease.release()
        await owner.value
        #expect(await observation.ownerReturned)

        try await transaction.cancel()
        await engine.shutdown()
    }

    @Test("Throwing owner preserves the error after draining nested leases")
    func throwingOwnerDrainsNestedLeasesBeforeRethrowing() async throws {
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let leaseAcquired = TransactionContextTestGate()
        let releaseLease = TransactionContextTestGate()
        let observation = TransactionContextTestObservation()

        let owner = Task {
            do {
                try await ActiveTransactionContext.withActiveTransaction(
                    transaction
                ) { _ in
                    Task {
                        let lease = ActiveTransactionContext
                            .acquireCurrentTransaction()
                        #expect(lease != nil)
                        await leaseAcquired.release()
                        await releaseLease.wait()
                        lease?.release()
                    }
                    await leaseAcquired.wait()
                    throw TransactionContextTestError.expected
                }
                return false
            } catch TransactionContextTestError.expected {
                await observation.markOwnerReturned()
                return true
            } catch {
                return false
            }
        }

        await leaseAcquired.wait()
        #expect(await observation.ownerReturned == false)
        await releaseLease.release()
        #expect(await owner.value)
        #expect(await observation.ownerReturned)

        try await transaction.cancel()
        await engine.shutdown()
    }
}

private enum TransactionContextTestError: Error {
    case expected
}

private actor TransactionContextTestGate {
    private var isReleased = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        await withCheckedContinuation { continuation in
            guard !isReleased else {
                continuation.resume()
                return
            }
            waiters.append(continuation)
        }
    }

    func release() {
        guard !isReleased else { return }
        isReleased = true
        let waiters = waiters
        self.waiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
    }
}

private actor TransactionContextTestObservation {
    private(set) var ownerReturned = false

    func markOwnerReturned() {
        ownerReturned = true
    }
}
