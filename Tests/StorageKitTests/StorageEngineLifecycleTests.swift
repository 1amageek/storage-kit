@testable import StorageKit
import Synchronization
import Testing

@Suite("Storage Engine Lifecycle Tests")
struct StorageEngineLifecycleTests {
    @Test func synchronousShutdownPreparesResourceExactlyOnce() async {
        let lifecycle = StorageEngineLifecycle()
        let preparationCount = Mutex(0)

        lifecycle.requestShutdown {
            #expect(lifecycle.isShutdownRequested)
            preparationCount.withLock { $0 += 1 }
        }
        lifecycle.requestShutdown {
            Issue.record("Shutdown preparation must run exactly once")
        }

        await lifecycle.waitUntilShutdown()
        #expect(preparationCount.withLock { $0 } == 1)
    }

    @Test func asynchronousShutdownSharesAuthoritativeCompletion() async {
        let lifecycle = StorageEngineLifecycle()
        let gate = StorageShutdownGate()

        lifecycle.requestShutdown {
            await gate.performCleanup()
        }
        lifecycle.requestShutdown {
            Issue.record("Shutdown cleanup must start exactly once")
        }

        await gate.waitUntilCleanupStarts()
        let firstWaiter = Task {
            await lifecycle.waitUntilShutdown()
        }
        let secondWaiter = Task {
            await lifecycle.waitUntilShutdown()
        }
        await Task.yield()
        #expect(!gate.isReleased)

        gate.releaseCleanup()
        await firstWaiter.value
        await secondWaiter.value

        #expect(gate.cleanupCount == 1)
    }

    @Test func shutdownPhaseRejectsAdmissionBeforeCleanupCompletes() async {
        let lifecycle = StorageEngineLifecycle()
        let gate = StorageShutdownGate()

        lifecycle.requestShutdown {
            await gate.performCleanup()
        }
        await gate.waitUntilCleanupStarts()

        do {
            try lifecycle.requireActive(
                backend: .inMemory,
                operation: .beginTransaction
            )
            Issue.record("Expected admission to close before cleanup completes")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .beginTransaction)
            #expect(error.backend == .inMemory)
        } catch {
            Issue.record("Expected StorageError, got \(error)")
        }

        gate.releaseCleanup()
        await lifecycle.waitUntilShutdown()
    }

    @Test func engineRejectsTransactionsAfterShutdownRequest() async throws {
        let engine = InMemoryEngine()
        engine.requestShutdown()
        await engine.waitUntilShutdown()

        do {
            _ = try engine.createTransaction()
            Issue.record("Expected shutdown engine to reject new transactions")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .beginTransaction)
            #expect(error.backend == .inMemory)
        } catch {
            Issue.record("Expected StorageError, got \(error)")
        }
    }
}

private final class StorageShutdownGate: Sendable {
    private struct State: Sendable {
        var cleanupCount = 0
        var isReleased = false
        var startWaiters: [CheckedContinuation<Void, Never>] = []
        var releaseWaiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    var cleanupCount: Int {
        state.withLock { $0.cleanupCount }
    }

    var isReleased: Bool {
        state.withLock { $0.isReleased }
    }

    func performCleanup() async {
        let startWaiters = state.withLock { state in
            state.cleanupCount += 1
            let waiters = state.startWaiters
            state.startWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in startWaiters {
            waiter.resume()
        }

        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                guard !state.isReleased else { return true }
                state.releaseWaiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func waitUntilCleanupStarts() async {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                guard state.cleanupCount == 0 else { return true }
                state.startWaiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func releaseCleanup() {
        let waiters = state.withLock { state in
            guard !state.isReleased else {
                return [CheckedContinuation<Void, Never>]()
            }
            state.isReleased = true
            let waiters = state.releaseWaiters
            state.releaseWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }
}
