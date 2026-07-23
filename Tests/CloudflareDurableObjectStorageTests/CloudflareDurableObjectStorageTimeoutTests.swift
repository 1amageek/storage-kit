import StorageKit
import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Storage Timeout Tests")
struct CloudflareDurableObjectStorageTimeoutTests {
    private enum ClockFailure: Error {
        case unavailable
    }

    private struct FailingClock: StorageMonotonicClock {
        var now: ContinuousClock.Instant {
            ContinuousClock().now
        }

        func sleep(until deadline: ContinuousClock.Instant) async throws {
            throw ClockFailure.unavailable
        }
    }

    @Test func readTimeoutTerminatesTransactionWithAuthoritativeFailure() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .read
        )
        let transaction = try makeTransaction(client: client)
        try transaction.setOption(
            forOption: .timeout(milliseconds: 25)
        )

        do {
            _ = try await transaction.getValue(for: [0x01])
            Issue.record("Expected the read to time out")
        } catch let error as StorageError {
            #expect(error.code == .transactionTimedOut)
            #expect(error.operation == .read)
        }

        do {
            _ = try await transaction.getValue(for: [0x01])
            Issue.record("Expected the timed-out transaction failure to be retained")
        } catch let error as StorageError {
            #expect(error.code == .transactionTimedOut)
            #expect(error.operation == .read)
        }
        let count = await client.invocationCount(for: .read)
        #expect(count == 1)
    }

    @Test func lazyRangeTimeoutCancelsTransaction() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .range
        )
        let transaction = try makeTransaction(client: client)
        try transaction.setOption(
            forOption: .timeout(milliseconds: 25)
        )
        var iterator = transaction
            .getRange(begin: [0x01], end: [0x02])
            .makeAsyncIterator()

        do {
            _ = try await iterator.next()
            Issue.record("Expected the range page to time out")
        } catch let error as StorageError {
            #expect(error.code == .transactionTimedOut)
            #expect(error.operation == .rangeRead)
        }

        let count = await client.invocationCount(for: .range)
        #expect(count == 1)
    }

    @Test func expiredDeadlinePreventsCommitDispatch() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .commit
        )
        let transaction = try makeTransaction(client: client)
        try transaction.setValue([0x10], for: [0x01])
        try transaction.setOption(
            forOption: .timeout(milliseconds: 1)
        )
        try await Task.sleep(for: .milliseconds(10))

        do {
            try await transaction.commit()
            Issue.record("Expected the commit preflight to time out")
        } catch let error as StorageError {
            #expect(error.code == .transactionTimedOut)
            #expect(error.operation == .commit)
        }

        let count = await client.invocationCount(for: .commit)
        #expect(count == 0)
    }

    @Test func timeoutAfterCommitDispatchProducesUnknownOutcome() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .commit
        )
        let transaction = try makeTransaction(client: client)
        try transaction.setValue([0x10], for: [0x01])
        try transaction.setOption(
            forOption: .timeout(milliseconds: 250)
        )

        let commitTask = Task {
            try await transaction.commit()
        }
        await client.waitUntilStarted(.commit)

        do {
            try await commitTask.value
            Issue.record("Expected an unknown commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
        }

        do {
            try await transaction.commit()
            Issue.record("Expected the commit-unknown transaction to be closed")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        let count = await client.invocationCount(for: .commit)
        #expect(count == 1)
    }

    @Test func unsupportedOptionsFailWithoutClosingTransaction() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .read
        )
        let transaction = try makeTransaction(client: client)

        #expect(transaction.capabilities.transactionTimeout)
        #expect(!transaction.capabilities.schedulingPriority)
        #expect(!transaction.capabilities.readPriority)
        #expect(!transaction.capabilities.historicalReadVersion)

        #expect(throws: StorageError.self) {
            try transaction.setOption(forOption: .priorityBatch)
        }
        #expect(throws: StorageError.self) {
            try transaction.setOption(forOption: .readPriorityHigh)
        }
        try transaction.setOption(forOption: .timeout(milliseconds: 0))

        #expect(throws: StorageError.self) {
            try transaction.setOption(
                to: [0x01],
                forOption: .priorityBatch
            )
        }
        #expect(throws: StorageError.self) {
            try transaction.setOption(
                to: 2,
                forOption: .timeout(milliseconds: 1)
            )
        }
        #expect(throws: StorageError.self) {
            try transaction.setOption(
                forOption: .timeout(milliseconds: -1)
            )
        }

        try await transaction.cancel()
    }

    @Test func clockFailureIsNotTreatedAsTimeoutSuccess() async throws {
        let client = SuspendingCloudflareDurableObjectStorageClient(
            suspending: .read
        )
        let transaction = try makeTransaction(
            client: client,
            clock: FailingClock()
        )
        try transaction.setOption(
            forOption: .timeout(milliseconds: 25)
        )

        do {
            _ = try await transaction.getValue(for: [0x01])
            Issue.record("Expected the clock failure to propagate")
        } catch let error as StorageError {
            #expect(error.code == .backendFailure)
            #expect(error.operation == .read)
            #expect(error.underlyingDescription?.contains("unavailable") == true)
        }
    }

    private func makeTransaction(
        client: any CloudflareDurableObjectStorageClient,
        clock: any StorageMonotonicClock = SystemStorageClock()
    ) throws -> CloudflareDurableObjectStorageTransaction {
        CloudflareDurableObjectStorageTransaction(
            scope: try CloudflareDurableObjectStorageScope(databaseID: "main"),
            client: client,
            limits: .default,
            monotonicClock: clock
        )
    }
}
