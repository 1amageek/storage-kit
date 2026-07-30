import DatabaseTypes
import StorageKit
import Synchronization
import Testing

@Suite("Transaction Range Cleanup Tests")
struct TransactionRangeCleanupTests {
    @Test func successfulConsumptionFinishesExactlyOnce() async throws {
        let iterationRecorder = RangeIterationRecorder()
        let rows = FinishRecordingRows(
            rows: [([0x01], [0x11]), ([0x02], [0x12])],
            iterationRecorder: iterationRecorder
        )
        var observedKeys: [ByteString] = []

        try await rows.consumeRows { key, _ in
            observedKeys.append(key)
        }

        #expect(observedKeys == [[0x01], [0x02]])
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func bodyFailureStillAwaitsFinish() async {
        let iterationRecorder = RangeIterationRecorder()
        let rows = FinishRecordingRows(
            rows: [([0x01], [0x11]), ([0x02], [0x12])],
            iterationRecorder: iterationRecorder
        )

        do {
            try await rows.consumeRows { _, _ in
                throw RangeCleanupFailure.body
            }
            Issue.record("Expected body failure")
        } catch let error as RangeCleanupFailure {
            #expect(error == .body)
        } catch {
            Issue.record("Expected RangeCleanupFailure, got \(error)")
        }

        #expect(iterationRecorder.nextCount == 1)
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func dualFailurePreservesIterationAndCleanupErrors() async {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        let rows = FinishRecordingRows(
            rows: [([0x01], [0x11])],
            iterationRecorder: iterationRecorder
        )

        do {
            try await rows.consumeRows { _, _ in
                throw RangeCleanupFailure.body
            }
            Issue.record("Expected combined failure")
        } catch let error as StorageRangeCleanupError {
            #expect(error.iterationError as? RangeCleanupFailure == .body)
            #expect(error.cleanupError as? RangeCleanupFailure == .finish)
        } catch {
            Issue.record("Expected StorageRangeCleanupError, got \(error)")
        }

        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func cursorFinishesNaturalExhaustionExactlyOnce() async throws {
        let iterationRecorder = RangeIterationRecorder()
        var cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [([0x01], [0x11])],
                iterationRecorder: iterationRecorder
            )
        )

        let first = try await cursor.next()
        let exhausted = try await cursor.next()
        try await cursor.finish()

        #expect(first?.0 == [0x01])
        #expect(exhausted == nil)
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func concurrentCursorAdvancesAreRejected() async throws {
        let iterationGate = RangeIterationGate()
        let iterationRecorder = RangeIterationRecorder()
        let cursor = KeyValueCursor(
            consuming: SuspendedRows(
                iterationGate: iterationGate,
                iterationRecorder: iterationRecorder
            )
        )
        var firstCursor = cursor
        var secondCursor = cursor

        let firstAdvance = Task {
            try await firstCursor.next()
        }
        await iterationGate.waitUntilAdvanceStarts()

        do {
            _ = try await secondCursor.next()
            Issue.record("Expected concurrent cursor advance failure")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        } catch {
            Issue.record("Expected StorageError, got \(error)")
        }

        iterationGate.resumeAdvance(with: ([0x01], [0x11]))
        _ = try await firstAdvance.value
        try await secondCursor.finish()
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func finishWaitsForAdvanceAndCleansUpExactlyOnce() async throws {
        let iterationGate = RangeIterationGate()
        let iterationRecorder = RangeIterationRecorder()
        let cursor = KeyValueCursor(
            consuming: SuspendedRows(
                iterationGate: iterationGate,
                iterationRecorder: iterationRecorder
            )
        )
        let advance = Task {
            var advancingCursor = cursor
            return try await advancingCursor.next()
        }
        await iterationGate.waitUntilAdvanceStarts()

        let finish = Task {
            var finishingCursor = cursor
            try await finishingCursor.finish()
        }
        await Task.yield()
        #expect(iterationRecorder.finishCount == 0)

        iterationGate.resumeAdvance(with: ([0x01], [0x11]))
        let row = try await advance.value
        try await finish.value
        var repeatedFinishCursor = cursor
        try await repeatedFinishCursor.finish()

        #expect(row?.0 == [0x01])
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func cursorPreservesAdvanceAndCleanupFailures() async {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        var cursor = KeyValueCursor(
            consuming: FailingRows(iterationRecorder: iterationRecorder)
        )

        do {
            _ = try await cursor.next()
            Issue.record("Expected combined failure")
        } catch let error as StorageRangeCleanupError {
            #expect(error.iterationError as? RangeCleanupFailure == .body)
            #expect(error.cleanupError as? RangeCleanupFailure == .finish)
        } catch {
            Issue.record("Expected StorageRangeCleanupError, got \(error)")
        }

        #expect(iterationRecorder.finishCount == 1)
    }

}

private enum RangeCleanupFailure: Error, Equatable, Sendable {
    case body
    case finish
}

private final class RangeIterationRecorder: Sendable {
    private struct State: Sendable {
        var nextCount = 0
        var finishCount = 0
    }

    private let state = Mutex(State())
    let finishError: RangeCleanupFailure?

    init(finishError: RangeCleanupFailure? = nil) {
        self.finishError = finishError
    }

    var nextCount: Int {
        state.withLock { $0.nextCount }
    }

    var finishCount: Int {
        state.withLock { $0.finishCount }
    }

    func recordNext() {
        state.withLock { $0.nextCount += 1 }
    }

    func recordFinish() {
        state.withLock { $0.finishCount += 1 }
    }
}

private struct FinishRecordingRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let rows: [Element]
    let iterationRecorder: RangeIterationRecorder

    func makeCursor() -> Cursor {
        Cursor(rows: rows, iterationRecorder: iterationRecorder)
    }

    struct Cursor: TransactionRangeCursor {
        let rows: [Element]
        let iterationRecorder: RangeIterationRecorder
        var index = 0

        mutating func next() async throws -> Element? {
            iterationRecorder.recordNext()
            guard index < rows.count else {
                return nil
            }
            defer { index += 1 }
            return rows[index]
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            iterationRecorder.recordFinish()
            if let finishError = iterationRecorder.finishError {
                throw finishError
            }
        }
    }
}

private final class RangeIterationGate: Sendable {
    private struct State: Sendable {
        var advanceStarted = false
        var startWaiters: [CheckedContinuation<Void, Never>] = []
        var advanceContinuation:
            CheckedContinuation<(ByteString, ByteString)?, Never>?
    }

    private let state = Mutex(State())

    func waitUntilAdvanceStarts() async {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.advanceStarted {
                    continuation.resume()
                } else {
                    state.startWaiters.append(continuation)
                }
            }
        }
    }

    func suspendAdvance() async -> (ByteString, ByteString)? {
        let waiters = state.withLock { state in
            state.advanceStarted = true
            let waiters = state.startWaiters
            state.startWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }

        return await withCheckedContinuation { continuation in
            state.withLock { state in
                precondition(state.advanceContinuation == nil)
                state.advanceContinuation = continuation
            }
        }
    }

    func resumeAdvance(with row: (ByteString, ByteString)?) {
        let continuation = state.withLock { state in
            let continuation = state.advanceContinuation
            state.advanceContinuation = nil
            return continuation
        }
        precondition(continuation != nil)
        continuation?.resume(returning: row)
    }
}

private struct SuspendedRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let iterationGate: RangeIterationGate
    let iterationRecorder: RangeIterationRecorder

    func makeCursor() -> Cursor {
        Cursor(
            iterationGate: iterationGate,
            iterationRecorder: iterationRecorder
        )
    }

    struct Cursor: TransactionRangeCursor {
        let iterationGate: RangeIterationGate
        let iterationRecorder: RangeIterationRecorder
        var hasAdvanced = false

        mutating func next() async throws -> Element? {
            guard !hasAdvanced else {
                return nil
            }
            hasAdvanced = true
            iterationRecorder.recordNext()
            return await iterationGate.suspendAdvance()
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            iterationRecorder.recordFinish()
            if let finishError = iterationRecorder.finishError {
                throw finishError
            }
        }
    }
}

private struct FailingRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let iterationRecorder: RangeIterationRecorder

    func makeCursor() -> Cursor {
        Cursor(iterationRecorder: iterationRecorder)
    }

    struct Cursor: TransactionRangeCursor {
        let iterationRecorder: RangeIterationRecorder

        mutating func next() async throws -> Element? {
            iterationRecorder.recordNext()
            throw RangeCleanupFailure.body
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            iterationRecorder.recordFinish()
            if let finishError = iterationRecorder.finishError {
                throw finishError
            }
        }
    }
}
