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
        var observedKeys: [Bytes] = []

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
    typealias Element = (Bytes, Bytes)

    let rows: [Element]
    let iterationRecorder: RangeIterationRecorder

    func makeAsyncIterator() -> Iterator {
        Iterator(rows: rows, iterationRecorder: iterationRecorder)
    }

    struct Iterator: TransactionRangeIterator {
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
