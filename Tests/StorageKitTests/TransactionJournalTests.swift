import DatabaseTypes
@testable import StorageKit
import Synchronization
import Testing

@Suite("Checkpoint journal lifecycle and resource contracts")
struct TransactionJournalTests {
    private func journal(bytes: Int = 4096, work: Int = 10000) throws -> TransactionJournal {
        TransactionJournal(
            limits: try TransactionCheckpointLimits(maximumEntries: 128, maximumDepth: 8,
                maximumRetainedBytes: bytes, maximumWorkUnits: work, maximumReadPageRows: 2),
            backend: .inMemory, mutationByteMeter: TransactionMutationByteMeter()
        )
    }

    @Test(.timeLimit(.minutes(1)))
    func terminalCompletionPreservesOwningFailureForWaiters() async throws {
        let completion = TransactionOperationCompletion()
        let failure = TransactionCheckpointError.limitExceeded(resource: .workUnits, actual: 2, maximum: 1)
        let first = Task { try await completion.wait() }
        let second = Task { try await completion.wait() }
        await Task.yield()
        completion.fail(failure)
        for waiter in [first, second] {
            do {
                try await waiter.value
                Issue.record("Terminal waiter unexpectedly succeeded")
            } catch let error as TransactionCheckpointError {
                #expect(error == failure)
            }
        }
        do {
            try await completion.wait()
            Issue.record("Repeated terminal observation unexpectedly succeeded")
        } catch let error as TransactionCheckpointError {
            #expect(error == failure)
        }
        let cleanupCompletion = TransactionOperationCompletion()
        cleanupCompletion.fail(StorageTransactionCleanupError(
            operationError: failure, cancellationError: JournalCursorFailure.cleanup
        ))
        do {
            try await cleanupCompletion.wait()
            Issue.record("Composed terminal observation unexpectedly succeeded")
        } catch let error as StorageTransactionCleanupError {
            #expect(error.operationError as? TransactionCheckpointError == failure)
            #expect(error.cancellationErrors.first as? JournalCursorFailure == .cleanup)
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func barrierDrainsCompoundOperationAndAllowsItsAdmittedContinuation() async throws {
        let journal = try journal()
        let gate = CheckpointReadGate()
        let backend = CheckpointTestBackend(gate: gate)
        let operation = Task {
            try await journal.performOperation {
                _ = try await journal.getValue(for: [1], snapshot: false, using: backend)
                try journal.append(.set(key: [2], value: [3], suppressWriteConflict: false))
            }
        }
        try await gate.started.wait()
        let marker = Task { try await journal.createCheckpoint() }
        await waitForBarrier(journal)
        #expect(throws: TransactionCheckpointError.busy) {
            try journal.append(.clear(key: [9], suppressWriteConflict: false))
        }
        gate.release.succeed()
        try await operation.value
        _ = try await marker.value
        #expect(try await journal.getValue(for: [2], snapshot: false, using: CheckpointTestBackend()) == [3])
        try await journal.close(committed: false)

        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        try transaction.configureCheckpoints(limits: TransactionCheckpointLimits(
            maximumEntries: 32, maximumDepth: 4, maximumRetainedBytes: 4096,
            maximumWorkUnits: 10000, maximumReadPageRows: 2
        ))
        let owner = try #require(transaction.checkpoints as? TransactionJournal)
        let continuationGate = CheckpointReadGate()
        let continuation = Task {
            try await transaction.performOperation {
                continuationGate.started.succeed()
                try await continuationGate.release.wait()
                try transaction.setValue([7], for: [7])
            }
        }
        try await continuationGate.started.wait()
        let commit = Task { try await transaction.commit() }
        await waitForBarrier(owner)
        continuationGate.release.succeed()
        try await continuation.value
        try await commit.value
        #expect(try await engine.withTransaction { try await $0.getValue(for: [7]) } == [7])
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func inheritedOperationCannotWriteAfterItsScopeReturns() async throws {
        let journal = try journal()
        let gate = TransactionOperationCompletion()
        let tasks = JournalEscapedTaskSlot()
        try await journal.performOperation {
            await tasks.set(Task {
                try await gate.wait()
                try journal.append(.set(key: [1], value: [2], suppressWriteConflict: false))
            })
        }
        gate.succeed()
        let escaped = await tasks.get()
        await #expect(throws: TransactionCheckpointError.closed) { try await escaped.value }
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func rollbackDetachesUnknownOwnerOnceAndReleasesRetainedCharge() async throws {
        let journal = try journal(bytes: 64)
        let marker = try await journal.createCheckpoint()
        let counter = JournalOwnerCounter()
        func appendOwner() throws {
            let owner = UnknownJournalBytes(bytes: [1, 2, 3], counter: counter)
            try journal.append(.set(key: [1], value: ByteString(retaining: owner), suppressWriteConflict: false))
        }
        try appendOwner()
        #expect(counter.borrowCount == 1)
        #expect(counter.releaseCount == 1)
        #expect(try await journal.getValue(for: [1], snapshot: true, using: CheckpointTestBackend()) == [1, 2, 3])
        #expect(journal.activity.availableBytes < 64)
        try await journal.rollback(to: marker)
        #expect(journal.activity.availableBytes == 64)
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func externalOwnerBorrowCanReenterJournalWithoutHoldingItsStateLock() async throws {
        let journal = try journal()
        let owner = ReentrantJournalBytes(journal: journal)
        try journal.append(.set(key: [1], value: ByteString(retaining: owner), suppressWriteConflict: false))
        #expect(owner.borrowCount == 1)
        let replay = try await journal.sealForCommit()
        #expect(replay.count == 1)
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func copyingAdmissionRejectsBeforeInvokingUserMaterializer() async throws {
        let journal = try journal(bytes: 4)
        let calls = Mutex(0)
        #expect(throws: TransactionCheckpointError.self) {
            try journal.append(copyingByteCount: 5) {
                calls.withLock { $0 += 1 }
                return .set(key: [1], value: [2], suppressWriteConflict: false)
            }
        }
        #expect(calls.withLock { $0 } == 0)
        #expect(journal.activity.availableBytes == 4)
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func rollbackNeverRefundsAttemptWorkAndCleanupDoesNotNeedWorkBudget() async throws {
        let journal = try journal(work: 4)
        let marker = try await journal.createCheckpoint()
        try journal.append(.set(key: [1], value: [1], suppressWriteConflict: false))
        try await journal.rollback(to: marker)
        try journal.append(.set(key: [2], value: [2], suppressWriteConflict: false))
        await #expect(throws: TransactionCheckpointError.self) { try await journal.rollback(to: marker) }
        try await journal.close(committed: false)
        #expect(journal.activity.availableBytes == 4096)
    }

    @Test(.timeLimit(.minutes(1)))
    func unresolvedVersionstampFailsClosedUntilDiscarded() async throws {
        let journal = try journal()
        let marker = try await journal.createCheckpoint()
        try journal.append(.atomic(key: [1], parameter: [2], type: .setVersionstampedValue, suppressWriteConflict: false))
        do {
            _ = try await journal.getValue(for: [1], snapshot: false, using: CheckpointTestBackend())
            Issue.record("Unresolved versionstamp unexpectedly produced a value")
        } catch let error as StorageError { #expect(error.code == .unsupportedOperation) }
        try await journal.rollback(to: marker)
        #expect(try await journal.getValue(for: [1], snapshot: false, using: CheckpointTestBackend()) == nil)
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func barrierFailurePreservesCleanupFailureAndClosesView() async throws {
        let journal = try journal()
        let state = JournalCursorCounts()
        let backend = CheckpointTestBackend(rows: [([1], [2])], counts: state, failCursor: 3)
        var cursor = journal.rangeCursor(from: .firstGreaterOrEqual([0]), to: .firstGreaterOrEqual([9]), limit: 0,
            reverse: false, snapshot: false, streamingMode: .iterator, using: backend)
        #expect(try await cursor.next()?.0 == [1])
        await #expect(throws: JournalCursorFailure.cleanup) { _ = try await journal.createCheckpoint() }
        #expect(state.finishCount == 3)
        #expect(throws: TransactionCheckpointError.closed) {
            try journal.append(.clear(key: [1], suppressWriteConflict: false))
        }
        try await journal.close(committed: false)
    }

    @Test(.timeLimit(.minutes(1)))
    func checkpointControlInsideAdmittedOperationFailsWithoutWaitingForItself() async throws {
        let journal = try journal()
        await #expect(throws: TransactionCheckpointError.barrierInsideOperation) {
            try await journal.performOperation { _ = try await journal.createCheckpoint() }
        }
        _ = try await journal.createCheckpoint()
        try await journal.close(committed: false)
    }

    private func waitForBarrier(_ journal: TransactionJournal) async {
        for _ in 0..<10000 {
            if journal.activity.activeBarrier != nil { return }
            await Task.yield()
        }
        Issue.record("Checkpoint did not close admission")
    }
}

private final class CheckpointReadGate: Sendable {
    let started = TransactionOperationCompletion()
    let release = TransactionOperationCompletion()
}

private actor JournalEscapedTaskSlot {
    private var task: Task<Void, any Error>?
    func set(_ task: Task<Void, any Error>) { self.task = task }
    func get() -> Task<Void, any Error> { task! }
}

private final class JournalOwnerCounter: Sendable {
    private let state = Mutex((borrows: 0, releases: 0))
    var borrowCount: Int { state.withLock { $0.borrows } }
    var releaseCount: Int { state.withLock { $0.releases } }
    func borrowed() { state.withLock { $0.borrows += 1 } }
    func released() { state.withLock { $0.releases += 1 } }
}

private final class UnknownJournalBytes: ByteStringOwner, Sendable {
    let bytes: [UInt8]
    let counter: JournalOwnerCounter
    init(bytes: [UInt8], counter: JournalOwnerCounter) { self.bytes = bytes; self.counter = counter }
    var count: Int { bytes.count }
    var retainedByteCount: Int? { nil }
    func borrowBytes(_ body: (UnsafeRawBufferPointer) throws -> Void) rethrows {
        counter.borrowed()
        try bytes.withUnsafeBytes(body)
    }
    deinit { counter.released() }
}

private final class ReentrantJournalBytes: ByteStringOwner, Sendable {
    private let journal: TransactionJournal
    private let calls = Mutex(0)
    init(journal: TransactionJournal) { self.journal = journal }
    var count: Int { 1 }
    var retainedByteCount: Int? { nil }
    var borrowCount: Int { calls.withLock { $0 } }
    func borrowBytes(_ body: (UnsafeRawBufferPointer) throws -> Void) rethrows {
        calls.withLock { $0 += 1 }
        // This takes journal state. Borrowing under append's mutex would deadlock.
        _ = journal.directoryProvenance(address: .root, prefix: [1])
        try [UInt8(7)].withUnsafeBytes(body)
    }
}

private enum JournalCursorFailure: Error, Equatable { case cleanup }

private final class JournalCursorCounts: Sendable {
    private let state = Mutex((created: 0, finished: 0))
    func create() -> Int { state.withLock { $0.created += 1; return $0.created } }
    func finish() { state.withLock { $0.finished += 1 } }
    var finishCount: Int { state.withLock { $0.finished } }
}

private struct CheckpointTestBackend: TransactionJournalBackendAccess {
    var rows: [(ByteString, ByteString)] = []
    var gate: CheckpointReadGate?
    var counts: JournalCursorCounts = JournalCursorCounts()
    var failCursor: Int = -1

    func getValue(for key: ByteString, snapshot: Bool, maximumByteCount: Int) async throws -> ByteString? {
        if let gate { gate.started.succeed(); try await gate.release.wait() }
        return rows.first { $0.0 == key }?.1
    }

    func rangeCursor(in range: TransactionJournalRange, reverse: Bool, snapshot: Bool, pageSize: Int, maximumByteCount: Int) -> KeyValueCursor {
        let selected = rows.filter { row in
            let lower: Bool
            switch range.lower {
            case .unbounded: lower = true
            case .inclusive(let key): lower = !row.0.lexicographicallyPrecedes(key)
            case .exclusive(let key): lower = key.lexicographicallyPrecedes(row.0)
            }
            let upper: Bool
            switch range.upper {
            case .unbounded: upper = true
            case .inclusive(let key): upper = !key.lexicographicallyPrecedes(row.0)
            case .exclusive(let key): upper = row.0.lexicographicallyPrecedes(key)
            }
            return lower && upper
        }
        let identifier = counts.create()
        return KeyValueCursor(consuming: JournalTestRange(rows: reverse ? Array(selected.reversed()) : selected,
            counts: counts, failFinish: identifier == failCursor))
    }
}

private struct JournalTestRange: TransactionRangeResult {
    let rows: [(ByteString, ByteString)]
    let counts: JournalCursorCounts
    let failFinish: Bool
    func makeCursor() -> Cursor { Cursor(rows: rows, counts: counts, failFinish: failFinish) }
    struct Cursor: TransactionRangeCursor {
        let rows: [(ByteString, ByteString)]
        let counts: JournalCursorCounts
        let failFinish: Bool
        var index = 0
        var closed = false
        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !closed, index < rows.count else { return nil }
            defer { index += 1 }
            return rows[index]
        }
        mutating func finish(isolation actor: isolated (any Actor)?) async throws {
            guard !closed else { return }
            closed = true
            counts.finish()
            if failFinish { throw JournalCursorFailure.cleanup }
        }
    }
}
