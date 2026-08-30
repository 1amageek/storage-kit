import DatabaseTypes
@testable import StorageKit
import Synchronization
import Testing

/// Closing a Partition binding is authoritative over the cursors that binding
/// issued: it completes their backend cleanup instead of only refusing their
/// next advance.
@Suite("Partition binding scope")
struct PartitionBindingScopeTests {
    private func makePartition(_ engine: InMemoryEngine) async throws -> Partition {
        try await engine.withTransaction { transaction in
            let root = try await engine.directoryAccess.openOrInitializeRoot(
                transaction: transaction
            )
            return try await engine.directoryAccess.openOrCreatePartition(
                "p",
                in: root,
                transaction: transaction
            )
        }
    }

    /// A cursor the closure kept is finished by the close, so the backend
    /// iterator and the transaction retention behind it are already released
    /// when the binding returns. The escaped cursor then refuses its next
    /// advance without reopening anything.
    @Test(.timeLimit(.minutes(1)))
    func closingFinishesACursorTheClosureKept() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let recorder = BindingRangeRecorder()
        let scripted = KeyValueCursor(
            consuming: BindingRecordedRows(
                rows: [([0x01], [0x11]), ([0x02], [0x22])],
                recorder: recorder
            )
        )

        try await engine.withTransaction { transaction in
            let reads = ScriptedCursorReadAccess(
                base: transaction,
                scriptedPrefix: bounds.prefix,
                scripted: scripted
            )
            let lease = try await engine.leasePartition(partition, transaction: reads)
            var escaped = try await lease.withReadAccess(reads) { access in
                var cursor = try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
                _ = try await cursor.next()
                return cursor
            }

            #expect(recorder.finishCount == 1)
            let error = await expectStorageError(.staleLease) {
                _ = try await escaped.next()
            }
            #expect(error?.message.contains("scope") == true)
            #expect(recorder.finishCount == 1)
            lease.release()
        }
        await engine.shutdown()
    }

    /// An advance still running when the closure returns is awaited by the
    /// close. The advance is released only after the close has entered the
    /// finish-while-advancing path, so the binding cannot return before the
    /// in-flight backend work completes. The row that advance had already read
    /// is not delivered either: it was admitted inside a binding that has since
    /// closed, and its bytes are borrowed from the cursor the close finished.
    @Test(.timeLimit(.minutes(1)))
    func closingAwaitsAnAdvanceStillInFlight() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let recorder = BindingRangeRecorder()
        let advanceGate = BindingAdvanceGate()
        let finishRequested = BindingSignal()
        let scripted = KeyValueCursor(
            testing: BindingSuspendedRows(gate: advanceGate, recorder: recorder),
            onFinishWhileAdvancing: { finishRequested.signal() }
        )

        let releaser = Task {
            await finishRequested.wait()
            advanceGate.resumeAdvance(with: ([0x01], [0x11]))
        }

        try await engine.withTransaction { transaction in
            let reads = ScriptedCursorReadAccess(
                base: transaction,
                scriptedPrefix: bounds.prefix,
                scripted: scripted
            )
            let lease = try await engine.leasePartition(partition, transaction: reads)
            let advance = try await lease.withReadAccess(reads) { access in
                let cursor = try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
                let advance = Task {
                    var advancing = cursor
                    return try await advancing.next()
                }
                await advanceGate.waitUntilAdvanceStarts()
                return advance
            }

            // Cleanup ran before the binding returned, and it runs only in
            // the resumed advance, which the close itself released.
            #expect(recorder.finishCount == 1)
            #expect(finishRequested.isSignalled)
            let error = await expectStorageError(.staleLease) {
                _ = try await advance.value
            }
            #expect(error?.message.contains("scope") == true)
            #expect(recorder.finishCount == 1)
            lease.release()
        }
        await releaser.value
        await engine.shutdown()
    }

    /// A cleanup failure the close caused fails the binding even though the
    /// closure succeeded: a value produced over storage whose cleanup failed is
    /// not a result.
    @Test(.timeLimit(.minutes(1)))
    func closingReportsItsCleanupFailureOverASuccessfulClosure() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let recorder = BindingRangeRecorder(finishError: .finish)
        let scripted = KeyValueCursor(
            consuming: BindingRecordedRows(rows: [([0x01], [0x11])], recorder: recorder)
        )

        try await engine.withTransaction { transaction in
            let reads = ScriptedCursorReadAccess(
                base: transaction,
                scriptedPrefix: bounds.prefix,
                scripted: scripted
            )
            let lease = try await engine.leasePartition(partition, transaction: reads)
            do {
                _ = try await lease.withReadAccess(reads) { access in
                    var cursor = try access.rangeCursor(
                        from: .firstGreaterOrEqual(bounds.prefix),
                        to: .firstGreaterOrEqual(bounds.end)
                    )
                    _ = try await cursor.next()
                    return 7
                }
                Issue.record("The binding must not return a value after cleanup failed")
            } catch let error as PartitionBindingCleanupError {
                #expect(error.operationError == nil)
                #expect(error.cursorCleanupErrors.count == 1)
                #expect(error.cursorCleanupErrors.first as? BindingCleanupFailure == .finish)
            }
            #expect(recorder.finishCount == 1)
            lease.release()
        }
        await engine.shutdown()
    }

    /// When both fail, neither is lost.
    @Test(.timeLimit(.minutes(1)))
    func closingCombinesItsCleanupFailureWithTheClosureFailure() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let recorder = BindingRangeRecorder(finishError: .finish)
        let scripted = KeyValueCursor(
            consuming: BindingRecordedRows(rows: [([0x01], [0x11])], recorder: recorder)
        )

        try await engine.withTransaction { transaction in
            let reads = ScriptedCursorReadAccess(
                base: transaction,
                scriptedPrefix: bounds.prefix,
                scripted: scripted
            )
            let lease = try await engine.leasePartition(partition, transaction: reads)
            do {
                try await lease.withReadAccess(reads) { access in
                    var cursor = try access.rangeCursor(
                        from: .firstGreaterOrEqual(bounds.prefix),
                        to: .firstGreaterOrEqual(bounds.end)
                    )
                    _ = try await cursor.next()
                    throw BindingCleanupFailure.body
                }
                Issue.record("The closure failure must reach the caller")
            } catch let error as PartitionBindingCleanupError {
                #expect(error.operationError as? BindingCleanupFailure == .body)
                #expect(error.cursorCleanupErrors.count == 1)
                #expect(error.cursorCleanupErrors.first as? BindingCleanupFailure == .finish)
            }
            lease.release()
        }
        await engine.shutdown()
    }

    /// A cursor the closure itself drove to a terminal failure has already
    /// reported that failure to the closure. Restating it at close would turn a
    /// failure the closure deliberately handled into a binding failure, so the
    /// close awaits that cursor and returns the closure's value.
    @Test(.timeLimit(.minutes(1)))
    func closingDoesNotRestateAFailureTheClosureAlreadyHandled() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let recorder = BindingRangeRecorder(finishError: .finish)
        let scripted = KeyValueCursor(
            consuming: BindingRecordedRows(rows: [([0x01], [0x11])], recorder: recorder)
        )

        try await engine.withTransaction { transaction in
            let reads = ScriptedCursorReadAccess(
                base: transaction,
                scriptedPrefix: bounds.prefix,
                scripted: scripted
            )
            let lease = try await engine.leasePartition(partition, transaction: reads)
            let handled = try await lease.withReadAccess(reads) { access in
                var cursor = try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
                _ = try await cursor.next()
                do {
                    try await cursor.finish()
                    return false
                } catch is BindingCleanupFailure {
                    return true
                }
            }

            #expect(handled)
            #expect(recorder.finishCount == 1)
            lease.release()
        }
        await engine.shutdown()
    }
}

private enum BindingCleanupFailure: Error, Equatable, Sendable {
    case body
    case finish
}

private final class BindingRangeRecorder: Sendable {
    private struct State: Sendable {
        var nextCount = 0
        var finishCount = 0
    }

    private let state = Mutex(State())
    let finishError: BindingCleanupFailure?

    init(finishError: BindingCleanupFailure? = nil) {
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

private final class BindingSignal: Sendable {
    private struct State {
        var isSignalled = false
        var waiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    var isSignalled: Bool {
        state.withLock { $0.isSignalled }
    }

    func signal() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.isSignalled = true
            defer { state.waiters = [] }
            return state.waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    func wait() async {
        await withCheckedContinuation { continuation in
            state.withLock { state in
                if state.isSignalled {
                    continuation.resume()
                } else {
                    state.waiters.append(continuation)
                }
            }
        }
    }
}

private final class BindingAdvanceGate: Sendable {
    private struct State {
        var advanceStarted = false
        var startWaiters: [CheckedContinuation<Void, Never>] = []
        var advanceContinuation: CheckedContinuation<(ByteString, ByteString)?, Never>?
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
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.advanceStarted = true
            defer { state.startWaiters = [] }
            return state.startWaiters
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
        let continuation = state.withLock { state -> CheckedContinuation<(ByteString, ByteString)?, Never>? in
            let continuation = state.advanceContinuation
            state.advanceContinuation = nil
            return continuation
        }
        precondition(continuation != nil)
        continuation?.resume(returning: row)
    }
}

private struct BindingRecordedRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let rows: [Element]
    let recorder: BindingRangeRecorder

    func makeCursor() -> Cursor {
        Cursor(rows: rows, recorder: recorder)
    }

    struct Cursor: TransactionRangeCursor {
        let rows: [Element]
        let recorder: BindingRangeRecorder
        var index = 0

        mutating func next() async throws -> Element? {
            recorder.recordNext()
            guard index < rows.count else {
                return nil
            }
            defer { index += 1 }
            return rows[index]
        }

        mutating func finish(isolation actor: isolated (any Actor)?) async throws {
            recorder.recordFinish()
            if let finishError = recorder.finishError {
                throw finishError
            }
        }
    }
}

private struct BindingSuspendedRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let gate: BindingAdvanceGate
    let recorder: BindingRangeRecorder

    func makeCursor() -> Cursor {
        Cursor(gate: gate, recorder: recorder)
    }

    struct Cursor: TransactionRangeCursor {
        let gate: BindingAdvanceGate
        let recorder: BindingRangeRecorder
        var hasAdvanced = false

        mutating func next() async throws -> Element? {
            guard !hasAdvanced else {
                return nil
            }
            hasAdvanced = true
            recorder.recordNext()
            return await gate.suspendAdvance()
        }

        mutating func finish(isolation actor: isolated (any Actor)?) async throws {
            recorder.recordFinish()
            if let finishError = recorder.finishError {
                throw finishError
            }
        }
    }
}

/// Forwards every read to the engine's transaction and substitutes one scripted
/// cursor for range reads that begin inside the Partition's own keyspace.
///
/// The catalog walk that validates a lease never range-reads inside a
/// Partition's keyspace, so this substitution reaches only the range read the
/// bound access performs.
private struct ScriptedCursorReadAccess: TransactionReadAccess {
    let base: any TransactionReadAccess
    let scriptedPrefix: ByteString
    let scripted: KeyValueCursor

    var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
        try await base.getValue(for: key, snapshot: snapshot)
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await base.getValue(for: key)
    }

    func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
        try await base.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        guard begin.key.starts(with: scriptedPrefix) else {
            return base.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }
        return scripted
    }
}
