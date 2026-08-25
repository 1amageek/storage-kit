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

    @Test func cursorConsumptionDoesNotNestAnExistingCleanupFailure() async {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        var cursor = KeyValueCursor(
            consuming: FailingRows(iterationRecorder: iterationRecorder)
        )

        do {
            try await cursor.consume { _, _ in }
            Issue.record("Expected combined failure")
        } catch let error as StorageRangeCleanupError {
            #expect(error.iterationError as? RangeCleanupFailure == .body)
            #expect(error.cleanupError as? RangeCleanupFailure == .finish)
        } catch {
            Issue.record("Expected StorageRangeCleanupError, got \(error)")
        }

        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func cursorReportsNaturalExhaustionCleanupFailureSeparately() async {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        var cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [],
                iterationRecorder: iterationRecorder
            )
        )

        do {
            _ = try await cursor.next()
            Issue.record("Expected terminal cleanup failure")
        } catch let error as StorageRangeTerminalCleanupError {
            #expect(error.cleanupError as? RangeCleanupFailure == .finish)
        } catch {
            Issue.record(
                "Expected StorageRangeTerminalCleanupError, got \(error)"
            )
        }

        #expect(iterationRecorder.nextCount == 1)
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func cursorConsumptionPreservesTerminalCleanupFailure() async {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        var cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [],
                iterationRecorder: iterationRecorder
            )
        )

        do {
            try await cursor.consume { _, _ in }
            Issue.record("Expected terminal cleanup failure")
        } catch let error as StorageRangeTerminalCleanupError {
            #expect(error.cleanupError as? RangeCleanupFailure == .finish)
        } catch {
            Issue.record(
                "Expected StorageRangeTerminalCleanupError, got \(error)"
            )
        }

        #expect(iterationRecorder.nextCount == 1)
        #expect(iterationRecorder.finishCount == 1)
    }

    @Test func cursorRetainsOwnerUntilNaturalExhaustion() async throws {
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        var cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [([0x01], [0x11])],
                iterationRecorder: iterationRecorder
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )

        #expect(lifetimeRecorder.releaseCount == 0)
        _ = try await cursor.next()
        #expect(lifetimeRecorder.releaseCount == 0)
        _ = try await cursor.next()

        #expect(iterationRecorder.finishCount == 1)
        #expect(lifetimeRecorder.releaseCount == 1)
    }

    @Test func cursorAliasesShareRetainedOwnerUntilTerminalCleanup() async throws {
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        let original = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [([0x01], [0x11])],
                iterationRecorder: iterationRecorder
            )
        )
        var alias = original
        var retained = original.retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )

        _ = try await retained.next()
        retained = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [],
                iterationRecorder: RangeIterationRecorder()
            )
        )
        #expect(lifetimeRecorder.releaseCount == 0)

        try await alias.finish()

        #expect(iterationRecorder.finishCount == 1)
        #expect(lifetimeRecorder.releaseCount == 1)
    }

    @Test func cancelledAdvanceFinishesAndReleasesOwnerExactlyOnce() async throws {
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        let cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [([0x01], [0x11]), ([0x02], [0x12])],
                iterationRecorder: iterationRecorder
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )
        var primingCursor = cursor
        _ = try await primingCursor.next()
        let cancelledAdvance = Task {
            var cancelledCursor = cursor
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            await #expect(throws: CancellationError.self) {
                _ = try await cancelledCursor.next()
            }
        }
        try await cancelledAdvance.value

        withExtendedLifetime(cursor) {
            #expect(iterationRecorder.finishCount == 1)
            #expect(lifetimeRecorder.releaseCount == 1)
        }
    }

    @Test func cancellationDuringAdvanceIsTerminalAfterBackendReturns() async {
        let iterationGate = RangeIterationGate()
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        let cursor = KeyValueCursor(
            consuming: SuspendedRows(
                iterationGate: iterationGate,
                iterationRecorder: iterationRecorder
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )
        var advancingCursor = cursor
        let advance = Task {
            try await advancingCursor.next()
        }
        await iterationGate.waitUntilAdvanceStarts()

        advance.cancel()
        iterationGate.resumeAdvance(with: ([0x01], [0x11]))

        do {
            _ = try await advance.value
            Issue.record("Expected cancellation to terminate the cursor")
        } catch is CancellationError {
        } catch {
            Issue.record("Expected CancellationError, got \(error)")
        }
        withExtendedLifetime(cursor) {
            #expect(iterationRecorder.finishCount == 1)
            #expect(lifetimeRecorder.releaseCount == 1)
        }
    }

    @Test func cancelledAdvancePreservesCleanupFailure() async throws {
        let iterationRecorder = RangeIterationRecorder(finishError: .finish)
        let lifetimeRecorder = CursorLifetimeRecorder()
        let cursor = KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [([0x01], [0x11]), ([0x02], [0x12])],
                iterationRecorder: iterationRecorder
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )
        var primingCursor = cursor
        _ = try await primingCursor.next()
        let cancelledAdvance = Task {
            var cancelledCursor = cursor
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            do {
                _ = try await cancelledCursor.next()
                Issue.record("Expected cancellation and cleanup failure")
            } catch let error as StorageRangeCleanupError {
                #expect(error.iterationError is CancellationError)
                #expect(error.cleanupError as? RangeCleanupFailure == .finish)
            } catch {
                Issue.record("Expected StorageRangeCleanupError, got \(error)")
            }
        }
        try await cancelledAdvance.value

        withExtendedLifetime(cursor) {
            #expect(iterationRecorder.finishCount == 1)
            #expect(lifetimeRecorder.releaseCount == 1)
        }
    }

    @Test func iterationFailureReleasesOwnerExactlyOnce() async {
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        var cursor = KeyValueCursor(
            consuming: FailingRows(iterationRecorder: iterationRecorder)
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: lifetimeRecorder)
        )

        await #expect(throws: RangeCleanupFailure.body) {
            _ = try await cursor.next()
        }

        withExtendedLifetime(cursor) {
            #expect(iterationRecorder.finishCount == 1)
            #expect(lifetimeRecorder.releaseCount == 1)
        }
    }

    @Test func returnedRowBuffersOutliveCursorCleanupWithoutCopying() async throws {
        let cursorLifetimeRecorder = CursorLifetimeRecorder()
        let keyLifetimeRecorder = CursorLifetimeRecorder()
        let valueLifetimeRecorder = CursorLifetimeRecorder()
        let fixture = makeOwnedRowCursor(
            key: [0x01, 0x02, 0x03],
            value: [0x11, 0x12, 0x13],
            cursorLifetimeRecorder: cursorLifetimeRecorder,
            keyLifetimeRecorder: keyLifetimeRecorder,
            valueLifetimeRecorder: valueLifetimeRecorder
        )
        var cursor = fixture.cursor
        var row = try await cursor.next()
        try await cursor.finish()

        withExtendedLifetime(cursor) {
            #expect(cursorLifetimeRecorder.releaseCount == 1)
            #expect(keyLifetimeRecorder.releaseCount == 0)
            #expect(valueLifetimeRecorder.releaseCount == 0)
            #expect(row?.0 == [0x01, 0x02, 0x03])
            #expect(row?.1 == [0x11, 0x12, 0x13])
            #expect(byteAddress(of: row?.0) == fixture.keyAddress)
            #expect(byteAddress(of: row?.1) == fixture.valueAddress)
        }

        row = nil
        #expect(keyLifetimeRecorder.releaseCount == 1)
        #expect(valueLifetimeRecorder.releaseCount == 1)
    }

    @Test func selectedKeyOutlivesCursorCleanupWithoutCopying() async throws {
        let cursorLifetimeRecorder = CursorLifetimeRecorder()
        let keyLifetimeRecorder = CursorLifetimeRecorder()
        let fixture = makeOwnedKeyCursor(
            key: [0x01, 0x02, 0x03],
            cursorLifetimeRecorder: cursorLifetimeRecorder,
            keyLifetimeRecorder: keyLifetimeRecorder
        )
        var selectedKey: ByteString? = try await TransactionKeySelection.resolve(
            .firstGreaterOrEqual([0x01]),
            in: ScriptedReadAccess(
                cursor: fixture.cursor
            ),
            snapshot: false
        )

        #expect(cursorLifetimeRecorder.releaseCount == 1)
        #expect(keyLifetimeRecorder.releaseCount == 0)
        #expect(selectedKey == [0x01, 0x02, 0x03])
        let selectedAddress = try #require(
            selectedKey?.withUnsafeBytes {
                $0.baseAddress.map(UInt.init(bitPattern:))
            }
        )
        #expect(selectedAddress == fixture.keyAddress)

        selectedKey = nil
        #expect(keyLifetimeRecorder.releaseCount == 1)
    }

    @Test func keySelectionFailureReleasesCursorOwnerExactlyOnce() async {
        let iterationRecorder = RangeIterationRecorder()
        let lifetimeRecorder = CursorLifetimeRecorder()
        let access = ScriptedReadAccess(
            cursor: KeyValueCursor(
                consuming: FailingRows(iterationRecorder: iterationRecorder)
            ).retainingLifetime(
                of: CursorLifetimeOwner(recorder: lifetimeRecorder)
            )
        )

        await #expect(throws: RangeCleanupFailure.body) {
            _ = try await TransactionKeySelection.resolve(
                .firstGreaterOrEqual([0x01]),
                in: access,
                snapshot: false
            )
        }

        withExtendedLifetime(access) {
            #expect(iterationRecorder.finishCount == 1)
            #expect(lifetimeRecorder.releaseCount == 1)
        }
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

private final class CursorLifetimeRecorder: Sendable {
    private let releases = Mutex(0)

    var releaseCount: Int {
        releases.withLock { $0 }
    }

    func recordRelease() {
        releases.withLock { $0 += 1 }
    }
}

private final class CursorLifetimeOwner: Sendable {
    private let recorder: CursorLifetimeRecorder

    init(recorder: CursorLifetimeRecorder) {
        self.recorder = recorder
    }

    deinit {
        recorder.recordRelease()
    }
}

private final class ReleaseTrackedBytesOwner: ByteStringOwner, Sendable {
    let count: Int

    private let bytes: [UInt8]
    private let recorder: CursorLifetimeRecorder

    init(bytes: [UInt8], recorder: CursorLifetimeRecorder) {
        self.bytes = bytes
        self.count = bytes.count
        self.recorder = recorder
    }

    deinit {
        recorder.recordRelease()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private struct ScriptedReadAccess: TransactionReadAccess {
    let transactionDomain = StorageTransactionDomain()
    let cursor: KeyValueCursor

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        nil
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        cursor
    }
}

private struct OwnedKeyCursorFixture: Sendable {
    let cursor: KeyValueCursor
    let keyAddress: UInt?
}

private struct OwnedRowCursorFixture: Sendable {
    let cursor: KeyValueCursor
    let keyAddress: UInt?
    let valueAddress: UInt?
}

private func makeOwnedKeyCursor(
    key: [UInt8],
    cursorLifetimeRecorder: CursorLifetimeRecorder,
    keyLifetimeRecorder: CursorLifetimeRecorder
) -> OwnedKeyCursorFixture {
    let key = ByteString(
        retaining: ReleaseTrackedBytesOwner(
            bytes: key,
            recorder: keyLifetimeRecorder
        )
    )
    let keyAddress = key.withUnsafeBytes {
        $0.baseAddress.map(UInt.init(bitPattern:))
    }
    return OwnedKeyCursorFixture(
        cursor: KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [(key, [0x11])],
                iterationRecorder: RangeIterationRecorder()
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: cursorLifetimeRecorder)
        ),
        keyAddress: keyAddress
    )
}

private func makeOwnedRowCursor(
    key: [UInt8],
    value: [UInt8],
    cursorLifetimeRecorder: CursorLifetimeRecorder,
    keyLifetimeRecorder: CursorLifetimeRecorder,
    valueLifetimeRecorder: CursorLifetimeRecorder
) -> OwnedRowCursorFixture {
    let key = ByteString(
        retaining: ReleaseTrackedBytesOwner(
            bytes: key,
            recorder: keyLifetimeRecorder
        )
    )
    let value = ByteString(
        retaining: ReleaseTrackedBytesOwner(
            bytes: value,
            recorder: valueLifetimeRecorder
        )
    )
    return OwnedRowCursorFixture(
        cursor: KeyValueCursor(
            consuming: FinishRecordingRows(
                rows: [(key, value)],
                iterationRecorder: RangeIterationRecorder()
            )
        ).retainingLifetime(
            of: CursorLifetimeOwner(recorder: cursorLifetimeRecorder)
        ),
        keyAddress: byteAddress(of: key),
        valueAddress: byteAddress(of: value)
    )
}

private func byteAddress(of bytes: ByteString?) -> UInt? {
    bytes?.withUnsafeBytes {
        $0.baseAddress.map(UInt.init(bitPattern:))
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
