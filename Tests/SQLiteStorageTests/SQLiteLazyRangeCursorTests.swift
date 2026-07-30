import DatabaseTypes
import StorageKit
import Testing
@testable import SQLiteStorage

@Suite("SQLite lazy range cursor")
struct SQLiteLazyRangeCursorTests {
    @Test("Range preparation, stepping, and copying start at first next")
    func firstNextIsTheOnlyExecutionBoundary() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        let range = transaction.range(
            begin: [0x00],
            end: [0xFF],
            limit: 0,
            reverse: false
        )
        #expect(engine.rangeInstrumentation == baseline)

        let first = try await consumeExactlyOne(range)
        #expect(first?.0 == [0x01])
        #expect(first?.1 == [0xA1])

        let measured = engine.rangeInstrumentation
        #expect(measured.prepareCount == baseline.prepareCount + 1)
        #expect(measured.stepCount == baseline.stepCount + 1)
        #expect(measured.payloadCopyCount == baseline.payloadCopyCount + 2)
        #expect(measured.finalizeCount == baseline.finalizeCount + 1)
        #expect(measured.openCursorCount == 0)
        try await transaction.cancel()
    }

    @Test("Owned row bytes survive later SQLite steps")
    func rowBytesSurviveSubsequentSteps() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0x10, 0x11], for: [0x01])
            try transaction.setValue([0x20, 0x21], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        var cursor = transaction.range(
            begin: [0x00],
            end: [0xFF]
        ).makeCursor()

        let firstValue = try await cursor.next()
        let first = try #require(firstValue)
        let secondValue = try await cursor.next()
        let second = try #require(secondValue)
        #expect(first.0 == [0x01])
        #expect(first.1 == [0x10, 0x11])
        #expect(second.0 == [0x02])
        #expect(second.1 == [0x20, 0x21])
        #expect(try await cursor.next() == nil)

        // The first row is checked again after two later sqlite3_step calls.
        #expect(first.0 == [0x01])
        #expect(first.1 == [0x10, 0x11])

        let measured = engine.rangeInstrumentation
        #expect(measured.prepareCount == baseline.prepareCount + 1)
        #expect(measured.stepCount == baseline.stepCount + 3)
        #expect(measured.payloadCopyCount == baseline.payloadCopyCount + 4)
        #expect(measured.finalizeCount == baseline.finalizeCount + 1)
        #expect(measured.openCursorCount == 0)
        try await transaction.cancel()
    }

    @Test("Copied cursor values share one cursor and one advancement state")
    func copiedIteratorsShareOneCursor() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        var firstIterator = transaction.range(
            begin: [0x00],
            end: [0xFF]
        ).makeCursor()
        var copiedIterator = firstIterator

        let first = try await firstIterator.next()
        let second = try await copiedIterator.next()
        #expect(first?.0 == [0x01])
        #expect(second?.0 == [0x02])
        #expect(try await firstIterator.next() == nil)

        let measured = engine.rangeInstrumentation
        #expect(measured.prepareCount == baseline.prepareCount + 1)
        #expect(measured.stepCount == baseline.stepCount + 3)
        #expect(measured.payloadCopyCount == baseline.payloadCopyCount + 4)
        #expect(measured.finalizeCount == baseline.finalizeCount + 1)
        #expect(measured.openCursorCount == 0)
        try await transaction.cancel()
    }

    @Test(.timeLimit(.minutes(1)))
    func concurrentNextIsRejectedWithoutOpeningAnotherCursor() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let leaseOwner = try engine.createTransaction()
        _ = try await leaseOwner.getValue(for: [0x00])

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        let cursor = transaction.range(
            begin: [0x00],
            end: [0xFF]
        ).makeCursor()
        var copiedIterator = cursor
        let blockedAdvance = Task {
            try await advanceOne(cursor)
        }
        await waitForWaitingLeaseCount(1, engine: engine)

        do {
            _ = try await copiedIterator.next()
            Issue.record("Expected concurrent SQLite cursor advancement to fail")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .rangeRead)
        }

        try await leaseOwner.commit()
        let first = try await blockedAdvance.value
        #expect(first?.0 == [0x01])
        #expect(try await copiedIterator.next()?.0 == [0x02])
        #expect(try await copiedIterator.next() == nil)

        let measured = engine.rangeInstrumentation
        #expect(measured.prepareCount == baseline.prepareCount + 1)
        #expect(measured.stepCount == baseline.stepCount + 3)
        #expect(measured.payloadCopyCount == baseline.payloadCopyCount + 4)
        #expect(measured.finalizeCount == baseline.finalizeCount + 1)
        #expect(measured.openCursorCount == 0)
        try await transaction.cancel()
    }

    @Test("Commit finalizes a partially consumed cursor exactly once")
    func commitInvalidatesOpenCursorExactlyOnce() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        var cursor = transaction.range(
            begin: [0x00],
            end: [0xFF]
        ).makeCursor()
        let first = try await cursor.next()
        _ = try #require(first)

        try await transaction.commit()
        let afterCommit = engine.rangeInstrumentation
        #expect(afterCommit.finalizeCount == baseline.finalizeCount + 1)
        #expect(afterCommit.openCursorCount == 0)

        do {
            _ = try await cursor.next()
            Issue.record("Expected a terminal transaction error")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
        #expect(
            engine.rangeInstrumentation.finalizeCount
                == afterCommit.finalizeCount
        )
    }

    @Test("Reaching the requested limit finalizes before returning control")
    func limitFinalizesImmediately() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        var cursor = transaction.range(
            begin: [0x00],
            end: [0xFF],
            limit: 1
        ).makeCursor()
        let first = try await cursor.next()
        #expect(first?.0 == [0x01])

        let measured = engine.rangeInstrumentation
        #expect(measured.prepareCount == baseline.prepareCount + 1)
        #expect(measured.stepCount == baseline.stepCount + 1)
        #expect(measured.payloadCopyCount == baseline.payloadCopyCount + 2)
        #expect(measured.finalizeCount == baseline.finalizeCount + 1)
        #expect(measured.openCursorCount == 0)
        #expect(try await cursor.next() == nil)
        try await transaction.cancel()
    }

    @Test("Cancellation finalizes a partially consumed cursor exactly once")
    func cancellationInvalidatesOpenCursorExactlyOnce() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        var cursor = transaction.range(
            begin: [0x00],
            end: [0xFF]
        ).makeCursor()
        let first = try await cursor.next()
        _ = try #require(first)

        try await transaction.cancel()
        let afterCancellation = engine.rangeInstrumentation
        #expect(
            afterCancellation.finalizeCount
                == baseline.finalizeCount + 1
        )
        #expect(afterCancellation.openCursorCount == 0)

        do {
            _ = try await cursor.next()
            Issue.record("Expected a terminal transaction error")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
        #expect(
            engine.rangeInstrumentation.finalizeCount
                == afterCancellation.finalizeCount
        )
    }

    @Test("Nested rollback finalizes the child cursor and preserves parent writes")
    func nestedRollbackFinalizesCursor() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let leaseBaseline = await engine.leaseInstrumentation

        try await engine.withTransaction { parent in
            try parent.setValue([0xA1], for: [0x01])
            let child = try engine.createTransaction()
            try child.setValue([0xA2], for: [0x02])

            let baseline = engine.rangeInstrumentation
            var cursor = child.range(
                begin: [0x00],
                end: [0xFF]
            ).makeCursor()
            let first = try await cursor.next()
            _ = try #require(first)
            try await child.cancel()

            let afterCancel = engine.rangeInstrumentation
            #expect(afterCancel.finalizeCount == baseline.finalizeCount + 1)
            #expect(afterCancel.openCursorCount == 0)
            #expect(try await parent.getValue(for: [0x01]) == [0xA1])
            #expect(try await parent.getValue(for: [0x02]) == nil)
        }

        let leaseMeasured = await engine.leaseInstrumentation
        #expect(
            leaseMeasured.savepointBeginCount
                == leaseBaseline.savepointBeginCount + 1
        )
        #expect(
            leaseMeasured.savepointRollbackCount
                == leaseBaseline.savepointRollbackCount + 1
        )
        #expect(
            leaseMeasured.savepointReleaseCount
                == leaseBaseline.savepointReleaseCount + 1
        )
    }

    private func consumeExactlyOne(
        _ range: SQLiteRangeResult
    ) async throws -> (ByteString, ByteString)? {
        var cursor = range.makeCursor()
        return try await cursor.next()
    }

    private func advanceOne(
        _ cursor: SQLiteRangeResult.Cursor
    ) async throws -> (ByteString, ByteString)? {
        var cursor = cursor
        return try await cursor.next()
    }

    private func waitForWaitingLeaseCount(
        _ expectedCount: Int,
        engine: SQLiteStorageEngine
    ) async {
        for _ in 0..<10_000 {
            if await engine.leaseInstrumentation.waitingRootCount
                == expectedCount {
                return
            }
            await Task.yield()
        }
        Issue.record(
            "Timed out waiting for \(expectedCount) queued SQLite leases"
        )
    }
}

private extension SQLiteStorageTransaction {
    func range(
        begin: ByteString,
        end: ByteString,
        limit: Int = 0,
        reverse: Bool = false
    ) -> SQLiteRangeResult {
        getRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit,
            reverse: reverse,
            snapshot: false,
            streamingMode: .iterator
        )
    }
}
