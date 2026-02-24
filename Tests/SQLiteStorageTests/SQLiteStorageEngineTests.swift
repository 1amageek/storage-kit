import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLiteStorageEngine Tests")
struct SQLiteStorageEngineTests {

    // =========================================================================
    // MARK: - Write Buffer Reverse Scan
    //
    // getValue iterates writeBuffer.reversed(). The FIRST matching operation
    // found in reverse order determines the result. Unmatched operations are
    // skipped; if no match, fall through to SQLite.
    // =========================================================================

    @Test func setThenClear_clearWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func setThenClearThenSet_lastSetWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        tx.setValue([2], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])
        try await tx.commit()
    }

    @Test func setThenClearRange_clearRangeWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func clearRangeThenSet_setWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x05])
        tx.setValue([99], for: [0x03])
        let value = try await tx.getValue(for: [0x03])
        #expect(value == [99])
        try await tx.commit()
    }

    @Test func multipleOverwrites_lastWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.setValue([2], for: [0x01])
        tx.setValue([3], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [3])
        try await tx.commit()
    }

    @Test func setClearRangeSetClearRange_lastClearRangeWins() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        tx.setValue([2], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func bufferOverridesSQLiteValue() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        tx.setValue([20], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [20])
        try await tx.commit()
    }

    @Test func clearInBufferHidesSQLiteValue() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func noMatchInBufferFallsThroughToSQLite() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        tx.setValue([99], for: [0xFF]) // different key
        // [0x01] not in buffer → falls through to SQLite
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - clearRange Boundary Semantics
    //
    // compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0
    // begin INCLUSIVE, end EXCLUSIVE.
    // =========================================================================

    @Test func clearRange_beginInclusive() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x02], end: [0x05])
        let beginValue = try await tx.getValue(for: [0x02])
        #expect(beginValue == nil)
        try await tx.commit()
    }

    @Test func clearRange_endExclusive() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x05])
        tx.clearRange(begin: [0x02], end: [0x05])
        let endValue = try await tx.getValue(for: [0x05])
        #expect(endValue == [1])
        try await tx.commit()
    }

    @Test func clearRange_justBeforeEnd() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x04])
        tx.clearRange(begin: [0x02], end: [0x05])
        let justBeforeEndValue = try await tx.getValue(for: [0x04])
        #expect(justBeforeEndValue == nil)
        try await tx.commit()
    }

    @Test func clearRange_justBeforeBegin() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clearRange(begin: [0x02], end: [0x05])
        let justBeforeBeginValue = try await tx.getValue(for: [0x01])
        #expect(justBeforeBeginValue == [1])
        try await tx.commit()
    }

    @Test func clearRange_multiByteKeyBoundary() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01, 0xFF])
        tx.setValue([2], for: [0x02, 0x00])
        tx.clearRange(begin: [0x02, 0x00], end: [0x03, 0x00])
        // [0x01, 0xFF] < begin → preserved
        let preservedValue = try await tx.getValue(for: [0x01, 0xFF])
        #expect(preservedValue == [1])
        // [0x02, 0x00] == begin → cleared
        let clearedValue = try await tx.getValue(for: [0x02, 0x00])
        #expect(clearedValue == nil)
        try await tx.commit()
    }

    // Also verify clearRange boundary on committed SQLite data
    @Test func clearRange_boundaryOnSQLiteData() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
            tx.setValue([5], for: [0x05])
        }

        try await engine.withTransaction { tx in
            tx.clearRange(begin: [0x02], end: [0x05])
        }

        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            let v3 = try await tx.getValue(for: [0x03])
            let v4 = try await tx.getValue(for: [0x04])
            let v5 = try await tx.getValue(for: [0x05])
            #expect(v1 == [1])   // before range
            #expect(v2 == nil)   // begin inclusive
            #expect(v3 == nil)   // within range
            #expect(v4 == nil)   // within range
            #expect(v5 == [5])   // end exclusive
        }
    }

    // =========================================================================
    // MARK: - Flush Semantics
    //
    // SQLite's getRange calls flushWriteBuffer() before executing SQL.
    // After flush, the buffer is empty. Subsequent getValue calls
    // will find no match in buffer and read from SQLite instead.
    // This is a key difference from InMemory.
    // =========================================================================

    private func collectRange(
        _ tx: any Transaction,
        begin: Bytes, end: Bytes
    ) async throws -> [(key: Bytes, value: Bytes)] {
        let seq = try await tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: Bytes, value: Bytes)] = []
        for try await item in seq { result.append(item) }
        return result
    }

    @Test func flush_getValueAfterFlushReadsSQLite() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()

        // Buffer has set(0x01, [10])
        tx.setValue([10], for: [0x01])

        // getRange triggers flush → buffer is now empty, data is in SQLite
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF])

        // getValue: no match in (now empty) buffer → reads SQLite → [10]
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
        try await tx.commit()
    }

    @Test func flush_writesAfterFlushAreBuffered() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()

        tx.setValue([10], for: [0x01])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush

        // New writes go into a fresh buffer
        tx.setValue([20], for: [0x02])
        tx.clear(key: [0x01])

        // getValue for [0x01]: clear in buffer → nil
        let clearedAfterFlush = try await tx.getValue(for: [0x01])
        #expect(clearedAfterFlush == nil)

        // getValue for [0x02]: set in buffer → [20]
        let newAfterFlush = try await tx.getValue(for: [0x02])
        #expect(newAfterFlush == [20])

        try await tx.commit()
    }

    @Test func flush_multipleFlushesAreIdempotent() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()

        tx.setValue([10], for: [0x01])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 1

        tx.setValue([20], for: [0x02])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 2

        tx.setValue([30], for: [0x03])
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 3

        #expect(range.count == 3)
        #expect(range[0].key == [0x01])
        #expect(range[1].key == [0x02])
        #expect(range[2].key == [0x03])
        try await tx.commit()
    }

    @Test func flush_clearAfterFlushThenGetRange() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()

        tx.setValue([10], for: [0x01])
        tx.setValue([20], for: [0x02])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush

        tx.clear(key: [0x01]) // buffer: clear(0x01)
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush again

        // After second flush: SQLite has [0x02] only
        #expect(range.count == 1)
        #expect(range[0].key == [0x02])
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - getValue / getRange Consistency
    // =========================================================================

    @Test func consistency_setClearSet() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        tx.setValue([2], for: [0x01])

        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])

        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 1)
        #expect(range[0].value == [2])
        try await tx.commit()
    }

    @Test func consistency_clearRangeThenSet() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x04])
        tx.setValue([99], for: [0x02])

        let crts1 = try await tx.getValue(for: [0x01])
        let crts2 = try await tx.getValue(for: [0x02])
        let crts3 = try await tx.getValue(for: [0x03])
        #expect(crts1 == nil)
        #expect(crts2 == [99])
        #expect(crts3 == nil)

        // getRange flushes: clearRange then set(0x02,99) applied to SQLite
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 1)
        #expect(range[0].key == [0x02])
        #expect(range[0].value == [99])
        try await tx.commit()
    }

    @Test func consistency_overwriteAndClearRange() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        tx.setValue([99], for: [0x01])    // overwrite
        tx.clearRange(begin: [0x01], end: [0x03])  // then clear range

        let oc1 = try await tx.getValue(for: [0x01])
        let oc2 = try await tx.getValue(for: [0x02])
        #expect(oc1 == nil)
        #expect(oc2 == nil)

        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 0)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - Transaction Lifecycle
    //
    // SQLite has both `committed` and `cancelled` flags:
    // - commit: checks !cancelled, checks !committed, flushes, COMMIT, releases lock
    // - cancel: checks !committed && !cancelled, sets cancelled, ROLLBACK, releases lock
    // =========================================================================

    @Test func commitPersists() async throws {
        let engine = try SQLiteStorageEngine()

        let tx1 = try engine.createTransaction()
        tx1.setValue([42], for: [0x01])
        try await tx1.commit()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: [0x01])
        #expect(value == [42])
        try await tx2.commit()
    }

    @Test func cancelRollsBack() async throws {
        let engine = try SQLiteStorageEngine()

        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        tx.cancel()

        let tx2 = try engine.createTransaction()
        let cancelledValue = try await tx2.getValue(for: [0x01])
        #expect(cancelledValue == nil)
        try await tx2.commit()
    }

    @Test func commitAfterCancel_throws() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.cancel()
        do {
            try await tx.commit()
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func doubleCommit_isNoOp() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        try await tx.commit()
        // Second commit should return early (guard !committed)
        try await tx.commit()
    }

    @Test func cancelAfterCommit_isNoOp() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        try await tx.commit()
        // Cancel after commit should return early (guard !committed)
        tx.cancel()

        // Data should still be persisted
        let tx2 = try engine.createTransaction()
        let persistedValue = try await tx2.getValue(for: [0x01])
        #expect(persistedValue == [42])
        try await tx2.commit()
    }

    @Test func cancelledTransactionThrowsOnRead() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.cancel()
        do {
            _ = try await tx.getValue(for: [0x01])
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func cancelledTransactionThrowsOnGetRange() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.cancel()
        do {
            _ = try await tx.getRange(begin: [0x00], end: [0xFF], limit: 0, reverse: false)
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func writesAfterCancelAreSilentlyIgnored() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.cancel()
        tx.setValue([42], for: [0x01])

        // Value should not be visible in next transaction
        let tx2 = try engine.createTransaction()
        let ignoredValue = try await tx2.getValue(for: [0x01])
        #expect(ignoredValue == nil)
        try await tx2.commit()
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = try SQLiteStorageEngine()
        struct TestError: Error {}

        do {
            try await engine.withTransaction { tx in
                tx.setValue([42], for: [0x01])
                throw TestError()
            }
        } catch is TestError {}

        try await engine.withTransaction { tx in
            let rolledBackValue = try await tx.getValue(for: [0x01])
            #expect(rolledBackValue == nil)
        }
    }

    // =========================================================================
    // MARK: - SQL Transaction Integrity
    //
    // Verifies BEGIN IMMEDIATE → COMMIT/ROLLBACK lifecycle and
    // NSLock acquire/release for transaction serialization.
    // =========================================================================

    @Test func sequentialTransactions() async throws {
        let engine = try SQLiteStorageEngine()

        // Multiple sequential transactions should work
        // (each acquires and releases the lock)
        for i: UInt8 in 0..<10 {
            try await engine.withTransaction { tx in
                tx.setValue([i], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 10)
        }
    }

    @Test func closeThenCreateTransaction_throws() async throws {
        let engine = try SQLiteStorageEngine()
        engine.close()
        do {
            _ = try engine.createTransaction()
            Issue.record("Expected error after close")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    // =========================================================================
    // MARK: - File Persistence
    // =========================================================================

    @Test func filePersistence_surviveReopen() async throws {
        let tmpDir = FileManager.default.temporaryDirectory
        let dbPath = tmpDir.appendingPathComponent("test-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: dbPath) }

        do {
            let engine = try SQLiteStorageEngine(path: dbPath)
            try await engine.withTransaction { tx in
                tx.setValue([1, 2, 3], for: [0x01])
            }
            engine.close()
        }

        do {
            let engine = try SQLiteStorageEngine(path: dbPath)
            try await engine.withTransaction { tx in
                let persistedData = try await tx.getValue(for: [0x01])
                #expect(persistedData == [1, 2, 3])
            }
            engine.close()
        }
    }

    // =========================================================================
    // MARK: - getRange Reverse + Limit
    // =========================================================================

    @Test func getRange_reverseThenLimit() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                tx.setValue([i * 10], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x01], end: [0x06], limit: 2, reverse: true
            )
            var collected: [(key: Bytes, value: Bytes)] = []
            for try await item in results {
                collected.append(item)
            }
            // Last 2 items: [5]=50, [4]=40
            #expect(collected.count == 2)
            #expect(collected[0].key == [0x05])
            #expect(collected[0].value == [50])
            #expect(collected[1].key == [0x04])
            #expect(collected[1].value == [40])
        }
    }

    // =========================================================================
    // MARK: - Ordering (SQLite BLOB comparison)
    // =========================================================================

    @Test func blobOrderingMatchesLexicographic() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            // Insert keys that test SQLite's BLOB ordering
            tx.setValue([1], for: [0x01, 0x02])
            tx.setValue([2], for: [0x01])
            tx.setValue([3], for: [0x01, 0x02, 0x03])
            tx.setValue([4], for: [0x02])
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            // Shorter prefix comes first in lexicographic order
            #expect(range[0].key == [0x01])
            #expect(range[1].key == [0x01, 0x02])
            #expect(range[2].key == [0x01, 0x02, 0x03])
            #expect(range[3].key == [0x02])
        }
    }

    // =========================================================================
    // MARK: - Tuple Integration
    // =========================================================================

    @Test func subspaceRangeIsolation() async throws {
        let engine = try SQLiteStorageEngine()
        let spaceA = Subspace("alpha")
        let spaceB = Subspace("beta")

        try await engine.withTransaction { tx in
            tx.setValue([1], for: spaceA.pack(Tuple(Int64(1))))
            tx.setValue([2], for: spaceA.pack(Tuple(Int64(2))))
            tx.setValue([3], for: spaceB.pack(Tuple(Int64(1))))
        }

        try await engine.withTransaction { tx in
            let (begin, end) = spaceA.range()
            let range = try await collectRange(tx, begin: begin, end: end)
            #expect(range.count == 2)
        }
    }
}
