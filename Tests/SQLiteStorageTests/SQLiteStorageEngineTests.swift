import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLiteStorageEngine Tests")
struct SQLiteStorageEngineTests {

    @Test func beginTransaction_busyIsRetryable() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("storage-kit-\(UUID().uuidString).sqlite")
            .path
        defer {
            do {
                try FileManager.default.removeItem(atPath: path)
            } catch {
                // Temporary-file cleanup is best effort in tests.
            }
        }

        let first = try SQLiteStorageEngine(configuration: .file(path))
        let second = try SQLiteStorageEngine(configuration: .file(path))
        defer {
            first.shutdown()
            second.shutdown()
        }

        let held = try first.createTransaction()
        _ = try await held.getValue(for: [0x00])
        let contending = try second.createTransaction()

        do {
            _ = try await contending.getValue(for: [0x00])
            Issue.record("Expected SQLite busy error")
        } catch let error as StorageError {
            #expect(error.code == .transactionBusy)
            #expect(error.backend == .sqlite)
            #expect(error.operation == .beginTransaction)
            #expect(error.isRetryable == true)
        }
        try await contending.cancel()
        try await held.cancel()
    }

    // =========================================================================
    // MARK: - Write Buffer Reverse Scan
    //
    // getValue iterates writeBuffer.reversed(). The FIRST matching operation
    // found in reverse order determines the result. Unmatched operations are
    // skipped; if no match, fall through to SQLite.
    // =========================================================================

    @Test func setThenClear_clearWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func setThenClearThenSet_lastSetWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        try tx.setValue([2], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])
        try await tx.commit()
    }

    @Test func setThenClearRange_clearRangeWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func clearRangeThenSet_setWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        try tx.setValue([99], for: [0x03])
        let value = try await tx.getValue(for: [0x03])
        #expect(value == [99])
        try await tx.commit()
    }

    @Test func multipleOverwrites_lastWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.setValue([2], for: [0x01])
        try tx.setValue([3], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [3])
        try await tx.commit()
    }

    @Test func setClearRangeSetClearRange_lastClearRangeWins() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        try tx.setValue([2], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func bufferOverridesSQLiteValue() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([20], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [20])
        try await tx.commit()
    }

    @Test func clearInBufferHidesSQLiteValue() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        try tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func noMatchInBufferFallsThroughToSQLite() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([99], for: [0xFF]) // different key
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        let beginValue = try await tx.getValue(for: [0x02])
        #expect(beginValue == nil)
        try await tx.commit()
    }

    @Test func clearRange_endExclusive() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x05])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        let endValue = try await tx.getValue(for: [0x05])
        #expect(endValue == [1])
        try await tx.commit()
    }

    @Test func clearRange_justBeforeEnd() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x04])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        let justBeforeEndValue = try await tx.getValue(for: [0x04])
        #expect(justBeforeEndValue == nil)
        try await tx.commit()
    }

    @Test func clearRange_justBeforeBegin() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        let justBeforeBeginValue = try await tx.getValue(for: [0x01])
        #expect(justBeforeBeginValue == [1])
        try await tx.commit()
    }

    @Test func clearRange_multiByteKeyBoundary() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01, 0xFF])
        try tx.setValue([2], for: [0x02, 0x00])
        try tx.clearRange(beginKey: [0x02, 0x00], endKey: [0x03, 0x00])
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([4], for: [0x04])
            try tx.setValue([5], for: [0x05])
        }

        try await engine.withTransaction { tx in
            try tx.clearRange(beginKey: [0x02], endKey: [0x05])
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
        _ tx: some Transaction,
        begin: Bytes, end: Bytes
    ) async throws -> [(key: Bytes, value: Bytes)] {
        let seq = tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq { result.append((key: key, value: value)) }
        return result
    }

    @Test func flush_getValueAfterFlushReadsSQLite() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()

        // Buffer has set(0x01, [10])
        try tx.setValue([10], for: [0x01])

        // getRange triggers flush → buffer is now empty, data is in SQLite
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF])

        // getValue: no match in (now empty) buffer → reads SQLite → [10]
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
        try await tx.commit()
    }

    @Test func flush_writesAfterFlushAreBuffered() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()

        try tx.setValue([10], for: [0x01])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush

        // New writes go into a fresh buffer
        try tx.setValue([20], for: [0x02])
        try tx.clear(key: [0x01])

        // getValue for [0x01]: clear in buffer → nil
        let clearedAfterFlush = try await tx.getValue(for: [0x01])
        #expect(clearedAfterFlush == nil)

        // getValue for [0x02]: set in buffer → [20]
        let newAfterFlush = try await tx.getValue(for: [0x02])
        #expect(newAfterFlush == [20])

        try await tx.commit()
    }

    @Test func flush_multipleFlushesAreIdempotent() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()

        try tx.setValue([10], for: [0x01])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 1

        try tx.setValue([20], for: [0x02])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 2

        try tx.setValue([30], for: [0x03])
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush 3

        #expect(range.count == 3)
        #expect(range[0].key == [0x01])
        #expect(range[1].key == [0x02])
        #expect(range[2].key == [0x03])
        try await tx.commit()
    }

    @Test func flush_clearAfterFlushThenGetRange() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()

        try tx.setValue([10], for: [0x01])
        try tx.setValue([20], for: [0x02])
        _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush

        try tx.clear(key: [0x01]) // buffer: clear(0x01)
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        try tx.setValue([2], for: [0x01])

        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])

        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 1)
        #expect(range[0].value == [2])
        try await tx.commit()
    }

    @Test func consistency_clearRangeThenSet() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
            try tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x01], endKey: [0x04])
        try tx.setValue([99], for: [0x02])

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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([99], for: [0x01])    // overwrite
        try tx.clearRange(beginKey: [0x01], endKey: [0x03])  // then clear range

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
    // SQLite transactions use a terminal lifecycle:
    // - commit failure marks the transaction failed and rolls back
    // - cancelled/failed transactions cannot be reused
    // - locks are released exactly once
    // =========================================================================

    @Test func commitPersists() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let tx1 = try engine.createTransaction()
        try tx1.setValue([42], for: [0x01])
        try await tx1.commit()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: [0x01])
        #expect(value == [42])
        try await tx2.commit()
    }

    @Test func cancelRollsBack() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let tx = try engine.createTransaction()
        try tx.setValue([42], for: [0x01])
        try await tx.cancel()

        let tx2 = try engine.createTransaction()
        let cancelledValue = try await tx2.getValue(for: [0x01])
        #expect(cancelledValue == nil)
        try await tx2.commit()
    }

    @Test func commitAfterCancel_throws() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try await tx.cancel()
        do {
            try await tx.commit()
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func repeatedCommitReturnsAuthoritativeSuccess() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([42], for: [0x01])
        try await tx.commit()
        try await tx.commit()
    }

    @Test func failedCommitTerminatesTransactionAndPreventsDoubleApply() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()

        try tx.setValue([0xAA], for: [0x01])
        try tx.atomicOp(key: [0x02], param: [0x00], mutationType: .setVersionstampedKey)

        do {
            try await tx.commit()
            Issue.record("Expected commit to fail")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        do {
            _ = try await tx.getValue(for: [0x01])
            Issue.record("Expected failed transaction to reject reads")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        do {
            try tx.setValue([0xBB], for: [0x03])
            Issue.record("Expected failed transaction to reject writes")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        do {
            try await tx.commit()
            Issue.record("Expected failed transaction to reject a second commit")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        let verifier = try engine.createTransaction()
        let firstValue = try await verifier.getValue(for: [0x01])
        let laterValue = try await verifier.getValue(for: [0x03])
        #expect(firstValue == nil)
        #expect(laterValue == nil)
        try await verifier.commit()
    }

    @Test func cancelAfterCommitReportsTerminalState() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try tx.setValue([42], for: [0x01])
        try await tx.commit()
        do {
            try await tx.cancel()
            Issue.record("Expected invalidOperation")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        // Data should still be persisted
        let tx2 = try engine.createTransaction()
        let persistedValue = try await tx2.getValue(for: [0x01])
        #expect(persistedValue == [42])
        try await tx2.commit()
    }

    @Test func cancelledTransactionThrowsOnRead() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try await tx.cancel()
        do {
            _ = try await tx.getValue(for: [0x01])
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func cancelledTransactionThrowsOnGetRange() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try await tx.cancel()
        do {
            let seq = tx.getRange(begin: [0x00], end: [0xFF], limit: 0, reverse: false)
            for try await _ in seq {
                Issue.record("Expected error")
            }
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func writesAfterCancelReportTerminalState() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let tx = try engine.createTransaction()
        try await tx.cancel()

        do {
            try tx.setValue([42], for: [0x01])
            Issue.record("Expected invalidOperation")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        let tx2 = try engine.createTransaction()
        let ignoredValue = try await tx2.getValue(for: [0x01])
        #expect(ignoredValue == nil)
        try await tx2.commit()
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        struct TransactionBodyFailure: Error {}

        do {
            try await engine.withTransaction { tx in
                try tx.setValue([42], for: [0x01])
                throw TransactionBodyFailure()
            }
        } catch is TransactionBodyFailure {}

        try await engine.withTransaction { tx in
            let rolledBackValue = try await tx.getValue(for: [0x01])
            #expect(rolledBackValue == nil)
        }
    }

    // =========================================================================
    // MARK: - SQL Transaction Integrity
    //
    // Verifies BEGIN IMMEDIATE → COMMIT/ROLLBACK lifecycle and FIFO actor
    // lease acquisition/release for transaction serialization.
    // =========================================================================

    @Test func sequentialTransactions() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        // Each transaction acquires and releases the FIFO lease.
        for i: UInt8 in 0..<10 {
            try await engine.withTransaction { tx in
                try tx.setValue([i], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 10)
        }
    }

    @Test func closeThenCreateTransaction_throws() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        engine.close()
        do {
            _ = try engine.createTransaction()
            Issue.record("Expected error after close")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
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
        defer {
            if FileManager.default.fileExists(atPath: dbPath) {
                do {
                    try FileManager.default.removeItem(atPath: dbPath)
                } catch {
                    Issue.record("Failed to remove SQLite test database: \(error)")
                }
            }
        }

        do {
            let engine = try SQLiteStorageEngine(configuration: .file(dbPath))
            try await engine.withTransaction { tx in
                try tx.setValue([1, 2, 3], for: [0x01])
            }
            engine.close()
        }

        do {
            let engine = try SQLiteStorageEngine(configuration: .file(dbPath))
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                try tx.setValue([i * 10], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let collected = try await tx.collectRange(
                begin: [0x01], end: [0x06], limit: 2, reverse: true
            )
            // Last 2 items: [5]=50, [4]=40
            #expect(collected.count == 2)
            #expect(collected[0].0 == [0x05])
            #expect(collected[0].1 == [50])
            #expect(collected[1].0 == [0x04])
            #expect(collected[1].1 == [40])
        }
    }

    // =========================================================================
    // MARK: - Ordering (SQLite BLOB comparison)
    // =========================================================================

    @Test func blobOrderingMatchesLexicographic() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { tx in
            // Insert keys that test SQLite's BLOB ordering
            try tx.setValue([1], for: [0x01, 0x02])
            try tx.setValue([2], for: [0x01])
            try tx.setValue([3], for: [0x01, 0x02, 0x03])
            try tx.setValue([4], for: [0x02])
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
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let spaceA = Subspace("alpha")
        let spaceB = Subspace("beta")

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: spaceA.pack(Tuple(Int64(1))))
            try tx.setValue([2], for: spaceA.pack(Tuple(Int64(2))))
            try tx.setValue([3], for: spaceB.pack(Tuple(Int64(1))))
        }

        try await engine.withTransaction { tx in
            let (begin, end) = spaceA.range()
            let range = try await collectRange(tx, begin: begin, end: end)
            #expect(range.count == 2)
        }
    }

    // =========================================================================
    // MARK: - Lease Release Safety (Regression)
    //
    // The coordinator lease must always advance after commit or cancel, even
    // when mutation application fails. Otherwise all later transactions wait
    // forever.
    // =========================================================================

    @Test func leaseReleasedAfterWithTransactionError() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        struct TransactionBodyFailure: Error {}

        // First transaction throws — its lease must be released.
        do {
            try await engine.withTransaction { tx in
                try tx.setValue([1], for: [0x01])
                throw TransactionBodyFailure()
            }
        } catch is TransactionBodyFailure {}

        // Second transaction succeeding proves the lease advanced.
        try await engine.withTransaction { tx in
            try tx.setValue([2], for: [0x02])
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0x02])
            #expect(value == [2])
        }
    }

    @Test func leaseReleasedAfterCancel() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        let tx1 = try engine.createTransaction()
        try tx1.setValue([1], for: [0x01])
        try await tx1.cancel()

        // Must not deadlock — cancellation must advance the lease.
        let tx2 = try engine.createTransaction()
        try tx2.setValue([2], for: [0x02])
        try await tx2.commit()

        let tx3 = try engine.createTransaction()
        let value = try await tx3.getValue(for: [0x02])
        #expect(value == [2])
        try await tx3.commit()
    }

    @Test(.timeLimit(.minutes(1)))
    func releasedActiveTransactionRollsBackAndAdvancesLease() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        var abandoned: SQLiteStorageTransaction? = try engine.createTransaction()
        try abandoned?.setValue([0xA1], for: [0x01])
        _ = try await abandoned?.getValue(for: [0x01])

        let follower = try engine.createTransaction()
        let followerTask = Task {
            let value = try await follower.getValue(for: [0x01])
            try await follower.commit()
            return value
        }
        await waitForWaitingLeaseCount(1, engine: engine)

        abandoned = nil
        let value = try await followerTask.value
        #expect(value == nil)
        #expect(await engine.leaseInstrumentation.hasActiveRoot == false)
    }

    @Test(.timeLimit(.minutes(1)))
    func fifoLeaseCompletesConcurrentAccess() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        // Multiple concurrent calls must all complete through the FIFO lease.
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i: UInt8 in 0..<20 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        try tx.setValue([i], for: [i])
                    }
                }
            }
            try await group.waitForAll()
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 20)
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func transactionLeaseAdvancesInFIFOOrder() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        let third = try engine.createTransaction()
        _ = try await first.getValue(for: [0x00])

        let completionOrder = SQLiteTransactionCompletionOrder()
        let secondTask = Task {
            _ = try await second.getValue(for: [0x00])
            await completionOrder.append(2)
            try await second.commit()
        }
        await waitForWaitingLeaseCount(1, engine: engine)

        let thirdTask = Task {
            _ = try await third.getValue(for: [0x00])
            await completionOrder.append(3)
            try await third.commit()
        }
        await waitForWaitingLeaseCount(2, engine: engine)

        try await first.commit()
        try await secondTask.value
        try await thirdTask.value
        #expect(await completionOrder.values == [2, 3])
    }

    @Test(.timeLimit(.minutes(1)))
    func cancelledLeaseWaiterIsRemovedWithoutBlockingFollowers() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let first = try engine.createTransaction()
        let cancelled = try engine.createTransaction()
        _ = try await first.getValue(for: [0x00])

        let blockedRead = Task {
            try await cancelled.getValue(for: [0x00])
        }
        await waitForWaitingLeaseCount(1, engine: engine)
        blockedRead.cancel()

        do {
            _ = try await blockedRead.value
            Issue.record("Expected the queued lease acquisition to cancel")
        } catch let error as StorageError {
            #expect(error.code == .transactionCancelled)
        }
        await waitForWaitingLeaseCount(0, engine: engine)
        try await cancelled.cancel()
        try await first.commit()

        let follower = try engine.createTransaction()
        _ = try await follower.getValue(for: [0x00])
        try await follower.commit()
    }

    @Test(.timeLimit(.minutes(1)))
    func shutdownWakesQueuedLeaseWaiters() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let first = try engine.createTransaction()
        let queued = try engine.createTransaction()
        _ = try await first.getValue(for: [0x00])

        let blockedRead = Task {
            try await queued.getValue(for: [0x00])
        }
        await waitForWaitingLeaseCount(1, engine: engine)
        engine.shutdown()

        do {
            _ = try await blockedRead.value
            Issue.record("Expected shutdown to reject the queued lease")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
        try await queued.cancel()
        try await first.cancel()
    }

    @Test func leaseReleasedAfterMultipleErrorsInSequence() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        struct TransactionBodyFailure: Error {}

        // Three consecutive failing transactions
        for _ in 0..<3 {
            do {
                try await engine.withTransaction { tx in
                    try tx.setValue([1], for: [0x01])
                    throw TransactionBodyFailure()
                }
            } catch is TransactionBodyFailure {}
        }

        // Must still work after repeated failures
        try await engine.withTransaction { tx in
            try tx.setValue([99], for: [0x01])
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0x01])
            #expect(value == [99])
        }
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

private actor SQLiteTransactionCompletionOrder {
    private(set) var values: [Int] = []

    func append(_ value: Int) {
        values.append(value)
    }
}
