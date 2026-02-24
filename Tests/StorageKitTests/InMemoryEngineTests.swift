import Testing
import Foundation
@testable import StorageKit

@Suite("InMemoryEngine Tests")
struct InMemoryEngineTests {

    // =========================================================================
    // MARK: - Write Buffer Reverse Scan
    //
    // getValue iterates writeBuffer.reversed(). The FIRST matching operation
    // found in reverse order determines the result. This is the core invariant
    // of read-your-writes semantics.
    // =========================================================================

    @Test func setThenClear_clearWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
    }

    @Test func setThenClearThenSet_lastSetWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        tx.setValue([2], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])
    }

    @Test func setThenClearRange_clearRangeWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func clearRangeThenSet_setWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x05])
        tx.setValue([99], for: [0x03])
        let value = try await tx.getValue(for: [0x03])
        #expect(value == [99])
    }

    @Test func multipleOverwrites_lastWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.setValue([2], for: [0x01])
        tx.setValue([3], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [3])
    }

    @Test func setClearRangeSetClearRange_lastClearRangeWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        tx.setValue([2], for: [0x02])
        tx.clearRange(begin: [0x01], end: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func bufferOverridesSnapshot() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        // Snapshot has [0x01]=10. Buffer overwrites it.
        tx.setValue([20], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [20])
    }

    @Test func clearInBufferHidesSnapshotValue() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
    }

    @Test func clearRangeInBufferHidesSnapshotValues() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x03])
        let cr1 = try await tx.getValue(for: [0x01])
        let cr2 = try await tx.getValue(for: [0x02])
        let cr3 = try await tx.getValue(for: [0x03])
        #expect(cr1 == nil)
        #expect(cr2 == nil)
        #expect(cr3 == [30])
    }

    @Test func noMatchInBufferFallsThroughToSnapshot() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        // Buffer has an operation on a DIFFERENT key
        tx.setValue([99], for: [0xFF])
        // [0x01] should fall through to snapshot
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
    }

    // =========================================================================
    // MARK: - clearRange Boundary Semantics
    //
    // clearRange uses: compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0
    // begin is INCLUSIVE, end is EXCLUSIVE.
    // =========================================================================

    @Test func clearRange_beginInclusive() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x02])
        tx.clearRange(begin: [0x02], end: [0x05])
        // key == begin → inside range → nil
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func clearRange_endExclusive() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x05])
        tx.clearRange(begin: [0x02], end: [0x05])
        // key == end → outside range → value preserved
        let value = try await tx.getValue(for: [0x05])
        #expect(value == [1])
    }

    @Test func clearRange_justBeforeEnd() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x04])
        tx.clearRange(begin: [0x02], end: [0x05])
        // key < end → inside range → nil
        let value = try await tx.getValue(for: [0x04])
        #expect(value == nil)
    }

    @Test func clearRange_justBeforeBegin() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clearRange(begin: [0x02], end: [0x05])
        // key < begin → outside range → value preserved
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [1])
    }

    @Test func clearRange_justAfterEnd() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x06])
        tx.clearRange(begin: [0x02], end: [0x05])
        // key > end → outside range → value preserved
        let value = try await tx.getValue(for: [0x06])
        #expect(value == [1])
    }

    @Test func clearRange_multiByteKeyBoundary() async throws {
        let engine = InMemoryEngine()
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
    }

    // =========================================================================
    // MARK: - getValue / getRange Consistency
    //
    // Both getValue and getRange observe the same logical state.
    // InMemory: getRange builds an "effective store" by applying buffer ops
    //   in forward order. getValue scans buffer in reverse. Both should agree.
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

    @Test func consistency_setClearSet() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.clear(key: [0x01])
        tx.setValue([2], for: [0x01])

        // getValue sees [2]
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])

        // getRange should also see [0x01]=[2]
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 1)
        #expect(range[0].key == [0x01])
        #expect(range[0].value == [2])
    }

    @Test func consistency_clearRangeThenSet() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x04])
        tx.setValue([99], for: [0x02])

        // getValue: [0x01]=nil (clearRange), [0x02]=99 (set after clearRange), [0x03]=nil (clearRange)
        let crts1 = try await tx.getValue(for: [0x01])
        let crts2 = try await tx.getValue(for: [0x02])
        let crts3 = try await tx.getValue(for: [0x03])
        #expect(crts1 == nil)
        #expect(crts2 == [99])
        #expect(crts3 == nil)

        // getRange should agree
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 1)
        #expect(range[0].key == [0x02])
        #expect(range[0].value == [99])
    }

    @Test func consistency_overwriteAndClearRange() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        tx.setValue([99], for: [0x01])    // overwrite 0x01
        tx.clearRange(begin: [0x01], end: [0x03])  // then clear entire range

        // getValue: both cleared by clearRange (which comes after set)
        let oc1 = try await tx.getValue(for: [0x01])
        let oc2 = try await tx.getValue(for: [0x02])
        #expect(oc1 == nil)
        #expect(oc2 == nil)

        // getRange should agree
        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 0)
    }

    @Test func getRange_includesBufferedNewKeys() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        tx.setValue([20], for: [0x02])  // new key in buffer

        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 3)
        // Should be in lexicographic order including buffered key
        #expect(range[0].key == [0x01])
        #expect(range[1].key == [0x02])
        #expect(range[2].key == [0x03])
    }

    @Test func getRange_excludesClearedKeys() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        tx.clear(key: [0x02])

        let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
        #expect(range.count == 2)
        #expect(range[0].key == [0x01])
        #expect(range[1].key == [0x03])
    }

    // =========================================================================
    // MARK: - getRange Reverse + Limit Interaction
    //
    // reverse=true, limit=N: take the last N items in descending order.
    // =========================================================================

    @Test func getRange_reverseThenLimit() async throws {
        let engine = InMemoryEngine()

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
            // Should be the last 2 items: [5]=50, [4]=40
            #expect(collected.count == 2)
            #expect(collected[0].key == [0x05])
            #expect(collected[0].value == [50])
            #expect(collected[1].key == [0x04])
            #expect(collected[1].value == [40])
        }
    }

    // =========================================================================
    // MARK: - Snapshot Isolation
    //
    // A transaction sees a snapshot taken at creation time.
    // Concurrent commits should not affect an open transaction's reads.
    // =========================================================================

    @Test func snapshotIsolation() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        // tx1 takes snapshot with [0x01]=10
        let tx1 = try engine.createTransaction()

        // tx2 overwrites [0x01] and commits
        try await engine.withTransaction { tx in
            tx.setValue([20], for: [0x01])
        }

        // tx1 should still see [0x01]=10 from its snapshot
        let value = try await tx1.getValue(for: [0x01])
        #expect(value == [10])
    }

    // =========================================================================
    // MARK: - Transaction Lifecycle
    // =========================================================================

    @Test func commitAppliesBufferToStore() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        try await tx.commit()
        #expect(engine.count == 1)
    }

    @Test func cancelDiscards() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        tx.cancel()
        #expect(engine.count == 0)
    }

    @Test func cancelledTransactionThrowsOnGetValue() async throws {
        let engine = InMemoryEngine()
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
        let engine = InMemoryEngine()
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

    @Test func cancelledTransactionThrowsOnCommit() async throws {
        let engine = InMemoryEngine()
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

    @Test func writesAfterCancelAreSilentlyIgnored() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.cancel()
        tx.setValue([42], for: [0x01])
        tx.clear(key: [0x02])
        tx.clearRange(begin: [0x03], end: [0x04])
        #expect(engine.count == 0)
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = InMemoryEngine()

        struct TestError: Error {}

        do {
            try await engine.withTransaction { tx in
                tx.setValue([42], for: [0x01])
                throw TestError()
            }
        } catch is TestError {}

        #expect(engine.count == 0)
    }

    @Test func withTransaction_autoCommits() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([42], for: [0x01])
        }

        #expect(engine.count == 1)
    }

    // =========================================================================
    // MARK: - Large Data / Ordering
    // =========================================================================

    @Test func largeRangeScan_maintainsOrder() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            for i: UInt16 in 0..<500 {
                let key = withUnsafeBytes(of: i.bigEndian) { Array($0) }
                tx.setValue(key, for: key)
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x00, 0x00], end: [0xFF, 0xFF], limit: 0, reverse: false
            )
            var prevKey: Bytes?
            var count = 0
            for try await item in results {
                if let prev = prevKey {
                    // Verify ascending order via compareBytes
                    #expect(prev.lexicographicallyPrecedes(item.key))
                }
                prevKey = item.key
                count += 1
            }
            #expect(count == 500)
        }
    }

    @Test func concurrentTransactions() async throws {
        let engine = InMemoryEngine()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i: UInt8 in 0..<10 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        tx.setValue([i], for: [i])
                    }
                }
            }
            try await group.waitForAll()
        }
        #expect(engine.count == 10)
    }

    // =========================================================================
    // MARK: - Tuple Integration
    // =========================================================================

    @Test func subspaceRangeIsolation() async throws {
        let engine = InMemoryEngine()
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
