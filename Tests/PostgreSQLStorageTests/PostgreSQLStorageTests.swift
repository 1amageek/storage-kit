import Testing
import Foundation
@testable import PostgreSQLStorage
@testable import StorageKit

/// PostgreSQL storage backend tests.
///
/// Requires a running PostgreSQL instance. Set these environment variables:
/// - `POSTGRES_TEST_HOST` (required, e.g. "localhost")
/// - `POSTGRES_TEST_PORT` (optional, default: 5432)
/// - `POSTGRES_TEST_USER` (optional, default: "postgres")
/// - `POSTGRES_TEST_PASSWORD` (optional, default: "")
/// - `POSTGRES_TEST_DB` (optional, default: "storage_kit_test")
///
/// Quick start with Docker:
/// ```
/// docker run --rm -d -p 5432:5432 \
///   -e POSTGRES_PASSWORD=test \
///   -e POSTGRES_DB=storage_kit_test \
///   postgres:16
/// ```
extension AllPostgreSQLTests {
@Suite("PostgreSQLStorage Tests", .serialized)
struct PostgreSQLStorageTests {

    private func makeEngine() async throws -> PostgreSQLStorageEngine {
        let engine = try await PostgreSQLTestHelper.makeEngine()

        // Clean all data — suites are serialized so no concurrent conflict
        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
        }

        return engine
    }

    private func collectRange(
        _ tx: some Transaction,
        begin: Bytes, end: Bytes,
        limit: Int = 0,
        reverse: Bool = false
    ) async throws -> [(key: Bytes, value: Bytes)] {
        let seq = tx.getRange(begin: begin, end: end, limit: limit, reverse: reverse)
        var result: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq { result.append((key: key, value: value)) }
        return result
    }

    // =========================================================================
    // MARK: - Basic CRUD
    // =========================================================================

    @Test func basicSetAndGet() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: [UInt8] = [0x01, 0x02, 0x03]
        let value: [UInt8] = [0xAA, 0xBB, 0xCC]

        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key)
        }
        #expect(result == value)
    }

    @Test func clearKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: [UInt8] = [0x10]
        let value: [UInt8] = [0x20]

        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }

        try await engine.withTransaction { tx in
            tx.clear(key: key)
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key)
        }
        #expect(result == nil)
    }

    @Test func clearRange() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
        }

        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x02], endKey: [0x04])
        }

        let results = try await engine.withTransaction { tx -> [[UInt8]?] in
            let r1 = try await tx.getValue(for: [0x01])
            let r2 = try await tx.getValue(for: [0x02])
            let r3 = try await tx.getValue(for: [0x03])
            let r4 = try await tx.getValue(for: [0x04])
            return [r1, r2, r3, r4]
        }
        #expect(results[0] == [1])
        #expect(results[1] == nil)
        #expect(results[2] == nil)
        #expect(results[3] == [4])
    }

    @Test func rangeQuery() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x0A])
            tx.setValue([20], for: [0x14])
            tx.setValue([30], for: [0x1E])
            tx.setValue([40], for: [0x28])
        }

        let results = try await engine.withTransaction { tx -> [(Bytes, Bytes)] in
            try await tx.collectRange(
                begin: [0x0A], end: [0x28]
            )
        }

        #expect(results.count == 3)
        #expect(results[0].0 == [0x0A])
        #expect(results[1].0 == [0x14])
        #expect(results[2].0 == [0x1E])
    }

    // =========================================================================
    // MARK: - Write Buffer Reverse Scan
    //
    // getValue iterates writeBuffer.reversed(). The FIRST matching operation
    // found in reverse order determines the result. Unmatched operations are
    // skipped; if no match, fall through to PostgreSQL.
    // =========================================================================

    @Test func setThenClear_clearWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x01])
            tx.clear(key: [0x01])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == nil)
    }

    @Test func setThenClearThenSet_lastSetWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x01])
            tx.clear(key: [0x01])
            tx.setValue([2], for: [0x01])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == [2])
    }

    @Test func setThenClearRange_clearRangeWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x02])
            tx.clearRange(beginKey: [0x01], endKey: [0x05])
            return try await tx.getValue(for: [0x02])
        }
        #expect(result == nil)
    }

    @Test func clearRangeThenSet_setWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.clearRange(beginKey: [0x01], endKey: [0x05])
            tx.setValue([99], for: [0x03])
            return try await tx.getValue(for: [0x03])
        }
        #expect(result == [99])
    }

    @Test func multipleOverwrites_lastWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x01])
            tx.setValue([3], for: [0x01])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == [3])
    }

    @Test func setClearRangeSetClearRange_lastClearRangeWins() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x02])
            tx.clearRange(beginKey: [0x01], endKey: [0x05])
            tx.setValue([2], for: [0x02])
            tx.clearRange(beginKey: [0x01], endKey: [0x05])
            return try await tx.getValue(for: [0x02])
        }
        #expect(result == nil)
    }

    @Test func bufferOverridesPersistedValue() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([20], for: [0x01])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == [20])
    }

    @Test func clearInBufferHidesPersistedValue() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.clear(key: [0x01])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == nil)
    }

    @Test func noMatchInBufferFallsThroughToPostgreSQL() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([99], for: [0xFF]) // different key
            // [0x01] not in buffer → falls through to PostgreSQL
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == [10])
    }

    // =========================================================================
    // MARK: - clearRange Boundary Semantics
    //
    // compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0
    // begin INCLUSIVE, end EXCLUSIVE.
    // =========================================================================

    @Test func clearRange_beginInclusive() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x02])
            tx.clearRange(beginKey: [0x02], endKey: [0x05])
            return try await tx.getValue(for: [0x02])
        }
        #expect(result == nil)
    }

    @Test func clearRange_endExclusive() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x05])
            tx.clearRange(beginKey: [0x02], endKey: [0x05])
            return try await tx.getValue(for: [0x05])
        }
        #expect(result == [1])
    }

    @Test func clearRange_justBeforeEnd() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x04])
            tx.clearRange(beginKey: [0x02], endKey: [0x05])
            return try await tx.getValue(for: [0x04])
        }
        #expect(result == nil)
    }

    @Test func clearRange_justBeforeBegin() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([1], for: [0x01])
            tx.clearRange(beginKey: [0x02], endKey: [0x05])
            return try await tx.getValue(for: [0x01])
        }
        #expect(result == [1])
    }

    @Test func clearRange_multiByteKeyBoundary() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let results = try await engine.withTransaction { tx -> ([UInt8]?, [UInt8]?) in
            tx.setValue([1], for: [0x01, 0xFF])
            tx.setValue([2], for: [0x02, 0x00])
            tx.clearRange(beginKey: [0x02, 0x00], endKey: [0x03, 0x00])
            // [0x01, 0xFF] < begin → preserved
            let preserved = try await tx.getValue(for: [0x01, 0xFF])
            // [0x02, 0x00] == begin → cleared
            let cleared = try await tx.getValue(for: [0x02, 0x00])
            return (preserved, cleared)
        }
        #expect(results.0 == [1])
        #expect(results.1 == nil)
    }

    @Test func clearRange_boundaryOnPersistedData() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
            tx.setValue([5], for: [0x05])
        }

        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x02], endKey: [0x05])
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
    // PostgreSQL's getRange flushes the write buffer before executing SQL.
    // After flush, the buffer is empty. Subsequent getValue calls
    // will find no match in buffer and read from PostgreSQL instead.
    // =========================================================================

    @Test func flush_getValueAfterFlushReadsPostgreSQL() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            // Buffer has set(0x01, [10])
            tx.setValue([10], for: [0x01])

            // getRange triggers flush → buffer is now empty, data is in PostgreSQL
            _ = try await collectRange(tx, begin: [0x00], end: [0xFF])

            // getValue: no match in (now empty) buffer → reads PostgreSQL → [10]
            let value = try await tx.getValue(for: [0x01])
            #expect(value == [10])
        }
    }

    @Test func flush_writesAfterFlushAreBuffered() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
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
        }
    }

    @Test func flush_multipleFlushesAreIdempotent() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
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
        }
    }

    @Test func flush_clearAfterFlushThenGetRange() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            _ = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush

            tx.clear(key: [0x01]) // buffer: clear(0x01)
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF]) // flush again

            // After second flush: PostgreSQL has [0x02] only
            #expect(range.count == 1)
            #expect(range[0].key == [0x02])
        }
    }

    // =========================================================================
    // MARK: - getValue / getRange Consistency
    // =========================================================================

    @Test func consistency_setClearSet() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.clear(key: [0x01])
            tx.setValue([2], for: [0x01])

            let value = try await tx.getValue(for: [0x01])
            #expect(value == [2])

            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 1)
            #expect(range[0].value == [2])
        }
    }

    @Test func consistency_clearRangeThenSet() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x01], endKey: [0x04])
            tx.setValue([99], for: [0x02])

            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            let v3 = try await tx.getValue(for: [0x03])
            #expect(v1 == nil)
            #expect(v2 == [99])
            #expect(v3 == nil)

            // getRange flushes: clearRange then set(0x02,99) applied to PostgreSQL
            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 1)
            #expect(range[0].key == [0x02])
            #expect(range[0].value == [99])
        }
    }

    @Test func consistency_overwriteAndClearRange() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        try await engine.withTransaction { tx in
            tx.setValue([99], for: [0x01])    // overwrite
            tx.clearRange(beginKey: [0x01], endKey: [0x03])  // then clear range

            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            #expect(v1 == nil)
            #expect(v2 == nil)

            let range = try await collectRange(tx, begin: [0x00], end: [0xFF])
            #expect(range.count == 0)
        }
    }

    // =========================================================================
    // MARK: - Read-Your-Writes
    // =========================================================================

    @Test func readYourWritesWithinTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.setValue([0xFF], for: [0x42])
            return try await tx.getValue(for: [0x42])
        }
        #expect(result == [0xFF])
    }

    @Test func readYourWritesClearWithinTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x50])
        }

        let result = try await engine.withTransaction { tx -> [UInt8]? in
            tx.clear(key: [0x50])
            return try await tx.getValue(for: [0x50])
        }
        #expect(result == nil)
    }

    // =========================================================================
    // MARK: - Transaction Lifecycle
    //
    // PostgreSQL uses withTransaction for all operations.
    // The engine handles BEGIN/COMMIT/ROLLBACK automatically.
    // =========================================================================

    @Test func commitPersistsAcrossTransactions() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([42], for: [0x01])
        }

        let value = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(value == [42])
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }
        struct TestError: Error {}

        do {
            try await engine.withTransaction { tx in
                tx.setValue([42], for: [0x01])
                throw TestError()
            }
        } catch is TestError {}

        let rolledBackValue = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(rolledBackValue == nil)
    }

    @Test func writesAfterCancelAreSilentlyIgnored() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.cancel()
            tx.setValue([42], for: [0x01])
        }

        // Value should not be visible in next transaction
        let value = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(value == nil)
    }

    @Test func cancelledTransactionThrowsOnRead() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        do {
            try await engine.withTransaction { tx in
                tx.cancel()
                _ = try await tx.getValue(for: [0x01])
            }
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func cancelledTransactionReturnsErrorOnGetRange() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        do {
            try await engine.withTransaction { tx in
                tx.cancel()
                let seq = tx.getRange(begin: [0x00], end: [0xFF], limit: 0, reverse: false)
                for try await _ in seq {
                    Issue.record("Should not yield any elements")
                }
            }
            Issue.record("Expected error")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func sequentialTransactions() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        for i: UInt8 in 0..<10 {
            try await engine.withTransaction { tx in
                tx.setValue([i], for: [i])
            }
        }

        let range = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x00], end: [0xFF])
        }
        #expect(range.count == 10)
    }

    // =========================================================================
    // MARK: - getRange Reverse + Limit
    // =========================================================================

    @Test func getRange_reverseThenLimit() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                tx.setValue([i * 10], for: [i])
            }
        }

        let collected = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x01], end: [0x06], limit: 2, reverse: true)
        }
        // Last 2 items: [5]=50, [4]=40
        #expect(collected.count == 2)
        #expect(collected[0].key == [0x05])
        #expect(collected[0].value == [50])
        #expect(collected[1].key == [0x04])
        #expect(collected[1].value == [40])
    }

    @Test func getRange_limitForward() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                tx.setValue([i * 10], for: [i])
            }
        }

        let collected = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x01], end: [0x06], limit: 3, reverse: false)
        }
        #expect(collected.count == 3)
        #expect(collected[0].key == [0x01])
        #expect(collected[1].key == [0x02])
        #expect(collected[2].key == [0x03])
    }

    @Test func getRange_emptyResult() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let collected = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x01], end: [0x05])
        }
        #expect(collected.count == 0)
    }

    // =========================================================================
    // MARK: - Ordering (BYTEA comparison)
    // =========================================================================

    @Test func byteOrdering() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Verify BYTEA comparison matches lexicographic ordering
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x00, 0x01])
            tx.setValue([2], for: [0x00, 0xFF])
            tx.setValue([3], for: [0x01, 0x00])
        }

        let results = try await engine.withTransaction { tx -> [(Bytes, Bytes)] in
            try await tx.collectRange(
                begin: [0x00], end: [0x02]
            )
        }
        #expect(results.count == 3)
        #expect(results[0].0 == [0x00, 0x01])
        #expect(results[1].0 == [0x00, 0xFF])
        #expect(results[2].0 == [0x01, 0x00])
    }

    @Test func blobOrderingMatchesLexicographic() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            // Insert keys that test BYTEA ordering
            tx.setValue([1], for: [0x01, 0x02])
            tx.setValue([2], for: [0x01])
            tx.setValue([3], for: [0x01, 0x02, 0x03])
            tx.setValue([4], for: [0x02])
        }

        let range = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x00], end: [0xFF])
        }
        // Shorter prefix comes first in lexicographic order
        #expect(range[0].key == [0x01])
        #expect(range[1].key == [0x01, 0x02])
        #expect(range[2].key == [0x01, 0x02, 0x03])
        #expect(range[3].key == [0x02])
    }

    // =========================================================================
    // MARK: - Tuple/Subspace Integration
    // =========================================================================

    @Test func subspaceRangeIsolation() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }
        let spaceA = Subspace("alpha")
        let spaceB = Subspace("beta")

        try await engine.withTransaction { tx in
            tx.setValue([1], for: spaceA.pack(Tuple(Int64(1))))
            tx.setValue([2], for: spaceA.pack(Tuple(Int64(2))))
            tx.setValue([3], for: spaceB.pack(Tuple(Int64(1))))
        }

        let range = try await engine.withTransaction { tx in
            let (begin, end) = spaceA.range()
            return try await collectRange(tx, begin: begin, end: end)
        }
        #expect(range.count == 2)
    }

    // =========================================================================
    // MARK: - Nested Transactions
    // =========================================================================

    @Test func nestedTransactionReuse() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x60])

            // Nested withTransaction should reuse the same transaction
            try await engine.withTransaction { innerTx in
                innerTx.setValue([2], for: [0x61])
            }
        }

        let r1 = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x60])
        }
        let r2 = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x61])
        }
        #expect(r1 == [1])
        #expect(r2 == [2])
    }

    @Test func nestedWithTransaction_innerSeesOuterWrites() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            try await engine.withTransaction { innerTx in
                innerTx.setValue([20], for: [0x02])

                // Inner should see outer's writes
                let v1 = try await innerTx.getValue(for: [0x01])
                #expect(v1 == [10])
            }

            // Outer should see inner's writes
            let v2 = try await outerTx.getValue(for: [0x02])
            #expect(v2 == [20])
        }
    }

    @Test func nestedWithTransaction_errorInInner_propagatesToOuter() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }
        struct InnerError: Error {}

        do {
            try await engine.withTransaction { outerTx in
                outerTx.setValue([10], for: [0x01])

                try await engine.withTransaction { innerTx in
                    innerTx.setValue([20], for: [0x02])
                    throw InnerError()
                }
            }
            Issue.record("Expected error")
        } catch is InnerError {}

        // Everything should be rolled back
        let v1 = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        let v2 = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x02])
        }
        #expect(v1 == nil)
        #expect(v2 == nil)
    }

    @Test func multipleSequentialNestedTransactions() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            try await engine.withTransaction { inner1 in
                inner1.setValue([20], for: [0x02])
            }

            try await engine.withTransaction { inner2 in
                inner2.setValue([30], for: [0x03])
                // Should see writes from both outer and inner1
                let v1 = try await inner2.getValue(for: [0x01])
                let v2 = try await inner2.getValue(for: [0x02])
                #expect(v1 == [10])
                #expect(v2 == [20])
            }
        }

        let results = try await engine.withTransaction { tx -> ([UInt8]?, [UInt8]?, [UInt8]?) in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            let v3 = try await tx.getValue(for: [0x03])
            return (v1, v2, v3)
        }
        #expect(results.0 == [10])
        #expect(results.1 == [20])
        #expect(results.2 == [30])
    }

    // =========================================================================
    // MARK: - Shutdown / Lifecycle
    // =========================================================================

    @Test func shutdown_idempotent() async throws {
        let engine = try await makeEngine()
        engine.shutdown()
        engine.shutdown() // Second call should not crash
    }

    // =========================================================================
    // MARK: - DirectoryService
    // =========================================================================

    @Test func directoryServiceIsStatic() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let subspace1 = try await engine.directoryService.createOrOpen(path: ["app", "users"])
        let subspace2 = try await engine.directoryService.createOrOpen(path: ["app", "users"])
        #expect(subspace1 == subspace2)

        let exists = try await engine.directoryService.exists(path: ["app", "users"])
        #expect(exists == true)
    }

    // =========================================================================
    // MARK: - PostgreSQLRangeResult Error Path
    // =========================================================================

    @Test func rangeResult_errorThrowsOnIteration() async throws {
        let result = PostgreSQLRangeResult(error: StorageError.backendError("test"))

        do {
            for try await _ in result {
                Issue.record("Should not yield any elements")
            }
            Issue.record("Expected error to be thrown")
        } catch let error as StorageError {
            guard case .backendError = error else {
                Issue.record("Expected backendError, got \(error)")
                return
            }
        }
    }

    // =========================================================================
    // MARK: - Concurrent Transactions (MVCC)
    //
    // PostgreSQL supports concurrent transactions via MVCC.
    // Multiple withTransaction calls can run in parallel.
    // =========================================================================

    @Test(.timeLimit(.minutes(1)))
    func concurrentTransactions_noBlocking() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Multiple concurrent withTransaction calls must all complete
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i: UInt8 in 0..<20 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        tx.setValue([i], for: [i])
                    }
                }
            }
            try await group.waitForAll()
        }

        let range = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [0x00], end: [0xFF])
        }
        #expect(range.count == 20)
    }

    @Test(.timeLimit(.minutes(1)))
    func concurrentTransactions_differentKeys() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Concurrent writes to different keys should not interfere
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i: UInt8 in 0..<10 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        // Write to unique key and then read it back
                        tx.setValue([i * 10], for: [0xA0 + i])
                    }
                }
            }
            try await group.waitForAll()
        }

        // Verify all writes are visible
        for i: UInt8 in 0..<10 {
            let value = try await engine.withTransaction { tx in
                try await tx.getValue(for: [0xA0 + i])
            }
            #expect(value == [i * 10])
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func concurrentReadAndWrite() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Seed data
        try await engine.withTransaction { tx in
            tx.setValue([42], for: [0x01])
        }

        // Concurrent reads + writes to different keys
        try await withThrowingTaskGroup(of: Void.self) { group in
            // Readers
            for _ in 0..<10 {
                group.addTask {
                    let value = try await engine.withTransaction { tx in
                        try await tx.getValue(for: [0x01])
                    }
                    #expect(value == [42])
                }
            }
            // Writers (different keys)
            for i: UInt8 in 0..<10 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        tx.setValue([i], for: [0xF0 + i])
                    }
                }
            }
            try await group.waitForAll()
        }
    }

    // =========================================================================
    // MARK: - Error Recovery
    // =========================================================================

    @Test func errorRecovery_transactionFailThenSucceed() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }
        struct TestError: Error {}

        // Three consecutive failing transactions
        for _ in 0..<3 {
            do {
                try await engine.withTransaction { tx in
                    tx.setValue([1], for: [0x01])
                    throw TestError()
                }
            } catch is TestError {}
        }

        // Must still work after repeated failures
        try await engine.withTransaction { tx in
            tx.setValue([99], for: [0x01])
        }

        let value = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(value == [99])
    }

    // =========================================================================
    // MARK: - Atomic Operations
    // =========================================================================

    @Test func atomicOp_addOnNewKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // atomicOp(.add) on non-existent key: should create entry
        // Param = 5 as little-endian Int64
        var addend: Int64 = 5
        let param = withUnsafeBytes(of: &addend) { Array($0) }

        try await engine.withTransaction { tx in
            tx.atomicOp(key: [0x01], param: param, mutationType: .add)
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(result != nil)
        if let result {
            #expect(result.count == 8)
            let value = result.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
            #expect(value == 5) // 0 + 5 = 5
        }
    }

    @Test func atomicOp_addOnExistingKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Set initial value = 10 as little-endian Int64
        var initial: Int64 = 10
        let initialBytes = withUnsafeBytes(of: &initial) { Array($0) }

        try await engine.withTransaction { tx in
            tx.setValue(initialBytes, for: [0x01])
        }

        // Add 7
        var addend: Int64 = 7
        let param = withUnsafeBytes(of: &addend) { Array($0) }

        try await engine.withTransaction { tx in
            tx.atomicOp(key: [0x01], param: param, mutationType: .add)
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(result != nil)
        if let result {
            let value = result.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
            #expect(value == 17) // 10 + 7 = 17
        }
    }

    @Test func atomicOp_addAccumulates() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Multiple sequential adds
        for i: Int64 in 1...5 {
            var addend = i
            let param = withUnsafeBytes(of: &addend) { Array($0) }
            try await engine.withTransaction { tx in
                tx.atomicOp(key: [0x01], param: param, mutationType: .add)
            }
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(result != nil)
        if let result {
            let value = result.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
            #expect(value == 15) // 1+2+3+4+5 = 15
        }
    }

    @Test func atomicOp_unsupportedTypesThrow() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        try await engine.withTransaction { tx in
            tx.setValue([42], for: [0x01])
        }

        // .max, .min, .bitOr etc. are not implemented — should throw on commit
        do {
            try await engine.withTransaction { tx in
                tx.atomicOp(key: [0x01], param: [0xFF], mutationType: .max)
            }
            Issue.record("Expected error for unsupported atomicOp")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }

        // Value should remain unchanged (transaction rolled back)
        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: [0x01])
        }
        #expect(result == [42])
    }

    @Test func atomicOp_versionstampThrows() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        do {
            try await engine.withTransaction { tx in
                tx.atomicOp(key: [0x01], param: [0x00], mutationType: .setVersionstampedKey)
            }
            Issue.record("Expected error for versionstamp")
        } catch let error as StorageError {
            guard case .invalidOperation = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    // =========================================================================
    // MARK: - Error Mapping (Static)
    // =========================================================================

    @Test func mapError_storageErrorPassthrough() {
        let original = StorageError.transactionConflict
        let mapped = PostgreSQLStorageEngine.mapError(original)
        if case .transactionConflict = mapped {
            // pass
        } else {
            Issue.record("Expected passthrough, got \(mapped)")
        }
    }

    @Test func mapError_unknownErrorWrapped() {
        struct UnknownError: Error {}
        let mapped = PostgreSQLStorageEngine.mapError(UnknownError())
        if case .backendError = mapped {
            // pass
        } else {
            Issue.record("Expected backendError, got \(mapped)")
        }
    }

    // =========================================================================
    // MARK: - Large Data
    // =========================================================================

    @Test func largeValue() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: [UInt8] = [0x01]
        let value: [UInt8] = Array(repeating: 0xAB, count: 100_000)

        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }

        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key)
        }
        #expect(result == value)
    }

    @Test func manyKeys() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let count = 100
        // Use a dedicated prefix (0x50) to avoid collisions with other test suites
        let prefix: UInt8 = 0x50
        try await engine.withTransaction { tx in
            for i in 0..<count {
                var key: Bytes = [prefix]
                key.append(contentsOf: withUnsafeBytes(of: UInt32(i).bigEndian) { Array($0) })
                tx.setValue([UInt8(i % 256)], for: key)
            }
        }

        let range = try await engine.withTransaction { tx in
            try await collectRange(tx, begin: [prefix, 0x00], end: [prefix + 1])
        }
        #expect(range.count == count)
    }
}
} // extension AllPostgreSQLTests
