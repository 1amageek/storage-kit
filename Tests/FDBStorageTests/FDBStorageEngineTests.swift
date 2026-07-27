import DatabaseTypes
import Testing
import Foundation
@testable import StorageKit
@testable import FDBStorage
import FoundationDB

/// Tests require a running FoundationDB instance:
/// ```
/// sudo launchctl load /Library/LaunchDaemons/com.apple.foundationdb.fdbmonitor.plist
/// ```
@Suite("FDBStorageEngine Tests", .serialized)
struct FDBStorageEngineTests {

    private func makeEngine() async throws -> FDBStorageEngine {
        try await FDBStorageEngine(configuration: .init())
    }

    private func testPrefix() -> ByteString {
        let uuid = Foundation.UUID().uuidString.prefix(8)
        return ByteString(Array("_test_\(uuid)_".utf8))
    }

    private func prefixedKey(_ prefix: ByteString, _ suffix: [UInt8]) -> ByteString {
        ByteString(prefix.copyBytes() + suffix)
    }

    private func cleanup(engine: FDBStorageEngine, prefix: ByteString) async throws {
        try await engine.withTransaction { tx in
            try tx.clearRange(
                beginKey: prefix,
                endKey: prefixedKey(prefix, [0xFF, 0xFF])
            )
        }
    }

    private func collectRange(
        _ tx: some TransactionAccess,
        begin: ByteString, end: ByteString
    ) async throws -> [(key: ByteString, value: ByteString)] {
        let seq = tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq { result.append((key: key, value: value)) }
        return result
    }

    @Test func fdbResultsRemainBorrowedUntilExplicitDetach() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])
        let expectedValue = ByteString([UInt8](repeating: 0xA5, count: 4_096))

        try await engine.withTransaction { transaction in
            try transaction.setValue(expectedValue, for: key)
        }

        try await engine.withTransaction { transaction in
            let value = try #require(
                await transaction.getValue(for: key, snapshot: true)
            )
            let detachedValue = value.detached()
            let valueAddress = try #require(value.withUnsafeBytes {
                $0.baseAddress.map { UInt(bitPattern: $0) }
            })
            let detachedValueAddress = try #require(
                detachedValue.withUnsafeBytes {
                    $0.baseAddress.map { UInt(bitPattern: $0) }
                }
            )
            #expect(value == detachedValue)
            #expect(valueAddress != detachedValueAddress)

            var cursor = transaction.rangeCursor(
                from: .firstGreaterOrEqual(key),
                to: .firstGreaterThan(key),
                limit: 1,
                snapshot: true,
                streamingMode: .exact
            )
            let row = try #require(await cursor.next())
            try await cursor.finish()

            let detachedKey = row.0.detached()
            let rangeValue = row.1.detached()
            let keyAddress = try #require(row.0.withUnsafeBytes {
                $0.baseAddress.map { UInt(bitPattern: $0) }
            })
            let detachedKeyAddress = try #require(
                detachedKey.withUnsafeBytes {
                    $0.baseAddress.map { UInt(bitPattern: $0) }
                }
            )
            let rowValueAddress = try #require(row.1.withUnsafeBytes {
                $0.baseAddress.map { UInt(bitPattern: $0) }
            })
            let rangeValueAddress = try #require(
                rangeValue.withUnsafeBytes {
                    $0.baseAddress.map { UInt(bitPattern: $0) }
                }
            )
            #expect(row.0 == detachedKey)
            #expect(row.1 == rangeValue)
            #expect(keyAddress != detachedKeyAddress)
            #expect(rowValueAddress != rangeValueAddress)
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func taskCancellationRemainsCancellationErrorAcrossStorageBoundary() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()

        let task = Task {
            while !Task.isCancelled {
                await Task.yield()
            }
            return try await transaction.getValue(
                for: [0x01],
                snapshot: true
            )
        }
        task.cancel()

        do {
            _ = try await task.value
            Issue.record("Expected CancellationError")
        } catch is CancellationError {
            // Expected.
        } catch {
            Issue.record("Expected CancellationError, got \(error)")
        }
        try await transaction.cancel()
    }

    // =========================================================================
    // MARK: - Reverse + Limit Ordering
    //
    // FDB adapter collects all results forward via AsyncKVSequence,
    // then reverses in memory, then applies limit via prefix().
    // This ordering is critical: limit AFTER reverse.
    // =========================================================================

    @Test func reverse_resultsInDescendingOrder() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                try tx.setValue([i * 10], for: prefixedKey(prefix, [i]))
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.collectRange(
                begin: prefixedKey(prefix, [0x01]),
                end: prefixedKey(prefix, [0x06]),
                reverse: true
            )
            let values = results.map { $0.1 }
            #expect(values == [[50], [40], [30], [20], [10]])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func reverseThenLimit_takesLastNItems() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                try tx.setValue([i * 10], for: prefixedKey(prefix, [i]))
            }
        }

        try await engine.withTransaction { tx in
            let collected = try await tx.collectRange(
                begin: prefixedKey(prefix, [0x01]),
                end: prefixedKey(prefix, [0x06]),
                limit: 2,
                reverse: true
            )
            // limit=2 applied AFTER reverse: [5]=50, [4]=40
            #expect(collected.count == 2)
            #expect(collected[0].1 == [50])
            #expect(collected[1].1 == [40])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func forwardLimit_takesFirstNItems() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt8 in 1...5 {
                try tx.setValue([i * 10], for: prefixedKey(prefix, [i]))
            }
        }

        try await engine.withTransaction { tx in
            let collected = try await tx.collectRange(
                begin: prefixedKey(prefix, [0x01]),
                end: prefixedKey(prefix, [0x06]),
                limit: 2
            )
            // limit=2 forward: [1]=10, [2]=20
            #expect(collected.count == 2)
            #expect(collected[0].1 == [10])
            #expect(collected[1].1 == [20])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - Multi-Batch Collection
    //
    // FDB returns results in batches. AsyncKVSequence handles pagination.
    // Verify all batches are collected correctly.
    // =========================================================================

    @Test func largeScan_collectsAllBatches() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt16 in 0..<500 {
                let key = prefixedKey(
                    prefix,
                    withUnsafeBytes(of: i.bigEndian) { Array($0) }
                )
                try tx.setValue(
                    withUnsafeBytes(of: i) { ByteString(Array($0)) },
                    for: key
                )
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.collectRange(
                begin: prefix,
                end: prefixedKey(prefix, [0xFF, 0xFF])
            )
            var prevKey: ByteString?
            for (key, _) in results {
                if let prev = prevKey {
                    #expect(prev.lexicographicallyPrecedes(key))
                }
                prevKey = key
            }
            #expect(results.count == 500)
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func largeScan_reverseCollectsAllBatches() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt16 in 0..<500 {
                let key = prefixedKey(
                    prefix,
                    withUnsafeBytes(of: i.bigEndian) { Array($0) }
                )
                try tx.setValue(
                    withUnsafeBytes(of: i) { ByteString(Array($0)) },
                    for: key
                )
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.collectRange(
                begin: prefix,
                end: prefixedKey(prefix, [0xFF, 0xFF]),
                reverse: true
            )
            var prevKey: ByteString?
            for (key, _) in results {
                if let prev = prevKey {
                    // Descending order
                    #expect(key.lexicographicallyPrecedes(prev))
                }
                prevKey = key
            }
            #expect(results.count == 500)
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - Commit Bool → StorageError Mapping
    //
    // FDB commit() returns Bool. false → StorageError.transactionConflict.
    // We can't easily trigger a real conflict in a unit test, so we verify
    // the commit path works correctly for the success case.
    // =========================================================================

    @Test func commit_persistsData() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        let tx1 = try engine.createTransaction()
        try tx1.setValue([42], for: key)
        try await tx1.commit()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: key)
        #expect(value == [42])
        try await tx2.cancel()

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func cancel_discardsData() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        let tx1 = try engine.createTransaction()
        try tx1.setValue([42], for: key)
        try await tx1.cancel()

        let tx2 = try engine.createTransaction()
        let cancelledValue = try await tx2.getValue(for: key)
        #expect(cancelledValue == nil)
        try await tx2.cancel()
    }

    // =========================================================================
    // MARK: - FDB Read-Your-Writes (native)
    //
    // Unlike SQLite/InMemory, FDB has native read-your-writes.
    // Writes are immediately visible within the same transaction.
    // =========================================================================

    @Test func readYourWrites_setValue() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        try await engine.withTransaction { tx in
            try tx.setValue([42], for: key)
            let value = try await tx.getValue(for: key)
            #expect(value == [42])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func readYourWrites_clearThenGet() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        try await engine.withTransaction { tx in
            try tx.setValue([42], for: key)
        }

        try await engine.withTransaction { tx in
            try tx.clear(key: key)
            let value = try await tx.getValue(for: key)
            #expect(value == nil)
        }
    }

    @Test func readYourWrites_setInRangeScan() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: prefixedKey(prefix, [0x01]))
            try tx.setValue([30], for: prefixedKey(prefix, [0x03]))
        }

        try await engine.withTransaction { tx in
            try tx.setValue([20], for: prefixedKey(prefix, [0x02]))
            let range = try await collectRange(
                tx, begin: prefix, end: prefixedKey(prefix, [0xFF])
            )
            // New key [0x02] should appear in range scan
            #expect(range.count == 3)
            #expect(range[1].value == [20])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - Range Boundary Semantics (begin inclusive, end exclusive)
    // =========================================================================

    @Test func range_beginInclusive_endExclusive() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: prefixedKey(prefix, [0x01]))
            try tx.setValue([2], for: prefixedKey(prefix, [0x02]))
            try tx.setValue([3], for: prefixedKey(prefix, [0x03]))
            try tx.setValue([4], for: prefixedKey(prefix, [0x04]))
            try tx.setValue([5], for: prefixedKey(prefix, [0x05]))
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(
                tx,
                begin: prefixedKey(prefix, [0x02]),
                end: prefixedKey(prefix, [0x05])
            )
            #expect(range.count == 3)
            #expect(range[0].value == [2])  // begin included
            #expect(range[2].value == [4])  // end excluded
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func range_empty() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: prefixedKey(prefix, [0x05]))
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(
                tx,
                begin: prefixedKey(prefix, [0x01]),
                end: prefixedKey(prefix, [0x03])
            )
            #expect(range.count == 0)
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - withTransaction Error Handling
    //
    // FDBStorageEngine.withTransaction wraps non-FDBError/non-StorageError
    // in StorageError.backendError. Verify rollback on error.
    // =========================================================================

    @Test func withTransaction_autoCommits() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        try await engine.withTransaction { tx in
            try tx.setValue([99], for: key)
        }

        try await engine.withTransaction { tx in
            let committedValue = try await tx.getValue(for: key)
            #expect(committedValue == [99])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        struct TransactionBodyFailure: Error {}

        do {
            try await engine.withTransaction { tx in
                try tx.setValue([42], for: key)
                throw TransactionBodyFailure()
            }
        } catch {
            // FDB wraps TransactionBodyFailure → StorageError.backendError
        }

        try await engine.withTransaction { tx in
            let rolledBackValue = try await tx.getValue(for: key)
            #expect(rolledBackValue == nil)
        }
    }

    // =========================================================================
    // MARK: - clearRange Semantics
    // =========================================================================

    @Test func clearRange_boundaryVerification() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: prefixedKey(prefix, [0x01]))
            try tx.setValue([2], for: prefixedKey(prefix, [0x02]))
            try tx.setValue([3], for: prefixedKey(prefix, [0x03]))
            try tx.setValue([4], for: prefixedKey(prefix, [0x04]))
            try tx.setValue([5], for: prefixedKey(prefix, [0x05]))
        }

        try await engine.withTransaction { tx in
            try tx.clearRange(
                beginKey: prefixedKey(prefix, [0x02]),
                endKey: prefixedKey(prefix, [0x05])
            )
        }

        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: prefixedKey(prefix, [0x01]))
            let v2 = try await tx.getValue(for: prefixedKey(prefix, [0x02]))
            let v3 = try await tx.getValue(for: prefixedKey(prefix, [0x03]))
            let v4 = try await tx.getValue(for: prefixedKey(prefix, [0x04]))
            let v5 = try await tx.getValue(for: prefixedKey(prefix, [0x05]))
            #expect(v1 == [1])   // before range
            #expect(v2 == nil)   // begin inclusive
            #expect(v3 == nil)   // within range
            #expect(v4 == nil)   // within range
            #expect(v5 == [5])   // end exclusive
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - Ordering
    // =========================================================================

    @Test func keyOrdering_preserved() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            // Insert in non-sorted order
            try tx.setValue([5], for: prefixedKey(prefix, [0x05]))
            try tx.setValue([1], for: prefixedKey(prefix, [0x01]))
            try tx.setValue([3], for: prefixedKey(prefix, [0x03]))
            try tx.setValue([2], for: prefixedKey(prefix, [0x02]))
        }

        try await engine.withTransaction { tx in
            let range = try await collectRange(
                tx, begin: prefix, end: prefixedKey(prefix, [0xFF])
            )
            let values = range.map { $0.value }
            #expect(values == [[1], [2], [3], [5]])
        }

        try await cleanup(engine: engine, prefix: prefix)
    }

    // =========================================================================
    // MARK: - Subspace Isolation
    // =========================================================================

    @Test func subspaceRangeIsolation() async throws {
        let engine = try await makeEngine()
        let prefix = testPrefix()
        let spaceA = Subspace(prefix: prefixedKey(prefix, Array("alpha".utf8)))
        let spaceB = Subspace(prefix: prefixedKey(prefix, Array("beta".utf8)))

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

        try await cleanup(engine: engine, prefix: prefix)
    }
}
