import Testing
import Foundation
@testable import StorageKit

@Suite("Transaction Default Implementations Tests")
struct TransactionDefaultsTests {

    // =========================================================================
    // MARK: - Version Management Defaults
    //
    // Non-FDB backends use default implementations that return 0 or no-op.
    // =========================================================================

    @Test func setReadVersion_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        // Should not throw
        tx.setReadVersion(42)
        try await tx.commit()
    }

    @Test func getReadVersion_returnsZero() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        let version = try await tx.getReadVersion()
        #expect(version == 0)
        try await tx.commit()
    }

    @Test func getCommittedVersion_returnsZero() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try await tx.commit()
        let version = try tx.getCommittedVersion()
        #expect(version == 0)
    }

    // =========================================================================
    // MARK: - Transaction Options Defaults
    //
    // Non-FDB backends silently accept all options (no-op).
    // =========================================================================

    @Test func setOption_noValue_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setOption(forOption: .timeout(milliseconds: 5000))
        try tx.setOption(forOption: .priorityBatch)
        try tx.setOption(forOption: .prioritySystemImmediate)
        try tx.setOption(forOption: .readPriorityLow)
        try tx.setOption(forOption: .readPriorityHigh)
        try tx.setOption(forOption: .accessSystemKeys)
        try tx.setOption(forOption: .readServerSideCacheDisable)
        try await tx.commit()
    }

    @Test func setOption_intValue_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setOption(to: 5000, forOption: .timeout(milliseconds: 5000))
        try await tx.commit()
    }

    @Test func setOption_bytesValue_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setOption(to: [0x01, 0x02] as Bytes?, forOption: .accessSystemKeys)
        try await tx.commit()
    }

    @Test func setOption_stringValue_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setOption(to: "test-value", forOption: .accessSystemKeys)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - Atomic Operations (InMemory wiring)
    //
    // InMemoryTransaction routes atomicOp through MutationType.apply in all
    // three code paths: getValue (read-your-writes replay), getRange (replay),
    // and commit (staged mutation). The arithmetic itself is covered
    // exhaustively by MutationTypeApplyTests; these tests confirm the wiring.
    // =========================================================================

    @Test func atomicAdd_visibleViaReadYourWrites() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([10], for: [0x01])
        tx.atomicOp(key: [0x01], param: [5], mutationType: .add)

        // getValue replays the buffer, so the addition is visible pre-commit.
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [15])
        try await tx.commit()
    }

    @Test func atomicAdd_onMissingKeyTreatsExistingAsZero() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        // No prior setValue: existing is nil, treated as zero.
        tx.atomicOp(key: [0x01], param: [7], mutationType: .add)
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [7])
        try await tx.commit()
    }

    @Test func atomicAdd_persistsAfterCommit() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }
        try await engine.withTransaction { tx in
            tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
        }

        // A fresh transaction sees the committed, mutated value.
        let tx = try engine.createTransaction()
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [15])
        try await tx.commit()
    }

    @Test func atomicAdd_visibleViaGetRange() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([10], for: [0x01])
        tx.atomicOp(key: [0x01], param: [5], mutationType: .add)

        // getRange replays the buffer, so the addition is visible in results.
        let results = try await tx.collectRange(begin: [0x01], end: [0x02])
        #expect(results.count == 1)
        #expect(results[0].1 == [15])
        try await tx.commit()
    }

    @Test func atomicCompareAndClear_removesMatchingKey() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        tx.atomicOp(key: [0x01], param: [42], mutationType: .compareAndClear)

        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func atomicVersionstamp_throwsOnCommit() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.atomicOp(key: [0x01], param: [0x01], mutationType: .setVersionstampedValue)

        // Versionstamp mutations cannot be computed outside FDB; commit throws
        // and the store is left untouched (staged-then-swap guarantees this).
        await #expect(throws: StorageError.self) {
            try await tx.commit()
        }
        #expect(engine.count == 0)
    }

    // =========================================================================
    // MARK: - Conflict Range Default
    // =========================================================================

    @Test func addConflictRange_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        // Should not throw
        try tx.addConflictRange(beginKey: [0x00], endKey: [0xFF], type: .read)
        try tx.addConflictRange(beginKey: [0x00], endKey: [0xFF], type: .write)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - Statistics Defaults
    // =========================================================================

    @Test func getEstimatedRangeSizeBytes_returnsZero() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        let size = try await tx.getEstimatedRangeSizeBytes(beginKey: [0x00], endKey: [0xFF])
        #expect(size == 0)
        try await tx.commit()
    }

    @Test func getRangeSplitPoints_returnsEmpty() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        let points = try await tx.getRangeSplitPoints(beginKey: [0x00], endKey: [0xFF], chunkSize: 100)
        #expect(points.isEmpty)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - Versionstamp Default
    // =========================================================================

    @Test func getVersionstamp_returnsNil() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try await tx.commit()
        let stamp = try await tx.getVersionstamp()
        #expect(stamp == nil)
    }

    // =========================================================================
    // MARK: - getKey Default Implementation
    //
    // getKey uses getRange internally with limit=1.
    // =========================================================================

    @Test func getKey_firstGreaterOrEqual_findsExactMatch() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        let key = try await tx.getKey(selector: .firstGreaterOrEqual([0x02]))
        #expect(key == [0x02])
        try await tx.commit()
    }

    @Test func getKey_firstGreaterOrEqual_findsNextKey() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        // [0x02] doesn't exist, so firstGreaterOrEqual finds [0x03]
        let key = try await tx.getKey(selector: .firstGreaterOrEqual([0x02]))
        #expect(key == [0x03])
        try await tx.commit()
    }

    @Test func getKey_firstGreaterThan_skipsExactMatch() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        let key = try await tx.getKey(selector: .firstGreaterThan([0x01]))
        #expect(key == [0x02])
        try await tx.commit()
    }

    @Test func getKey_noMatch_returnsNil() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        // No key >= [0x02]
        let key = try await tx.getKey(selector: .firstGreaterOrEqual([0x02]))
        #expect(key == nil)
        try await tx.commit()
    }

    @Test func getKey_emptyStore_returnsNil() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        let key = try await tx.getKey(selector: .firstGreaterOrEqual([0x00]))
        #expect(key == nil)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - Convenience getRange Overloads
    // =========================================================================

    @Test func getRange_bytesOverload() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        let results = try await tx.collectRange(begin: [0x01], end: [0x03])
        #expect(results.count == 2)
        #expect(results[0].0 == [0x01])
        #expect(results[1].0 == [0x02])
        try await tx.commit()
    }

    @Test func getRange_legacyBeginEndSelector() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        let seq = tx.getRange(
            beginSelector: .firstGreaterOrEqual([0x01]),
            endSelector: .firstGreaterOrEqual([0x03])
        )
        var results: [(Bytes, Bytes)] = []
        for try await pair in seq { results.append(pair) }
        #expect(results.count == 2)
        try await tx.commit()
    }

    @Test func getRange_legacyBeginEndKey() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        let seq = tx.getRange(beginKey: [0x01], endKey: [0x03])
        var results: [(Bytes, Bytes)] = []
        for try await pair in seq { results.append(pair) }
        #expect(results.count == 2)
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - forEachInRange
    // =========================================================================

    @Test func forEachInRange_iteratesAllPairs() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        var keys: [Bytes] = []
        try await tx.forEachInRange(
            from: .firstGreaterOrEqual([0x01]),
            to: .firstGreaterOrEqual([0x04])
        ) { key, _ in
            keys.append(key)
        }
        #expect(keys == [[0x01], [0x02], [0x03]])
        try await tx.commit()
    }

    // =========================================================================
    // MARK: - InMemoryEngine shutdown
    // =========================================================================

    @Test func shutdown_isNoOp() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { tx in
            tx.setValue([42], for: [0x01])
        }
        // Default shutdown is no-op; should not throw or affect state
        engine.shutdown()
        #expect(engine.count == 1)
    }

    // =========================================================================
    // MARK: - Double Commit on InMemory
    // =========================================================================

    @Test func doubleCommit_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        try await tx.commit()
        // Second commit should return early
        try await tx.commit()
        #expect(engine.count == 1)
    }

    @Test func cancelAfterCommit_isNoOp() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        tx.setValue([42], for: [0x01])
        try await tx.commit()
        tx.cancel()
        #expect(engine.count == 1)
    }
}
