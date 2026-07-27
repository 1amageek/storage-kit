import DatabaseTypes
import Testing
import Foundation
@testable import StorageKit

@Suite("InMemoryEngine Tests")
struct InMemoryEngineTests {

    // =========================================================================
    // MARK: - Ordered Write Buffer Replay
    //
    // getValue replays operations in insertion order so atomic mutations can
    // consume the value produced by earlier writes.
    // =========================================================================

    @Test func setThenClear_clearWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
    }

    @Test func setThenClearThenSet_lastSetWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        try tx.setValue([2], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])
    }

    @Test func setThenClearRange_clearRangeWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func clearRangeThenSet_setWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        try tx.setValue([99], for: [0x03])
        let value = try await tx.getValue(for: [0x03])
        #expect(value == [99])
    }

    @Test func multipleOverwrites_lastWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.setValue([2], for: [0x01])
        try tx.setValue([3], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [3])
    }

    @Test func setClearRangeSetClearRange_lastClearRangeWins() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        try tx.setValue([2], for: [0x02])
        try tx.clearRange(beginKey: [0x01], endKey: [0x05])
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func bufferOverridesSnapshot() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        // Snapshot has [0x01]=10. Buffer overwrites it.
        try tx.setValue([20], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [20])
    }

    @Test func clearInBufferHidesSnapshotValue() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        try tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
    }

    @Test func clearRangeInBufferHidesSnapshotValues() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
            try tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x01], endKey: [0x03])
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
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        // Buffer has an operation on a DIFFERENT key
        try tx.setValue([99], for: [0xFF])
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
        try tx.setValue([1], for: [0x02])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        // key == begin → inside range → nil
        let value = try await tx.getValue(for: [0x02])
        #expect(value == nil)
    }

    @Test func clearRange_endExclusive() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x05])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        // key == end → outside range → value preserved
        let value = try await tx.getValue(for: [0x05])
        #expect(value == [1])
    }

    @Test func clearRange_justBeforeEnd() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x04])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        // key < end → inside range → nil
        let value = try await tx.getValue(for: [0x04])
        #expect(value == nil)
    }

    @Test func clearRange_justBeforeBegin() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        // key < begin → outside range → value preserved
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [1])
    }

    @Test func clearRange_justAfterEnd() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x06])
        try tx.clearRange(beginKey: [0x02], endKey: [0x05])
        // key > end → outside range → value preserved
        let value = try await tx.getValue(for: [0x06])
        #expect(value == [1])
    }

    @Test func clearRange_multiByteKeyBoundary() async throws {
        let engine = InMemoryEngine()
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
    }

    // =========================================================================
    // MARK: - getValue / getRange Consistency
    //
    // Both getValue and getRange observe the same logical state.
    // InMemory: getRange builds an "effective store" by applying buffer ops
    //   in forward order. getValue scans buffer in reverse. Both should agree.
    // =========================================================================

    private func collectRange(
        _ tx: some TransactionAccess,
        begin: ByteString, end: ByteString
    ) async throws -> [(key: ByteString, value: ByteString)] {
        let seq = tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq { result.append((key: key, value: value)) }
        return result
    }

    @Test func consistency_setClearSet() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])
        try tx.clear(key: [0x01])
        try tx.setValue([2], for: [0x01])

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
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
            try tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x01], endKey: [0x04])
        try tx.setValue([99], for: [0x02])

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
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([99], for: [0x01])    // overwrite 0x01
        try tx.clearRange(beginKey: [0x01], endKey: [0x03])  // then clear entire range

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
            try tx.setValue([10], for: [0x01])
            try tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([20], for: [0x02])  // new key in buffer

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
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
            try tx.setValue([30], for: [0x03])
        }

        let tx = try engine.createTransaction()
        try tx.clear(key: [0x02])

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
                try tx.setValue([i * 10], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let collected = try await tx.collectRange(
                begin: [0x01], end: [0x06], limit: 2, reverse: true
            )
            // Should be the last 2 items: [5]=50, [4]=40
            #expect(collected.count == 2)
            #expect(collected[0].0 == [0x05])
            #expect(collected[0].1 == [50])
            #expect(collected[1].0 == [0x04])
            #expect(collected[1].1 == [40])
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
            try tx.setValue([10], for: [0x01])
        }

        // tx1 takes snapshot with [0x01]=10
        let tx1 = try engine.createTransaction()

        // tx2 overwrites [0x01] and commits
        try await engine.withTransaction { tx in
            try tx.setValue([20], for: [0x01])
        }

        // tx1 should still see [0x01]=10 from its snapshot
        let value = try await tx1.getValue(for: [0x01])
        #expect(value == [10])
    }

    // =========================================================================
    // MARK: - Optimistic Concurrency
    // =========================================================================

    @Test func concurrentWritesToSameKey_allowExactlyOneCommit() async throws {
        let engine = InMemoryEngine()
        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        try first.setValue([1], for: [0x10])
        try second.setValue([2], for: [0x10])

        let outcomes = await commitConcurrently(first, second)

        #expect(outcomes.filter { $0 == .success }.count == 1)
        #expect(
            outcomes.filter { $0 == .storageFailure(.transactionConflict) }.count == 1
        )
        let stored = try await readValue(engine, key: [0x10])
        #expect(stored == [1] || stored == [2])
    }

    @Test func concurrentDisjointBlindWrites_bothCommit() async throws {
        let engine = InMemoryEngine()
        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        try first.setValue([1], for: [0x10])
        try second.setValue([2], for: [0x20])

        let outcomes = await commitConcurrently(first, second)

        #expect(outcomes.filter { $0 == .success }.count == 2)
        #expect(try await readValue(engine, key: [0x10]) == [1])
        #expect(try await readValue(engine, key: [0x20]) == [2])
    }

    @Test func serializableAndSnapshotPointReads_haveDifferentConflicts() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x10])
        }

        let serializableReader = try engine.createTransaction()
        let snapshotReader = try engine.createTransaction()
        #expect(
            try await serializableReader.getValue(
                for: [0x10],
                snapshot: false
            ) == [1]
        )
        #expect(
            try await snapshotReader.getValue(
                for: [0x10],
                snapshot: true
            ) == [1]
        )

        try await engine.withTransaction { transaction in
            try transaction.setValue([2], for: [0x10])
        }

        #expect(
            await Self.commitOutcome(serializableReader)
                == .storageFailure(.transactionConflict)
        )
        #expect(await Self.commitOutcome(snapshotReader) == .success)
    }

    @Test func serializableEmptyRange_detectsPhantomAndRollsBackWrites() async throws {
        let engine = InMemoryEngine()
        let reader = try engine.createTransaction()
        let rows = try await reader.collectRange(
            begin: [0x10],
            end: [0x20],
            snapshot: false
        )
        #expect(rows.isEmpty)
        try reader.setValue([9], for: [0x90])

        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x15])
        }

        #expect(
            await Self.commitOutcome(reader)
                == .storageFailure(.transactionConflict)
        )
        #expect(try await readValue(engine, key: [0x15]) == [1])
        #expect(try await readValue(engine, key: [0x90]) == nil)
    }

    @Test func normalRange_doesNotConflictWithDisjointWrite() async throws {
        let engine = InMemoryEngine()
        let reader = try engine.createTransaction()
        let rows = try await reader.collectRange(
            begin: [0x10],
            end: [0x20],
            snapshot: false
        )
        #expect(rows.isEmpty)
        try reader.setValue([9], for: [0x90])

        try await engine.withTransaction { transaction in
            try transaction.setValue([3], for: [0x30])
        }

        try await reader.commit()
        #expect(try await readValue(engine, key: [0x30]) == [3])
        #expect(try await readValue(engine, key: [0x90]) == [9])
    }

    @Test func strictSelectorBoundary_doesNotConflictWithExcludedKey() async throws {
        let engine = InMemoryEngine()
        let reader = try engine.createTransaction()
        let rows = try await reader.collectRange(
            from: .firstGreaterThan([0x10]),
            to: .firstGreaterOrEqual([0x20]),
            snapshot: false
        )
        #expect(rows.isEmpty)

        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x10])
        }

        try await reader.commit()
    }

    @Test func complexSelector_usesConservativeConflictRange() async throws {
        let engine = InMemoryEngine()
        let reader = try engine.createTransaction()
        let rows = try await reader.collectRange(
            from: KeySelector(key: [0x10], orEqual: false, offset: 2),
            to: KeySelector(key: [0x20], orEqual: false, offset: 3),
            snapshot: false
        )
        #expect(rows.isEmpty)

        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0xF0])
        }

        #expect(
            await Self.commitOutcome(reader)
                == .storageFailure(.transactionConflict)
        )
    }

    @Test func concurrentAtomicOperationsToSameKey_allowExactlyOneCommit() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0], for: [0x10])
        }
        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        try first.atomicOp(key: [0x10], param: [1], mutationType: .add)
        try second.atomicOp(key: [0x10], param: [1], mutationType: .add)

        let outcomes = await commitConcurrently(first, second)

        #expect(outcomes.filter { $0 == .success }.count == 1)
        #expect(
            outcomes.filter { $0 == .storageFailure(.transactionConflict) }.count == 1
        )
        #expect(try await readValue(engine, key: [0x10]) == [1])
    }

    @Test func concurrentClearRangeAndPointWrite_overlapConflicts() async throws {
        let engine = InMemoryEngine()
        let rangeWriter = try engine.createTransaction()
        let pointWriter = try engine.createTransaction()
        try rangeWriter.clearRange(beginKey: [0x10], endKey: [0x20])
        try pointWriter.setValue([1], for: [0x15])

        let outcomes = await commitConcurrently(rangeWriter, pointWriter)

        #expect(outcomes.filter { $0 == .success }.count == 1)
        #expect(
            outcomes.filter { $0 == .storageFailure(.transactionConflict) }.count == 1
        )
    }

    @Test func clearRangeEndBoundaryAndPointWrite_areDisjoint() async throws {
        let engine = InMemoryEngine()
        let rangeWriter = try engine.createTransaction()
        let pointWriter = try engine.createTransaction()
        try rangeWriter.clearRange(beginKey: [0x10], endKey: [0x20])
        try pointWriter.setValue([1], for: [0x20])

        let outcomes = await commitConcurrently(rangeWriter, pointWriter)

        #expect(outcomes.filter { $0 == .success }.count == 2)
        #expect(try await readValue(engine, key: [0x20]) == [1])
    }

    @Test func explicitReadConflictRange_detectsConcurrentWrite() async throws {
        let engine = InMemoryEngine()
        let protected = try engine.createTransaction()
        try protected.addConflictRange(
            beginKey: [0x10],
            endKey: [0x20],
            type: .read
        )
        try protected.setValue([9], for: [0x90])

        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x15])
        }

        #expect(
            await Self.commitOutcome(protected)
                == .storageFailure(.transactionConflict)
        )
        #expect(try await readValue(engine, key: [0x90]) == nil)
    }

    @Test func terminalTransactions_releaseRegistrationsAndPruneHistory() async throws {
        let engine = InMemoryEngine()
        let oldestTransaction = try engine.createTransaction()

        for key: UInt8 in 1...3 {
            try await engine.withTransaction { transaction in
                try transaction.setValue([key], for: [key])
            }
        }

        let retainedState = engine._store.withLock {
            (
                activeTransactions: $0.activeTransactionCount,
                retainedVersions: $0.retainedConflictVersionCount
            )
        }
        #expect(retainedState.activeTransactions == 1)
        #expect(retainedState.retainedVersions == 3)

        try await oldestTransaction.cancel()

        let releasedState = engine._store.withLock {
            (
                activeTransactions: $0.activeTransactionCount,
                retainedVersions: $0.retainedConflictVersionCount
            )
        }
        #expect(releasedState.activeTransactions == 0)
        #expect(releasedState.retainedVersions == 0)
    }

    @Test func mutationByteLimit_rejectsBeforeBufferAndConflictAdmission() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([2], for: [0x20])
            try transaction.setValue([3], for: [0x30])
        }

        let setTransaction = try engine.createTransaction()
        try setTransaction.configureMutationByteLimit(maximumBytes: 18)
        #expect(throws: TransactionMutationByteLimitError.self) {
            try setTransaction.setValue([1], for: [0x10])
        }
        try await engine.withTransaction { competingTransaction in
            try competingTransaction.setValue([7], for: [0x10])
        }
        try await setTransaction.commit()

        let clearTransaction = try engine.createTransaction()
        try await expectRejectedMutation(
            maximumBytes: 9,
            transaction: clearTransaction
        ) {
            try clearTransaction.clear(key: [0x20])
        }

        let clearRangeTransaction = try engine.createTransaction()
        try await expectRejectedMutation(
            maximumBytes: 18,
            transaction: clearRangeTransaction
        ) {
            try clearRangeTransaction.clearRange(
                beginKey: [0x20],
                endKey: [0x40]
            )
        }

        let atomicTransaction = try engine.createTransaction()
        try await expectRejectedMutation(
            maximumBytes: 22,
            transaction: atomicTransaction
        ) {
            try atomicTransaction.atomicOp(
                key: [0x30],
                param: [1],
                mutationType: .add
            )
        }

        #expect(try await readValue(engine, key: [0x10]) == [7])
        #expect(try await readValue(engine, key: [0x20]) == [2])
        #expect(try await readValue(engine, key: [0x30]) == [3])
    }

    @Test func failedCommit_neverPublishesPartiallyAppliedWrites() async throws {
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        try transaction.setValue([1], for: [0x10])
        try transaction.atomicOp(
            key: [0x20],
            param: [0],
            mutationType: .setVersionstampedValue
        )

        let outcome = await Self.commitOutcome(transaction)

        guard case .storageFailure(let code) = outcome else {
            Issue.record("Expected a storage failure, got \(outcome)")
            return
        }
        #expect(code == .invalidOperation)
        #expect(try await readValue(engine, key: [0x10]) == nil)
        #expect(try await readValue(engine, key: [0x20]) == nil)
    }

    @Test func conflictRetry_reexecutesAndPublishesOneLogicalEffect() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0], for: [0x10])
        }

        let attempts = try await runWithConflictRetry(
            engine: engine,
            maximumAttempts: 3
        ) { transaction, attempt in
            guard let value = try await transaction.getValue(for: [0x10]),
                  let current = value.first else {
                throw InMemoryCommitObservationError.missingCounter
            }
            if attempt == 1 {
                try await engine.withTransaction { competingTransaction in
                    try competingTransaction.setValue([10], for: [0x10])
                }
            }
            try transaction.setValue([current + 1], for: [0x10])
        }

        #expect(attempts == 2)
        #expect(try await readValue(engine, key: [0x10]) == [11])
    }

    // =========================================================================
    // MARK: - Transaction Lifecycle
    // =========================================================================

    @Test func commitAppliesBufferToStore() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([42], for: [0x01])
        try await tx.commit()
        #expect(engine.count == 1)
    }

    @Test func cancelDiscards() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([42], for: [0x01])
        try await tx.cancel()
        #expect(engine.count == 0)
    }

    @Test func cancelledTransactionThrowsOnGetValue() async throws {
        let engine = InMemoryEngine()
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
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try await tx.cancel()
        do {
            let seq = tx.getRange(begin: [0x00], end: [0xFF], limit: 0, reverse: false)
            for try await _ in seq {
                Issue.record("Expected error")
            }
            Issue.record("Expected error")
        } catch let error {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func cancelledTransactionThrowsOnCommit() async throws {
        let engine = InMemoryEngine()
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

    @Test func writesAfterCancelReportTerminalState() async throws {
        let engine = InMemoryEngine()
        let tx = try engine.createTransaction()
        try await tx.cancel()

        for mutation in [
            { try tx.setValue([42], for: [0x01]) },
            { try tx.clear(key: [0x02]) },
            { try tx.clearRange(beginKey: [0x03], endKey: [0x04]) },
            { try tx.atomicOp(key: [0x05], param: [1], mutationType: .add) },
        ] {
            do {
                try mutation()
                Issue.record("Expected invalidOperation")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }
        }

        #expect(engine.count == 0)
    }

    @Test func withTransaction_errorCausesRollback() async throws {
        let engine = InMemoryEngine()

        struct TransactionBodyFailure: Error {}

        do {
            try await engine.withTransaction { tx in
                try tx.setValue([42], for: [0x01])
                throw TransactionBodyFailure()
            }
        } catch is TransactionBodyFailure {}

        #expect(engine.count == 0)
    }

    @Test func withTransaction_autoCommits() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([42], for: [0x01])
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
                let key = withUnsafeBytes(of: i.bigEndian) { ByteString(Array($0)) }
                try tx.setValue(key, for: key)
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.collectRange(
                begin: [0x00, 0x00], end: [0xFF, 0xFF]
            )
            var prevKey: ByteString?
            for (key, _) in results {
                if let prev = prevKey {
                    // Verify ascending order via compareBytes
                    #expect(prev.lexicographicallyPrecedes(key))
                }
                prevKey = key
            }
            #expect(results.count == 500)
        }
    }

    @Test func concurrentTransactions() async throws {
        let engine = InMemoryEngine()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i: UInt8 in 0..<10 {
                group.addTask {
                    try await engine.withTransaction { tx in
                        try tx.setValue([i], for: [i])
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

    private func readValue(
        _ engine: InMemoryEngine,
        key: ByteString
    ) async throws -> ByteString? {
        let transaction = try engine.createTransaction()
        let value = try await transaction.getValue(for: key, snapshot: true)
        try await transaction.commit()
        return value
    }

    private func commitConcurrently(
        _ first: InMemoryTransaction,
        _ second: InMemoryTransaction
    ) async -> [InMemoryCommitOutcome] {
        let barrier = InMemoryCommitBarrier(participantCount: 2)
        return await withTaskGroup(of: InMemoryCommitOutcome.self) { group in
            group.addTask {
                await barrier.arriveAndWait()
                return await Self.commitOutcome(first)
            }
            group.addTask {
                await barrier.arriveAndWait()
                return await Self.commitOutcome(second)
            }
            var outcomes: [InMemoryCommitOutcome] = []
            outcomes.reserveCapacity(2)
            for await outcome in group {
                outcomes.append(outcome)
            }
            return outcomes
        }
    }

    private static func commitOutcome(
        _ transaction: InMemoryTransaction
    ) async -> InMemoryCommitOutcome {
        do {
            try await transaction.commit()
            return .success
        } catch let error as StorageError {
            return .storageFailure(error.code)
        } catch {
            return .unexpectedFailure(String(describing: error))
        }
    }

    private func runWithConflictRetry(
        engine: InMemoryEngine,
        maximumAttempts: Int,
        operation: (InMemoryTransaction, Int) async throws -> Void
    ) async throws -> Int {
        precondition(maximumAttempts > 0)
        for attempt in 1...maximumAttempts {
            let transaction = try engine.createTransaction()
            do {
                try await operation(transaction, attempt)
                try await transaction.commit()
                return attempt
            } catch let error as StorageError
                where error.code == .transactionConflict {
                let conflictError = error
                do {
                    try await transaction.cancel()
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: conflictError,
                        cancellationError: error
                    )
                }
                if attempt == maximumAttempts {
                    throw conflictError
                }
            } catch {
                let operationError = error
                do {
                    try await transaction.cancel()
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                }
                throw operationError
            }
        }
        preconditionFailure("Retry loop exhausted without returning or throwing")
    }

    private func expectRejectedMutation(
        maximumBytes: Int,
        transaction: InMemoryTransaction,
        mutation: () throws -> Void
    ) async throws {
        try transaction.configureMutationByteLimit(
            maximumBytes: maximumBytes
        )
        #expect(throws: TransactionMutationByteLimitError.self) {
            try mutation()
        }
        try await transaction.commit()
    }
}

private enum InMemoryCommitOutcome: Sendable, Equatable, CustomStringConvertible {
    case success
    case storageFailure(StorageError.Code)
    case unexpectedFailure(String)

    var description: String {
        switch self {
        case .success:
            return "success"
        case .storageFailure(let code):
            return "storageFailure(\(code.rawValue))"
        case .unexpectedFailure(let description):
            return "unexpectedFailure(\(description))"
        }
    }
}

private enum InMemoryCommitObservationError: Error {
    case missingCounter
}

private actor InMemoryCommitBarrier {
    private let participantCount: Int
    private var arrivalCount = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []

    init(participantCount: Int) {
        precondition(participantCount > 0)
        self.participantCount = participantCount
    }

    func arriveAndWait() async {
        arrivalCount += 1
        if arrivalCount == participantCount {
            let readyWaiters = waiters
            waiters.removeAll(keepingCapacity: false)
            for waiter in readyWaiters {
                waiter.resume()
            }
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}
