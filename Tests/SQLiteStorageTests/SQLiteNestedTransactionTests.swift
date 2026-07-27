import DatabaseTypes
import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLite Nested Transaction Tests")
struct SQLiteNestedTransactionTests {

    private func collectRange(
        _ tx: some TransactionAccess,
        begin: ByteString, end: ByteString
    ) async throws -> [(key: ByteString, value: ByteString)] {
        let seq = tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq {
            result.append((key: key, value: value))
        }
        return result
    }

    private func collectRange(
        _ tx: some TransactionAccess,
        from begin: KeySelector,
        to end: KeySelector
    ) async throws -> [(key: ByteString, value: ByteString)] {
        let seq = tx.getRange(from: begin, to: end, limit: 0, reverse: false)
        var result: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq {
            result.append((key: key, value: value))
        }
        return result
    }

    // =========================================================================
    // MARK: - Nested withTransaction
    //
    // SQLiteStorageEngine uses ActiveTransactionScope (TaskLocal) to detect
    // nested withTransaction calls. The inner call opens a SQLite savepoint on
    // the active FIFO connection lease.
    // =========================================================================

    @Test func nestedTransactionUsesSQLiteSavepoint() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let leaseBaseline = await engine.leaseInstrumentation

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            // Nested withTransaction enters a child savepoint.
            try await engine.withTransaction { innerTx in
                try innerTx.setValue([20], for: [0x02])

                // Inner should see outer's writes
                let v1 = try await innerTx.getValue(for: [0x01])
                #expect(v1 == [10])
            }

            // Outer should see inner's writes
            let v2 = try await outerTx.getValue(for: [0x02])
            #expect(v2 == [20])
        }

        // Both writes should be committed
        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            #expect(v1 == [10])
            #expect(v2 == [20])
        }

        let leaseMeasured = await engine.leaseInstrumentation
        #expect(
            leaseMeasured.savepointBeginCount
                == leaseBaseline.savepointBeginCount + 1
        )
        #expect(
            leaseMeasured.savepointReleaseCount
                == leaseBaseline.savepointReleaseCount + 1
        )
        #expect(
            leaseMeasured.savepointRollbackCount
                == leaseBaseline.savepointRollbackCount
        )
    }

    @Test func activeTransactionFromAnotherEngineIsNotReused() async throws {
        let firstEngine = try SQLiteStorageEngine(configuration: .inMemory)
        let secondEngine = try SQLiteStorageEngine(configuration: .inMemory)
        let key: ByteString = [0x01]

        try await firstEngine.withTransaction { firstTransaction in
            try firstTransaction.setValue([10], for: key)

            try await secondEngine.withTransaction { secondTransaction in
                try secondTransaction.setValue([20], for: key)
                #expect(
                    try await secondTransaction.getValue(for: key) == [20]
                )
            }

            #expect(try await firstTransaction.getValue(for: key) == [10])
        }

        try await firstEngine.withTransaction { transaction in
            let value = try await transaction.getValue(for: key)
            #expect(value == [10])
        }
        try await secondEngine.withTransaction { transaction in
            let value = try await transaction.getValue(for: key)
            #expect(value == [20])
        }
    }

    @Test func nestedWithTransaction_errorInInner_propagatesToOuter() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        struct NestedTransactionBodyFailure: Error {}

        do {
            try await engine.withTransaction { outerTx in
                try outerTx.setValue([10], for: [0x01])

                try await engine.withTransaction { innerTx in
                    try innerTx.setValue([20], for: [0x02])
                    throw NestedTransactionBodyFailure()
                }
            }
            Issue.record("Expected error")
        } catch is NestedTransactionBodyFailure {}

        // Everything should be rolled back
        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            #expect(v1 == nil)
            #expect(v2 == nil)
        }
    }

    @Test func nestedCreateTransaction_returnsChildTransaction() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            // createTransaction inside withTransaction creates a child whose
            // first async operation opens a SQLite savepoint.
            let childTx = try engine.createTransaction()
            let inheritedValue = try await childTx.getValue(for: [0x01])
            #expect(inheritedValue == [10])

            try childTx.setValue([20], for: [0x02])
            try await childTx.commit()

            // Outer should see committed child writes.
            let v2 = try await outerTx.getValue(for: [0x02])
            #expect(v2 == [20])
        }
    }

    @Test func nestedCreateTransaction_cancelAfterRangeDoesNotLeakChildWrites() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            try childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            try await childTx.cancel()

            let outerValue = try await outerTx.getValue(for: [0x01])
            let cancelledChildValue = try await outerTx.getValue(for: [0x02])
            #expect(outerValue == [10])
            #expect(cancelledChildValue == nil)
        }

        try await engine.withTransaction { tx in
            let outerValue = try await tx.getValue(for: [0x01])
            let cancelledChildValue = try await tx.getValue(for: [0x02])
            #expect(outerValue == [10])
            #expect(cancelledChildValue == nil)
        }
    }

    @Test func nestedCreateTransaction_failedChildCommitDoesNotPoisonOuter() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            try childTx.setValue([20], for: [0x02])
            try childTx.atomicOp(key: [0x03], param: [0x00], mutationType: .setVersionstampedKey)

            do {
                try await childTx.commit()
                Issue.record("Expected child commit to fail")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

            let childValue = try await outerTx.getValue(for: [0x02])
            #expect(childValue == nil)

            try outerTx.setValue([30], for: [0x04])
        }

        try await engine.withTransaction { tx in
            let outerValue = try await tx.getValue(for: [0x01])
            let rolledBackChildValue = try await tx.getValue(for: [0x02])
            let laterOuterValue = try await tx.getValue(for: [0x04])
            #expect(outerValue == [10])
            #expect(rolledBackChildValue == nil)
            #expect(laterOuterValue == [30])
        }
    }

    @Test func nestedCreateTransaction_atomicSeesOuterBufferedValue() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            try childTx.atomicOp(key: [0x01], param: [5], mutationType: .add)
            try await childTx.commit()

            let value = try await outerTx.getValue(for: [0x01])
            #expect(value == [15])
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0x01])
            #expect(value == [15])
        }
    }

    @Test func nestedCreateTransaction_parentIsSuspendedUntilChildCancellation() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            try childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            do {
                _ = try await outerTx.getValue(for: [0x02])
                Issue.record("Expected the parent read to be suspended")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

            try await childTx.cancel()

            let parentPointRead = try await outerTx.getValue(for: [0x02])
            let parentRange = try await collectRange(
                outerTx,
                begin: [0x00],
                end: [0xFF]
            )
            #expect(parentPointRead == nil)
            #expect(parentRange.map(\.key) == [[0x01]])
        }
    }

    @Test func nestedCreateTransaction_parentWriteAfterChildRangeSurvivesFailedChildCommit() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            try childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            do {
                try outerTx.setValue([30], for: [0x03])
                Issue.record("Expected the parent write to be suspended")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }
            try childTx.atomicOp(key: [0x04], param: [0x00], mutationType: .setVersionstampedKey)

            do {
                try await childTx.commit()
                Issue.record("Expected child commit to fail")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

            try outerTx.setValue([30], for: [0x03])

            #expect(try await outerTx.getValue(for: [0x01]) == [10])
            #expect(try await outerTx.getValue(for: [0x02]) == nil)
            #expect(try await outerTx.getValue(for: [0x03]) == [30])
        }

        try await engine.withTransaction { tx in
            let outerValue = try await tx.getValue(for: [0x01])
            let rolledBackChildValue = try await tx.getValue(for: [0x02])
            let laterOuterValue = try await tx.getValue(for: [0x03])
            #expect(outerValue == [10])
            #expect(rolledBackChildValue == nil)
            #expect(laterOuterValue == [30])
        }
    }

    @Test func nestedCreateTransaction_parentMutationsResumeAfterChildCancellation() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])
            let childTx = try engine.createTransaction()

            do {
                try outerTx.setValue([20], for: [0x02])
                Issue.record("Expected the parent write to be suspended")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }
            try childTx.setValue([30], for: [0x03])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x03]])

            try await childTx.cancel()
            try outerTx.setValue([20], for: [0x02])
            let outerRange = try await collectRange(outerTx, begin: [0x00], end: [0xFF])
            #expect(outerRange.map(\.key) == [[0x01], [0x02]])
        }
    }

    @Test func rejectedParentMutationDoesNotConsumeAdmissionBudget() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let parent = try engine.createTransaction()
        try parent.configureMutationByteLimit(maximumBytes: 19)

        do {
            try await ActiveTransactionScope.withActiveTransaction(
                parent
            ) { parentAccess in
                let child = try engine.createTransaction()

                do {
                    try parentAccess.setValue([0xA1], for: [0x01])
                    Issue.record("Expected the parent write to be suspended")
                } catch let error as StorageError {
                    #expect(error.code == .invalidOperation)
                }

                try await child.cancel()
                try parentAccess.setValue([0xA1], for: [0x01])
            }
            try await parent.commit()
        } catch {
            try await parent.cancel()
            throw error
        }

        try await engine.withTransaction { transaction in
            let storedValue = try await transaction.getValue(for: [0x01])
            #expect(storedValue == [0xA1])
        }
    }

    @Test func nestedCreateTransaction_rangeResolvesSelectorsAgainstChildBuffer() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([20], for: [0x20])
            try outerTx.setValue([30], for: [0x30])

            let childTx = try engine.createTransaction()
            try childTx.clear(key: [0x30])

            let childRange = try await collectRange(
                childTx,
                from: .lastLessOrEqual([0x35]),
                to: .firstGreaterOrEqual([0x40])
            )

            #expect(childRange.map(\.key) == [[0x20]])
            try await childTx.cancel()
        }
    }

    @Test func nestedCreateTransaction_enforcesStrictLIFOBeforeSequentialSibling() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            let firstChild = try engine.createTransaction()
            try firstChild.setValue([20], for: [0x02])
            let firstRange = try await collectRange(firstChild, begin: [0x00], end: [0xFF])
            #expect(firstRange.map(\.key) == [[0x01], [0x02]])

            do {
                _ = try engine.createTransaction()
                Issue.record("Expected strict LIFO child creation rejection")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

            try await firstChild.cancel()

            let secondChild = try engine.createTransaction()
            try secondChild.setValue([30], for: [0x03])
            try await secondChild.commit()

            #expect(try await outerTx.getValue(for: [0x02]) == nil)
            #expect(try await outerTx.getValue(for: [0x03]) == [30])
        }

        try await engine.withTransaction { tx in
            let outerValue = try await tx.getValue(for: [0x01])
            let cancelledFirstChildValue = try await tx.getValue(for: [0x02])
            let committedSecondChildValue = try await tx.getValue(for: [0x03])
            #expect(outerValue == [10])
            #expect(cancelledFirstChildValue == nil)
            #expect(committedSecondChildValue == [30])
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func releasedActiveChildRollsBackSavepointAndResumesParent() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let leaseBaseline = await engine.leaseInstrumentation

        try await engine.withTransaction { parent in
            let sqliteParent = try #require(
                parent as? SQLiteStorageTransaction
            )
            try parent.setValue([0xA1], for: [0x01])
            var child: SQLiteStorageTransaction? = try engine.createTransaction()
            try child?.setValue([0xA2], for: [0x02])
            #expect(try await child?.getValue(for: [0x02]) == [0xA2])

            child = nil
            for _ in 0..<10_000 {
                if sqliteParent.hasActiveChild == false {
                    break
                }
                await Task.yield()
            }
            #expect(sqliteParent.hasActiveChild == false)
            #expect(try await parent.getValue(for: [0x01]) == [0xA1])
            #expect(try await parent.getValue(for: [0x02]) == nil)
        }

        let measured = await engine.leaseInstrumentation
        #expect(measured.savepointBeginCount == leaseBaseline.savepointBeginCount + 1)
        #expect(measured.savepointRollbackCount == leaseBaseline.savepointRollbackCount + 1)
        #expect(measured.savepointReleaseCount == leaseBaseline.savepointReleaseCount + 1)
    }

    @Test func multipleSequentialNestedTransactions() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            try outerTx.setValue([10], for: [0x01])

            try await engine.withTransaction { inner1 in
                try inner1.setValue([20], for: [0x02])
            }

            try await engine.withTransaction { inner2 in
                try inner2.setValue([30], for: [0x03])
                // Should see writes from both outer and inner1
                let v1 = try await inner2.getValue(for: [0x01])
                let v2 = try await inner2.getValue(for: [0x02])
                #expect(v1 == [10])
                #expect(v2 == [20])
            }
        }

        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            let v3 = try await tx.getValue(for: [0x03])
            #expect(v1 == [10])
            #expect(v2 == [20])
            #expect(v3 == [30])
        }
    }

    // =========================================================================
    // MARK: - Shutdown / Close
    // =========================================================================

    @Test func shutdown_closesConnection() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        try await engine.withTransaction { tx in
            try tx.setValue([42], for: [0x01])
        }

        engine.shutdown()

        // After shutdown, createTransaction should throw
        do {
            _ = try engine.createTransaction()
            Issue.record("Expected error after shutdown")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    @Test func shutdown_idempotent() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        engine.shutdown()
        engine.shutdown() // Second call should not crash
    }

    @Test func close_thenWithTransaction_throws() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        engine.close()

        do {
            try await engine.withTransaction { tx in
                try tx.setValue([42], for: [0x01])
            }
            Issue.record("Expected error after close")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
        }
    }

    // =========================================================================
    // MARK: - KeyValueRangeResult Error Path (via SQLite)
    // =========================================================================

    @Test func rangeResult_errorThrowsOnIteration() async throws {
        let result = KeyValueRangeResult(error: StorageError.backendError("test"))

        do {
            for try await _ in result {
                Issue.record("Should not yield any elements")
            }
            Issue.record("Expected error to be thrown")
        } catch let error as StorageError {
            guard error.code == .backendFailure else {
                Issue.record("Expected backendError, got \(error)")
                return
            }
        }
    }

    // =========================================================================
    // MARK: - Namespace resolution for SQLite
    // =========================================================================

    @Test func sqliteEngineUsesDeterministicNamespaceResolution() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        #expect(engine.namespaceResolver is DeterministicNamespaceResolver)
        #expect(engine.namespaceCatalog == nil)
        engine.close()
    }
}
