import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLite Nested Transaction Tests")
struct SQLiteNestedTransactionTests {

    private func collectRange(
        _ tx: some Transaction,
        begin: Bytes, end: Bytes
    ) async throws -> [(key: Bytes, value: Bytes)] {
        let seq = tx.getRange(begin: begin, end: end, limit: 0, reverse: false)
        var result: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq {
            result.append((key: key, value: value))
        }
        return result
    }

    private func collectRange(
        _ tx: some Transaction,
        from begin: KeySelector,
        to end: KeySelector
    ) async throws -> [(key: Bytes, value: Bytes)] {
        let seq = tx.getRange(from: begin, to: end, limit: 0, reverse: false)
        var result: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq {
            result.append((key: key, value: value))
        }
        return result
    }

    // =========================================================================
    // MARK: - Nested withTransaction
    //
    // SQLiteStorageEngine uses ActiveTransactionScope (TaskLocal) to detect
    // nested withTransaction calls. The inner call reuses the existing
    // transaction instead of acquiring a new lock.
    // =========================================================================

    @Test func nestedWithTransaction_reusesExistingTransaction() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            // Nested withTransaction should reuse the same transaction
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

        // Both writes should be committed
        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            #expect(v1 == [10])
            #expect(v2 == [20])
        }
    }

    @Test func nestedWithTransaction_errorInInner_propagatesToOuter() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
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
            outerTx.setValue([10], for: [0x01])

            // createTransaction inside withTransaction should create a child
            // transaction backed by its own write buffer.
            let childTx = try engine.createTransaction()
            let inheritedValue = try await childTx.getValue(for: [0x01])
            #expect(inheritedValue == [10])

            childTx.setValue([20], for: [0x02])
            try await childTx.commit()

            // Outer should see committed child writes.
            let v2 = try await outerTx.getValue(for: [0x02])
            #expect(v2 == [20])
        }
    }

    @Test func nestedCreateTransaction_cancelAfterRangeDoesNotLeakChildWrites() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            childTx.cancel()

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
            outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            childTx.setValue([20], for: [0x02])
            childTx.atomicOp(key: [0x03], param: [0x00], mutationType: .setVersionstampedKey)

            do {
                try await childTx.commit()
                Issue.record("Expected child commit to fail")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

            let childValue = try await outerTx.getValue(for: [0x02])
            #expect(childValue == nil)

            outerTx.setValue([30], for: [0x04])
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
            outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            childTx.atomicOp(key: [0x01], param: [5], mutationType: .add)
            try await childTx.commit()

            let value = try await outerTx.getValue(for: [0x01])
            #expect(value == [15])
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0x01])
            #expect(value == [15])
        }
    }

    @Test func nestedCreateTransaction_parentDoesNotSeeUncommittedChildAfterRange() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            let parentPointRead = try await outerTx.getValue(for: [0x02])
            let parentRange = try await collectRange(outerTx, begin: [0x00], end: [0xFF])
            #expect(parentPointRead == nil)
            #expect(parentRange.map(\.key) == [[0x01]])

            childTx.cancel()
        }
    }

    @Test func nestedCreateTransaction_parentWriteAfterChildRangeSurvivesFailedChildCommit() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            let childTx = try engine.createTransaction()
            childTx.setValue([20], for: [0x02])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02]])

            outerTx.setValue([30], for: [0x03])
            childTx.atomicOp(key: [0x04], param: [0x00], mutationType: .setVersionstampedKey)

            do {
                try await childTx.commit()
                Issue.record("Expected child commit to fail")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
            }

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

    @Test func nestedCreateTransaction_rangeSeesParentWritesAfterChildCreation() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])
            let childTx = try engine.createTransaction()

            outerTx.setValue([20], for: [0x02])
            childTx.setValue([30], for: [0x03])

            let childRange = try await collectRange(childTx, begin: [0x00], end: [0xFF])
            #expect(childRange.map(\.key) == [[0x01], [0x02], [0x03]])

            childTx.cancel()
            let outerRange = try await collectRange(outerTx, begin: [0x00], end: [0xFF])
            #expect(outerRange.map(\.key) == [[0x01], [0x02]])
        }
    }

    @Test func nestedCreateTransaction_rangeResolvesSelectorsAgainstChildBuffer() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([20], for: [0x20])
            outerTx.setValue([30], for: [0x30])

            let childTx = try engine.createTransaction()
            childTx.clear(key: [0x30])

            let childRange = try await collectRange(
                childTx,
                from: .lastLessOrEqual([0x35]),
                to: .firstGreaterOrEqual([0x40])
            )

            #expect(childRange.map(\.key) == [[0x20]])
            childTx.cancel()
        }
    }

    @Test func nestedCreateTransaction_siblingCommitSurvivesEarlierChildCancel() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

        try await engine.withTransaction { outerTx in
            outerTx.setValue([10], for: [0x01])

            let firstChild = try engine.createTransaction()
            firstChild.setValue([20], for: [0x02])
            let firstRange = try await collectRange(firstChild, begin: [0x00], end: [0xFF])
            #expect(firstRange.map(\.key) == [[0x01], [0x02]])

            let secondChild = try engine.createTransaction()
            secondChild.setValue([30], for: [0x03])
            try await secondChild.commit()

            firstChild.cancel()

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

    @Test func multipleSequentialNestedTransactions() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)

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
            tx.setValue([42], for: [0x01])
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
                tx.setValue([42], for: [0x01])
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
    // MARK: - DirectoryService for SQLite
    // =========================================================================

    @Test func sqliteEngine_directoryServiceIsStatic() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let service = engine.directoryService
        #expect(service is StaticDirectoryService)
        engine.close()
    }
}
