import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLite Nested Transaction Tests")
struct SQLiteNestedTransactionTests {

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

            // createTransaction inside withTransaction should detect nesting
            let childTx = try engine.createTransaction()
            childTx.setValue([20], for: [0x02])
            try await childTx.commit()

            // Outer should see child's writes (they share the same connection)
            let v2 = try await outerTx.getValue(for: [0x02])
            #expect(v2 == [20])
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
            guard case .invalidOperation = error else {
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
            guard case .invalidOperation = error else {
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
            guard case .backendError = error else {
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
