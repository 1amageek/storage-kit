import Testing
import Foundation
@testable import PostgreSQLStorage
@testable import StorageKit

/// Tests for StorageEngine.withAutoCommit() — auto-commit mode without BEGIN/COMMIT.
///
/// Verifies that auto-commit operations produce the same results as
/// transactional operations for single-statement use cases.
extension AllPostgreSQLTests {
@Suite("AutoCommit Tests", .serialized)
struct AutoCommitTests {

    private func makeEngine() async throws -> PostgreSQLStorageEngine {
        let engine = try await PostgreSQLTestHelper.makeEngine()
        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
        }
        return engine
    }

    // MARK: - Read Operations

    @Test func autoCommitReadExistingKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-read-1".utf8)
        let value: Bytes = Array("hello".utf8)

        // Write via transaction
        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }

        // Read via auto-commit
        let result = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == value)
    }

    @Test func autoCommitReadMissingKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let result = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: Array("ac-nonexistent".utf8), snapshot: false)
        }

        #expect(result == nil)
    }

    // MARK: - Write Operations

    @Test func autoCommitWriteSingleKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-write-1".utf8)
        let value: Bytes = Array("world".utf8)

        // Write via auto-commit
        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }

        // Verify via transaction
        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == value)
    }

    @Test func autoCommitDeleteSingleKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-delete-1".utf8)
        let value: Bytes = Array("to-delete".utf8)

        // Insert via transaction
        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)
        }

        // Delete via auto-commit
        try await engine.withAutoCommit { tx in
            tx.clear(key: key)
        }

        // Verify deletion via transaction
        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == nil)
    }

    // MARK: - Consistency with withTransaction

    @Test func autoCommitProducesSameResultAsTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-consistency".utf8)
        let value: Bytes = Array("consistent-value".utf8)

        // Write via auto-commit
        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }

        // Read via both paths
        let autoCommitResult = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        let transactionResult = try await engine.withTransaction { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(autoCommitResult == transactionResult)
    }

    @Test func autoCommitWriteVisibleToSubsequentTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-visibility".utf8)
        let value: Bytes = Array("visible-value".utf8)

        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }

        // Verify the write is durable and visible
        let result = try await engine.withTransaction { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == value)
    }

    // MARK: - Nesting Behavior

    @Test func autoCommitInsideTransactionReusesTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-nested".utf8)
        let value: Bytes = Array("nested-value".utf8)

        // Auto-commit inside a transaction should reuse the transaction
        try await engine.withTransaction { tx in
            tx.setValue(value, for: key)

            // Nested auto-commit should see the buffered write
            let result = try await engine.withAutoCommit { innerTx in
                try await innerTx.getValue(for: key, snapshot: false)
            }

            // Read-your-writes: the nested call reuses the parent transaction
            // and should see the buffered value
            #expect(result == value)
        }
    }

    @Test func transactionInsideAutoCommitReusesAutoCommit() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-reverse-nested".utf8)
        let value: Bytes = Array("reverse-value".utf8)

        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)

            // Nested withTransaction should reuse the auto-commit transaction
            let result = try await engine.withTransaction { innerTx in
                try await innerTx.getValue(for: key, snapshot: false)
            }

            #expect(result == value)
        }
    }

    // MARK: - Overwrite Semantics

    @Test func autoCommitOverwriteExistingKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-overwrite".utf8)
        let original: Bytes = Array("original".utf8)
        let updated: Bytes = Array("updated".utf8)

        try await engine.withAutoCommit { tx in
            tx.setValue(original, for: key)
        }

        try await engine.withAutoCommit { tx in
            tx.setValue(updated, for: key)
        }

        let result = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == updated)
    }

    // MARK: - Error Handling

    @Test func autoCommitReturnValue() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Array("ac-return".utf8)
        let value: Bytes = Array("return-value".utf8)

        try await engine.withAutoCommit { tx in
            tx.setValue(value, for: key)
        }

        let result: Bool = try await engine.withAutoCommit { tx in
            let v = try await tx.getValue(for: key, snapshot: false)
            return v != nil
        }

        #expect(result == true)
    }
}
} // extension AllPostgreSQLTests
