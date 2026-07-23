import Testing
import Foundation
@testable import PostgreSQLStorage
@testable import StorageKit

private struct PostgreSQLStorageDomainError: Error {}

/// Tests for StorageEngine.withAutoCommit() — auto-commit mode without BEGIN/COMMIT.
///
/// Verifies that auto-commit operations produce the same results as
/// transactional operations for single-statement use cases.
extension SerializedPostgreSQLStorageTests {
@Suite("AutoCommit Tests", .serialized, .enabled(if: PostgreSQLTestEnvironment.isConfigured))
struct AutoCommitTests {

    private func makeEngine() async throws -> PostgreSQLStorageEngine {
        let engine = try await PostgreSQLTestEnvironment.makeEngine()
        try await engine.withTransaction { tx in
            try tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
        }
        return engine
    }

    // MARK: - Read Operations

    @Test func autoCommitReadExistingKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Bytes("ac-read-1".utf8)
        let value: Bytes = Bytes("hello".utf8)

        // Write via transaction
        try await engine.withTransaction { tx in
            try tx.setValue(value, for: key)
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
            try await tx.getValue(for: Bytes("ac-nonexistent".utf8), snapshot: false)
        }

        #expect(result == nil)
    }

    // MARK: - Write Operations

    @Test func autoCommitWriteSingleKey() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Bytes("ac-write-1".utf8)
        let value: Bytes = Bytes("world".utf8)

        // Write via auto-commit
        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)
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

        let key: Bytes = Bytes("ac-delete-1".utf8)
        let value: Bytes = Bytes("to-delete".utf8)

        // Insert via transaction
        try await engine.withTransaction { tx in
            try tx.setValue(value, for: key)
        }

        // Delete via auto-commit
        try await engine.withAutoCommit { tx in
            try tx.clear(key: key)
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

        let key: Bytes = Bytes("ac-consistency".utf8)
        let value: Bytes = Bytes("consistent-value".utf8)

        // Write via auto-commit
        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)
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

        let key: Bytes = Bytes("ac-visibility".utf8)
        let value: Bytes = Bytes("visible-value".utf8)

        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)
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

        let key: Bytes = Bytes("ac-nested".utf8)
        let value: Bytes = Bytes("nested-value".utf8)

        // Auto-commit inside a transaction should reuse the transaction
        try await engine.withTransaction { tx in
            try tx.setValue(value, for: key)

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

        let key: Bytes = Bytes("ac-reverse-nested".utf8)
        let value: Bytes = Bytes("reverse-value".utf8)

        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)

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

        let key: Bytes = Bytes("ac-overwrite".utf8)
        let original: Bytes = Bytes("original".utf8)
        let updated: Bytes = Bytes("updated".utf8)

        try await engine.withAutoCommit { tx in
            try tx.setValue(original, for: key)
        }

        try await engine.withAutoCommit { tx in
            try tx.setValue(updated, for: key)
        }

        let result = try await engine.withAutoCommit { tx in
            try await tx.getValue(for: key, snapshot: false)
        }

        #expect(result == updated)
    }

    // MARK: - Error Handling

    @Test func transactionPreservesOperationDomainErrors() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        await #expect(throws: PostgreSQLStorageDomainError.self) {
            try await engine.withTransaction { tx in
                _ = try await tx.getValue(for: Bytes("domain-error-transaction".utf8), snapshot: false)
                throw PostgreSQLStorageDomainError()
            }
        }
    }

    @Test func autoCommitPreservesOperationDomainErrors() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        await #expect(throws: PostgreSQLStorageDomainError.self) {
            try await engine.withAutoCommit { tx in
                _ = try await tx.getValue(for: Bytes("domain-error-auto-commit".utf8), snapshot: false)
                throw PostgreSQLStorageDomainError()
            }
        }
    }

    @Test func autoCommitReturnValue() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key: Bytes = Bytes("ac-return".utf8)
        let value: Bytes = Bytes("return-value".utf8)

        try await engine.withAutoCommit { tx in
            try tx.setValue(value, for: key)
        }

        let result: Bool = try await engine.withAutoCommit { tx in
            let v = try await tx.getValue(for: key, snapshot: false)
            return v != nil
        }

        #expect(result == true)
    }
}
} // extension SerializedPostgreSQLStorageTests
