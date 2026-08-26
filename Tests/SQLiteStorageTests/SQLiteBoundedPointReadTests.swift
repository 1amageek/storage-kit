import DatabaseTypes
import StorageKit
import Testing
@testable import SQLiteStorage

@Suite("SQLite bounded point reads")
struct SQLiteBoundedPointReadTests {
    @Test("SQLite rejects an oversized value before returning it")
    func rejectsOversizedValueBeforeReturning() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.waitUntilShutdown() }
        let key: ByteString = [0x51]
        let value: ByteString = [0x01, 0x02, 0x03, 0x04]

        try await engine.withTransaction { transaction in
            try transaction.setValue(value, for: key)
        }

        let transaction = try engine.createTransaction()
        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: value.count
            ) == value
        )

        var failure: StorageError?
        do {
            _ = try await transaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: value.count - 1
            )
        } catch let error as StorageError {
            failure = error
        }
        #expect(failure?.code == .valueTooLarge)
        #expect(failure?.backend == .sqlite)
        #expect(failure?.operation == .read)
        #expect(
            failure?.byteLimitViolation?.observedByteCount
                == UInt64(value.count)
        )
        #expect(
            failure?.byteLimitViolation?.maximumByteCount
                == UInt64(value.count - 1)
        )
        try await transaction.cancel()
    }

    @Test("SQLite bounded reads preserve read-your-writes and missing values")
    func preservesReadYourWritesAndMissingValues() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.waitUntilShutdown() }
        let key: ByteString = [0x61]
        let value: ByteString = [0xA0, 0xB0]
        let transaction = try engine.createTransaction()

        try transaction.setValue(value, for: key)
        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count
            ) == value
        )

        do {
            _ = try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count - 1
            )
            Issue.record("Expected the pending value to exceed the bound")
        } catch let error as StorageError {
            #expect(error.code == .valueTooLarge)
            #expect(error.backend == .sqlite)
        }
        #expect(transaction.storageFailure == nil)

        // A caller-owned bound violation does not invalidate the transaction.
        // The coordinator owns the flushed pending write, so it must remain
        // readable and committable after the failed bounded read.
        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count
            ) == value
        )
        try await transaction.commit()

        let committedTransaction = try engine.createTransaction()
        #expect(
            try await committedTransaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: value.count
            ) == value
        )
        try await committedTransaction.cancel()

        let missingTransaction = try engine.createTransaction()
        #expect(
            try await missingTransaction.getValue(
                for: [0xFF],
                snapshot: true,
                maximumByteCount: 0
            ) == nil
        )

        do {
            _ = try await missingTransaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: -1
            )
            Issue.record("Expected an invalid point-read maximum")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.backend == .sqlite)
        }
        try await missingTransaction.cancel()
    }
}
