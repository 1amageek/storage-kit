import DatabaseTypes
import Foundation
import Testing
@testable import SQLiteStorage
@testable import StorageKit

@Suite("SQLite storage compaction")
struct SQLiteStorageCompactionTests {
    private enum InjectedCompactionFailure: Error {
        case capabilityMissing
        case abortTransaction
    }

    @Test func advertisesCompactionOnTransactionOnly() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }

        #expect(!((engine as Any) is any StorageCompactionTransaction))
        let transaction = try engine.createTransaction()
        #expect((transaction as Any) is any StorageCompactionTransaction)
        try await transaction.cancel()
    }

    @Test func newDatabaseUsesIncrementalAutoVacuum() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }

        let result = try await compact(
            engine: engine,
            maximumWorkUnits: 1,
            continuation: nil
        )

        #expect(result.isComplete)
        #expect(result.workUnitsConsumed == 0)
        #expect(result.remainingWorkUnits == 0)
        #expect(result.continuation == nil)
    }

    @Test func reclaimsFreelistAcrossBoundedSlices() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }
        let engine = try SQLiteStorageEngine(configuration: .file(path))
        defer { engine.shutdown() }

        try await populateAndDeleteRecords(engine: engine, count: 2_000, valueSize: 4_096)

        var result = try await compact(
            engine: engine,
            maximumWorkUnits: 1,
            continuation: nil
        )
        #expect(result.workUnitsConsumed == 1)
        #expect(result.remainingWorkUnits > 0)
        #expect(result.continuation?.bytes == [
            0x53, 0x43, 0x4D, 0x50,
            0x01, 0x01, 0x01, 0x00,
        ])

        let initialRemaining = result.remainingWorkUnits
        var totalConsumed = result.workUnitsConsumed
        for _ in 0..<16 where !result.isComplete {
            result = try await compact(
                engine: engine,
                maximumWorkUnits: SQLiteStorageTransaction.maximumCompactionWorkUnitsPerSlice,
                continuation: result.continuation
            )
            totalConsumed += result.workUnitsConsumed
        }

        #expect(result.isComplete)
        #expect(result.remainingWorkUnits == 0)
        #expect(totalConsumed > initialRemaining)
    }

    @Test func rejectsDatabaseWithoutIncrementalAutoVacuum() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }

        let unconfiguredConnection = try SQLiteConnection(path: path)
        try unconfiguredConnection.execute("CREATE TABLE preexisting_record (id INTEGER PRIMARY KEY)")
        #expect(try unconfiguredConnection.pragmaInt64("auto_vacuum") == 0)
        unconfiguredConnection.close()

        let engine = try SQLiteStorageEngine(configuration: .file(path))
        defer { engine.shutdown() }

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: 1,
                continuation: nil
            )
            Issue.record("Expected unsupported auto-vacuum configuration")
        } catch {
            #expect(error == .unsupportedConfiguration(
                feature: "sqlite.auto_vacuum.incremental",
                actualValue: 0
            ))
        }
    }

    @Test func rejectsZeroWorkLimit() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: 0,
                continuation: nil
            )
            Issue.record("Expected invalid work limit")
        } catch {
            #expect(error == .invalidMaximumWorkUnits(
                actual: 0,
                maximum: SQLiteStorageTransaction.maximumCompactionWorkUnitsPerSlice
            ))
        }
    }

    @Test func rejectsWorkLimitAboveBackendMaximum() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let maximum = SQLiteStorageTransaction.maximumCompactionWorkUnitsPerSlice

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: maximum + 1,
                continuation: nil
            )
            Issue.record("Expected excessive work limit rejection")
        } catch {
            #expect(error == .invalidMaximumWorkUnits(
                actual: maximum + 1,
                maximum: maximum
            ))
        }
    }

    @Test func rejectsCompactionOnNestedTransaction() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }

        try await engine.withTransaction { _ in
            let nested = try engine.createTransaction()
            let compaction: any StorageCompactionTransaction = nested
            do {
                _ = try await compaction.stageCompactionSlice(
                    maximumWorkUnits: 1,
                    continuation: nil
                )
                Issue.record("Expected nested transaction rejection")
            } catch let error as StorageCompactionError {
                #expect(error == .nestedTransaction)
            }
            try await nested.cancel()
        }
    }

    @Test func rollbackRestoresPhysicalCompactionAndBufferedWrites() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }
        let engine = try SQLiteStorageEngine(configuration: .file(path))
        defer { engine.shutdown() }

        try await populateAndDeleteRecords(engine: engine, count: 2_000, valueSize: 4_096)
        let before = try databasePageMetrics(path: path)
        #expect(before.freelistCount > 0)
        let markerKey: ByteString = [0xFD, 0x72, 0x6F, 0x6C, 0x6C, 0x62, 0x61, 0x63, 0x6B]

        do {
            try await engine.withTransaction { transaction in
                try transaction.setValue([0x01], for: markerKey)
                guard let compaction = transaction as? any StorageCompactionTransaction else {
                    throw InjectedCompactionFailure.capabilityMissing
                }
                let result = try await compaction.stageCompactionSlice(
                    maximumWorkUnits: 1,
                    continuation: nil
                )
                #expect(result.workUnitsConsumed == 1)
                throw InjectedCompactionFailure.abortTransaction
            }
            Issue.record("Expected transaction abort")
        } catch InjectedCompactionFailure.abortTransaction {
            // Expected: StorageEngine owns the authoritative rollback.
        }

        let marker = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: markerKey, snapshot: false)
        }
        #expect(marker == nil)

        engine.shutdown()
        let after = try databasePageMetrics(path: path)
        #expect(after.freelistCount == before.freelistCount)
        #expect(after.pageCount == before.pageCount)
    }

    @Test func rejectsMalformedContinuation() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let continuation = StorageCompactionContinuation(bytes: [0x00])

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: 1,
                continuation: continuation
            )
            Issue.record("Expected invalid continuation")
        } catch {
            #expect(error == .invalidContinuation)
        }
    }

    @Test func rejectsUnknownContinuationVersion() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let continuation = StorageCompactionContinuation(bytes: [
            0x53, 0x43, 0x4D, 0x50,
            0x02, 0x01, 0x01, 0x00,
        ])

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: 1,
                continuation: continuation
            )
            Issue.record("Expected unsupported continuation version")
        } catch {
            #expect(error == .unsupportedContinuationVersion(actual: 2, supported: 1))
        }
    }

    @Test func rejectsContinuationForAnotherBackend() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let continuation = StorageCompactionContinuation(bytes: [
            0x53, 0x43, 0x4D, 0x50,
            0x01, 0x7F, 0x01, 0x00,
        ])

        do {
            _ = try await compact(
                engine: engine,
                maximumWorkUnits: 1,
                continuation: continuation
            )
            Issue.record("Expected incompatible continuation")
        } catch {
            #expect(error == .incompatibleContinuation)
        }
    }

    private func populateAndDeleteRecords(
        engine: SQLiteStorageEngine,
        count: Int,
        valueSize: Int
    ) async throws {
        try await engine.withTransaction { transaction in
            for value in 0..<count {
                try transaction.setValue(
                    ByteString(
                        [UInt8](
                            repeating: UInt8(truncatingIfNeeded: value),
                            count: valueSize
                        )
                    ),
                    for: key(value)
                )
            }
        }
        try await engine.withTransaction { transaction in
            try transaction.clearRange(beginKey: [], endKey: [0xFF])
        }
    }

    private func compact(
        engine: SQLiteStorageEngine,
        maximumWorkUnits: UInt64,
        continuation: StorageCompactionContinuation?
    ) async throws(StorageCompactionError) -> StorageCompactionResult {
        do {
            return try await engine.withTransaction { transaction in
                guard let compaction = transaction as? any StorageCompactionTransaction else {
                    throw InjectedCompactionFailure.capabilityMissing
                }
                return try await compaction.stageCompactionSlice(
                    maximumWorkUnits: maximumWorkUnits,
                    continuation: continuation
                )
            }
        } catch let error as StorageCompactionError {
            throw error
        } catch {
            throw .backendFailure(description: String(describing: error))
        }
    }

    private func databasePageMetrics(path: String) throws -> (
        freelistCount: Int64,
        pageCount: Int64
    ) {
        let connection = try SQLiteConnection(path: path)
        defer { connection.close() }
        return (
            freelistCount: try connection.pragmaInt64("freelist_count"),
            pageCount: try connection.pragmaInt64("page_count")
        )
    }

    private func key(_ value: Int) -> ByteString {
        [
            UInt8(truncatingIfNeeded: value >> 24),
            UInt8(truncatingIfNeeded: value >> 16),
            UInt8(truncatingIfNeeded: value >> 8),
            UInt8(truncatingIfNeeded: value),
        ]
    }

    private func temporaryDatabasePath() -> String {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("storage-kit-compaction-\(UUID().uuidString).sqlite")
            .path
    }

    private func removeTemporaryDatabase(_ path: String) {
        let paths = [path, "\(path)-wal", "\(path)-shm"]
        for candidate in paths where FileManager.default.fileExists(atPath: candidate) {
            do {
                try FileManager.default.removeItem(atPath: candidate)
            } catch {
                Issue.record("Failed to remove temporary SQLite file: \(error)")
            }
        }
    }
}
