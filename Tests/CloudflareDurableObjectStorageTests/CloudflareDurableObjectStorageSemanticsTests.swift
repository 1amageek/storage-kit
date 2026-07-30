import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageTesting
import CloudflareDurableObjectStorageWire
import StorageKit
import StorageKitSystemClock
import Testing

@Suite("Cloudflare Durable Object Storage Semantics Tests")
struct CloudflareDurableObjectStorageSemanticsTests {
    @Test func lastLessOrEqualResolvesAgainstLocalInsertDeleteAndClearRangeView() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { transaction in
            for key in [UInt8(1), 2, 3, 4, 5, 7] {
                try transaction.setValue([key], for: [key])
            }
        }

        let transaction = try engine.createTransaction()
        try transaction.setValue([6], for: [0x06])
        try transaction.clear(key: [0x04])
        try transaction.clearRange(beginKey: [0x02], endKey: [0x04])

        let rows = try await transaction.collectRange(
            from: .lastLessOrEqual([0x04]),
            to: .firstGreaterThan([0x06]),
            limit: 0
        )

        // The visible keys are [1, 5, 6, 7], so lastLessOrEqual(4) resolves to 1.
        #expect(rows.map(\.0) == [[0x01], [0x05], [0x06]])
    }

    @Test func arbitrarySelectorOffsetResolvesAgainstMergedTransactionView() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x01])
            try transaction.setValue([3], for: [0x03])
            try transaction.setValue([5], for: [0x05])
        }

        let transaction = try engine.createTransaction()
        try transaction.setValue([2], for: [0x02])

        let rows = try await transaction.collectRange(
            from: KeySelector(key: [0x01], orEqual: true, offset: 2),
            to: .firstGreaterThan([0x05]),
            limit: 0
        )

        // The local key shifts the selector from committed key 5 to visible key 3.
        #expect(rows.map(\.0) == [[0x03], [0x05]])
    }

    @Test func setReadVersionPinsSubsequentReads() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        try transaction.setReadVersion(0)

        let interferingTransaction = try engine.createTransaction()
        try interferingTransaction.setValue([1], for: [0x01])
        try await interferingTransaction.commit()

        #expect(try await transaction.getReadVersion() == 0)
        await #expect(throws: StorageError.self) {
            _ = try await transaction.getValue(for: [0x01])
        }
    }

    @Test func setReadVersionRejectsNegativeVersion() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()

        do {
            try transaction.setReadVersion(-1)
            Issue.record("Expected a negative read version to fail")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .read)
        }
    }

    @Test func setReadVersionCannotSwitchSnapshots() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        try transaction.setReadVersion(0)
        try transaction.setReadVersion(0)

        do {
            try transaction.setReadVersion(1)
            Issue.record("Expected an established read version to remain fixed")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .read)
        }
    }

    @Test func explicitReadConflictRangeRejectsOverlappingCommittedWrite() async throws {
        let engine = try await makeEngine()
        let protectedTransaction = try engine.createTransaction()
        _ = try await protectedTransaction.getReadVersion()
        try protectedTransaction.addConflictRange(
            beginKey: [0x10],
            endKey: [0x20],
            type: .read
        )

        let interferingTransaction = try engine.createTransaction()
        try interferingTransaction.setValue([1], for: [0x15])
        try await interferingTransaction.commit()

        try protectedTransaction.setValue([1], for: [0x30])
        await #expect(throws: StorageError.self) {
            try await protectedTransaction.commit()
        }
    }

    @Test func explicitWriteConflictRangeConflictsWithEarlierReader() async throws {
        let engine = try await makeEngine()
        let reader = try engine.createTransaction()
        _ = try await reader.getValue(for: [0x15])

        let writer = try engine.createTransaction()
        try writer.addConflictRange(
            beginKey: [0x10],
            endKey: [0x20],
            type: .write
        )
        try writer.setValue([1], for: [0x30])
        try await writer.commit()

        try reader.setValue([1], for: [0x40])
        await #expect(throws: StorageError.self) {
            try await reader.commit()
        }
    }

    @Test func cancellingInFlightCommitProducesUnknownOutcome() async throws {
        let transport = SuspendingCloudflareDurableObjectStorageTransport()
        let client = CloudflareDurableObjectStorageWireClient(transport: transport)
        let scope = try StorageWireScope(databaseID: "main")
        let engine = try await CloudflareDurableObjectSharedClientRouter(
            client: client,
            monotonicClock: SystemStorageClock()
        ).engine(for: scope)
        let transaction = try engine.createTransaction()
        try transaction.setValue([1], for: [0x01])

        let commitTask = Task {
            try await transaction.commit()
        }
        await transport.waitUntilCommitStarts()
        commitTask.cancel()

        let commitError: StorageError
        do {
            try await commitTask.value
            Issue.record("Expected an unknown commit outcome")
            return
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
            commitError = error
        } catch {
            Issue.record(
                "In-flight cancellation must not be reported as a known rollback: \(error)")
            return
        }

        do {
            try await transaction.cancel()
            Issue.record("Cancellation must report the same unknown commit outcome")
        } catch let error as StorageError {
            #expect(error == commitError)
        }
    }

    private func makeEngine() async throws -> CloudflareDurableObjectStorageEngine {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let scope = try StorageWireScope(databaseID: "main")
        return try await CloudflareDurableObjectSharedClientRouter(
            client: client,
            monotonicClock: SystemStorageClock()
        ).engine(for: scope)
    }
}
