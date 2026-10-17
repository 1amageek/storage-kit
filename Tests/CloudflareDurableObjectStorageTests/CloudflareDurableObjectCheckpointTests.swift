import CloudflareDurableObjectStorageTesting
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import StorageKitConformance
import StorageKitSystemClock
import Synchronization
import Testing

@testable import CloudflareDurableObjectStorage

@Suite("Durable Object deferred checkpoints", .timeLimit(.minutes(1)))
struct CloudflareDurableObjectCheckpointTests {
    @Test func normalModeAndLateConfigurationRemainExplicit() async throws {
        let engine = try await makeEngine()
        let normal = try engine.createTransaction()
        #expect(normal.checkpoints == nil)
        _ = try await normal.getValue(for: [1], snapshot: true)
        #expect(throws: TransactionCheckpointError.configurationAfterAdmission) {
            try normal.configureCheckpoints(limits: limits())
        }
        try await normal.cancel()
        let cursorOwner = try engine.createTransaction()
        var cursor = cursorOwner.rangeCursor(from: .firstGreaterOrEqual([0]),
            to: .firstGreaterOrEqual([255]), limit: 1, reverse: false,
            snapshot: true, streamingMode: .exact)
        #expect(throws: TransactionCheckpointError.configurationAfterAdmission) {
            try cursorOwner.configureCheckpoints(limits: limits())
        }
        try await cursor.finish()
        try await cursorOwner.cancel()
    }

    @Test func rollbackReplaysOnlySurvivorsInOneCommit() async throws {
        let commits = Mutex<[StorageWireCommitRequest]>([])
        let client = InMemoryCloudflareDurableObjectStorageClient(commitResponseOverride: { request in
            commits.withLock { $0.append(request) }
            return nil
        })
        let engine = try await makeEngine(client: client)
        let tx = try configured(engine)
        let checkpoints = try #require(tx.checkpoints)
        try tx.setValue([11], for: [1])
        let outer = try await checkpoints.createCheckpoint()
        try tx.setValue([22], for: [2])
        let inner = try await checkpoints.createCheckpoint()
        try tx.clear(key: [1])
        try tx.addConflictRange(beginKey: [4], endKey: [5], type: .write)
        try await checkpoints.rollback(to: outer)
        await #expect(throws: TransactionCheckpointError.invalidToken) {
            try await checkpoints.release(inner)
        }
        let survivingValue = try await tx.getValue(for: [1], snapshot: false)
        #expect(survivingValue == [11])
        let discardedValue = try await tx.getValue(for: [2], snapshot: false)
        #expect(discardedValue == nil)
        #expect(commits.withLock { $0.isEmpty })
        try await checkpoints.release(outer)
        try await tx.commit()
        #expect(commits.withLock { $0.count } == 1)
        #expect(commits.withLock { $0[0].mutations.count } == 1)
        #expect(commits.withLock { $0[0].writeConflictRanges.isEmpty })
        let fresh = try engine.createTransaction()
        let persistedSurvivor = try await fresh.getValue(for: [1], snapshot: false)
        #expect(persistedSurvivor == [11])
        let persistedDiscard = try await fresh.getValue(for: [2], snapshot: false)
        #expect(persistedDiscard == nil)
        try await fresh.cancel()
    }

    @Test func mergedSelectorsRangesAndMetricsFollowRollback() async throws {
        let engine = try await makeEngine()
        let seed = try engine.createTransaction()
        try seed.setValue([10], for: [1])
        try seed.setValue([30], for: [3])
        try seed.setValue([50], for: [5])
        try await seed.commit()
        let tx = try configured(engine)
        let checkpoints = try #require(tx.checkpoints)
        try tx.setValue([20], for: [2])
        let marker = try await checkpoints.createCheckpoint()
        try tx.clearRange(beginKey: [1], endKey: [4])
        try tx.setValue([40], for: [4])
        try await checkpoints.rollback(to: marker)
        try tx.atomicOp(key: [1], param: [2], mutationType: .add)
        let incrementedValue = try await tx.getValue(for: [1], snapshot: false)
        #expect(incrementedValue == [12])
        let selectedKey = try await tx.getKey(selector: .firstGreaterThan([1]), snapshot: false)
        #expect(selectedKey == [2])
        let rows = try await tx.collectRange(from: .firstGreaterOrEqual([1]),
            to: .firstGreaterThan([5]), limit: 3, reverse: true, snapshot: false,
            streamingMode: .small)
        #expect(rows.map(\.0) == [[5], [3], [2]])
        let size = try await tx.getEstimatedRangeSizeBytes(beginKey: [1], endKey: [6])
        #expect(size == 8)
        let splitPoints = try await tx.getRangeSplitPoints(beginKey: [1], endKey: [6], chunkSize: 4)
        #expect(splitPoints == [[1], [3], [6]])
        try await tx.commit()
    }

    @Test func rawSnapshotReadConflictsSurviveRollback() async throws {
        let engine = try await makeEngine()
        let seed = try engine.createTransaction()
        try seed.setValue([1], for: [1])
        try await seed.commit()
        let tx = try configured(engine)
        let access = try #require(tx.checkpoints)
        let marker = try await access.createCheckpoint()
        let observedValue = try await tx.getValue(for: [1], snapshot: false)
        #expect(observedValue == [1])
        try tx.setValue([2], for: [2])
        try await access.rollback(to: marker)
        let writer = try engine.createTransaction()
        try writer.setValue([9], for: [1])
        try await writer.commit()
        try tx.setValue([3], for: [3])
        do {
            try await tx.commit()
            Issue.record("Expected the read conflict observed before rollback")
        } catch let error as StorageError {
            #expect(error.code == .transactionConflict)
        }
        let fresh = try engine.createTransaction()
        let uncommittedValue = try await fresh.getValue(for: [3], snapshot: false)
        #expect(uncommittedValue == nil)
        try await fresh.cancel()
    }

    @Test func unknownCommitOutcomeRemainsTerminal() async throws {
        let client = CloudflareDurableObjectStorageWireClient(
            transport: CommitFailingCloudflareDurableObjectStorageTransport())
        let engine = try await CloudflareDurableObjectStorageEngine(configuration: .init(
            partitionIdentity: StoragePartitionIdentity(databaseID: "checkpoint-unknown"),
            client: client, monotonicClock: SystemStorageClock()))
        let tx = try configured(engine)
        try tx.setValue([1], for: [1])
        let access = try #require(tx.checkpoints)
        do {
            try await tx.commit()
            Issue.record("Expected unknown commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        #expect(tx.storageFailure?.code == .commitUnknownResult)
        await #expect(throws: TransactionCheckpointError.self) {
            _ = try await access.createCheckpoint()
        }
        do {
            try await tx.commit()
            Issue.record("Unknown commits cannot report subsequent success")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
    }

    @Test func boundedPointReadsValidateTheFinalOverlayValue() async throws {
        let engine = try await makeEngine()
        let seed = try engine.createTransaction()
        try seed.setValue([1, 2, 3, 4], for: [1])
        try await seed.commit()
        let tx = try configured(engine)
        await #expect(throws: StorageError.self) {
            _ = try await tx.getValue(for: [1], snapshot: true, maximumByteCount: 2)
        }
        try tx.setValue([7], for: [1])
        let boundedValue = try await tx.getValue(for: [1], snapshot: false, maximumByteCount: 1)
        #expect(boundedValue == [7])
        try await tx.cancel()
    }

    @Test func journalCursorDoesNotRetainItsTransactionFacade() async throws {
        let engine = try await makeEngine()
        var tx: CloudflareDurableObjectStorageTransaction? = try configured(engine)
        weak let releasedTransaction = tx
        try tx?.setValue([1], for: [1])
        var cursor = try #require(tx).rangeCursor(from: .firstGreaterOrEqual([0]),
            to: .firstGreaterOrEqual([2]), limit: 0, reverse: false,
            snapshot: true, streamingMode: .small)
        tx = nil
        #expect(releasedTransaction == nil)
        let firstKey = try await cursor.next()?.0
        #expect(firstKey == [1])
        try await cursor.finish()
    }

    @Test func localOverlayReadsStillEnforceTheHostTransactionDeadline() async throws {
        let clock = CheckpointDeadlineClock()
        let engine = try await CloudflareDurableObjectStorageEngine(configuration: .init(
            partitionIdentity: StoragePartitionIdentity(databaseID: "checkpoint-deadline"),
            client: InMemoryCloudflareDurableObjectStorageClient(), monotonicClock: clock))
        let tx = try configured(engine)
        try tx.setValue([1], for: [1])
        try tx.setOption(forOption: .timeout(milliseconds: 10))
        clock.advance(by: .milliseconds(20))
        do {
            _ = try await tx.getValue(for: [1], snapshot: false)
            Issue.record("A locally resolved value must not bypass the transaction deadline")
        } catch let error as StorageError {
            #expect(error.code == .transactionTimedOut)
        }
        #expect(tx.storageFailure?.code == .transactionTimedOut)
        try await tx.cancel()
        let fresh = try engine.createTransaction()
        let uncommittedValue = try await fresh.getValue(for: [1], snapshot: false)
        #expect(uncommittedValue == nil)
        try await fresh.cancel()
    }

    @Test func sharedCheckpointTokens() async throws {
        try await conformance().verifyTokens()
    }

    @Test func sharedCheckpointOverlay() async throws {
        try await conformance().verifyOverlay()
    }

    @Test func sharedCheckpointDirectoryRollback() async throws {
        try await conformance().verifyDirectoryRollback()
    }

    @Test func sharedCheckpointConflicts() async throws {
        try await conformance().verifyConflicts()
    }

    @Test func sharedCheckpointLifecycle() async throws {
        try await conformance().verifyLifecycle()
    }

    @Test func sharedCheckpointBudgets() async throws {
        try await conformance().verifyBudgets()
    }

    private func conformance() -> TransactionCheckpointConformanceCase<CloudflareDurableObjectStorageEngine> {
        TransactionCheckpointConformanceCase {
            try await makeEngine()
        }
    }

    private func configured(_ engine: CloudflareDurableObjectStorageEngine) throws
        -> CloudflareDurableObjectStorageTransaction {
        let tx = try engine.createTransaction()
        try tx.configureCheckpoints(limits: limits())
        return tx
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 1_000, maximumDepth: 10,
            maximumRetainedBytes: 4_000_000, maximumWorkUnits: 100_000,
            maximumReadPageRows: 2)
    }

    private func makeEngine(client: InMemoryCloudflareDurableObjectStorageClient = .init())
        async throws -> CloudflareDurableObjectStorageEngine {
        try await CloudflareDurableObjectStorageEngine(configuration: .init(
            partitionIdentity: StoragePartitionIdentity(databaseID: "checkpoint-fixture"),
            client: client, monotonicClock: SystemStorageClock()))
    }
}
