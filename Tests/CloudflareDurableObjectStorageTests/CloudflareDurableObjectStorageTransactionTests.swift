import CloudflareDurableObjectStorageTesting
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing

@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Storage Transaction Tests")
struct CloudflareDurableObjectStorageTransactionTests {
    @Test func storedPairLimitConfigurationMatchesProtocolContract() {
        let protocolLimits = StorageWireLimits.cloudflareDurableObject
        #expect(
            CloudflareDurableObjectLimits.default.maxStoredKeyValueBytes
                == protocolLimits.maxStoredKeyValueBytes
        )
        #expect(protocolLimits.maxStoredKeyValueBytes == 2_000_000)

        do {
            _ = try CloudflareDurableObjectLimits(
                maxKeyBytes: 10,
                maxBoundaryBytes: 11,
                maxValueBytes: 10,
                maxStoredKeyValueBytes: 9,
                maxMutationsPerCommit: 20,
                maxConflictRangesPerCommit: 20,
                maxRangeLimit: 20,
                maxSplitPoints: 20
            )
            Issue.record("Expected the component to exceed the stored-pair limit")
        } catch let error as CloudflareDurableObjectLimitsError {
            #expect(
                error == .componentExceedsStoredPairLimit(
                    field: "maxKeyBytes",
                    value: 10,
                    maximum: 9
                )
            )
        } catch {
            Issue.record("Unexpected limits error: \(error)")
        }
    }

    @Test func configuredStoredPairLimitRejectsCombinedOverflow() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxStoredKeyValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 20,
            maxSplitPoints: 20
        )
        let engine = try await makeEngine(limits: limits)
        let transaction = try engine.createTransaction()

        #expect(throws: StorageError.self) {
            try transaction.setValue(
                ByteString([UInt8](repeating: 0x02, count: 5)),
                for: ByteString([UInt8](repeating: 0x01, count: 6))
            )
        }
        #expect(throws: StorageError.self) {
            try transaction.atomicOp(
                key: ByteString([UInt8](repeating: 0x01, count: 6)),
                param: ByteString([UInt8](repeating: 0x02, count: 5)),
                mutationType: .add
            )
        }
        try transaction.atomicOp(
            key: ByteString([UInt8](repeating: 0x01, count: 6)),
            param: ByteString([UInt8](repeating: 0x02, count: 5)),
            mutationType: .compareAndClear
        )
        try transaction.setValue(
            ByteString([UInt8](repeating: 0x02, count: 5)),
            for: ByteString([UInt8](repeating: 0x01, count: 5))
        )
    }

    @Test func configuredStoredPairLimitRejectsOversizedHostRead() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxStoredKeyValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 20,
            maxSplitPoints: 20
        )
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let partitionIdentity = try StoragePartitionIdentity(databaseID: "main")
        _ = try client.commitForTesting(
            StorageWireCommitRequest(
                partitionIdentity: partitionIdentity,
                observedReadVersion: nil,
                mutations: [
                    .set(
                        key: ByteString([UInt8](repeating: 0x01, count: 6)),
                        value: ByteString([UInt8](repeating: 0x02, count: 5))
                    ),
                ]
            )
        )
        let engine = try await makeEngine(client: client, limits: limits)
        let transaction = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await transaction.getValue(
                for: ByteString([UInt8](repeating: 0x01, count: 6))
            )
        }
    }

    @Test("Cloudflare bounded point reads preserve snapshots and exact bounds")
    func boundedPointReadPreservesSnapshotsAndExactBounds() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let partitionIdentity = try StoragePartitionIdentity(databaseID: "main")
        let key: ByteString = [0x71]
        let value: ByteString = [0x11, 0x22, 0x33]
        _ = try client.commitForTesting(
            StorageWireCommitRequest(
                partitionIdentity: partitionIdentity,
                observedReadVersion: nil,
                mutations: [.set(key: key, value: value)]
            )
        )
        let engine = try await makeEngine(client: client)
        defer { await engine.waitUntilShutdown() }
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
        #expect(failure?.backend == .cloudflareDurableObject)
        #expect(
            failure?.byteLimitViolation?.observedByteCount
                == UInt64(value.count)
        )
        #expect(
            failure?.byteLimitViolation?.maximumByteCount
                == UInt64(value.count - 1)
        )
        #expect(transaction.storageFailure == nil)

        // A caller-owned bound violation does not invalidate the transaction.
        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: value.count
            ) == value
        )

        do {
            _ = try await transaction.getValue(
                for: key,
                snapshot: true,
                maximumByteCount: -1
            )
            Issue.record("Expected an invalid point-read maximum")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.backend == .cloudflareDurableObject)
        }

        try await transaction.commit()
    }

    @Test func defaultLimitAllowsIndexExpandedMutationBatch() async throws {
        let engine = try await makeEngine()
        let mutationCount = 1_001

        try await engine.withTransaction { transaction in
            for index in 0..<mutationCount {
                let key = ByteString([
                    UInt8(truncatingIfNeeded: index >> 8),
                    UInt8(truncatingIfNeeded: index),
                ])
                try transaction.setValue([0x01], for: key)
            }
        }

        let finalKey = ByteString([
            UInt8(truncatingIfNeeded: (mutationCount - 1) >> 8),
            UInt8(truncatingIfNeeded: mutationCount - 1),
        ])
        let value = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: finalKey)
        }
        #expect(value == [0x01])
    }

    @Test func nonzeroIndexKeyViewsPreserveRangeOrdering() async throws {
        let engine = try await makeEngine()
        let keySource: ByteString = [0xAA, 0x10, 0xBB]
        let beginSource: ByteString = [0xAA, 0x0F]
        let endSource: ByteString = [0xAA, 0x11]
        let key = keySource[1..<2]
        let begin = beginSource[1..<2]
        let end = endSource[1..<2]

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x42], for: key)
        }

        let rows = try await engine.withTransaction { transaction in
            try await transaction.collectRange(begin: begin, end: end)
        }

        #expect(rows.count == 1)
        #expect(rows.first?.0 == key)
        #expect(rows.first?.1 == [0x42])
    }

    @Test func committedWriteIsVisibleToFreshTransaction() async throws {
        let engine = try await makeEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
        try await tx.commit()
    }

    @Test func readYourWritesReplaysSetAtomicAndClear() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()

        try tx.setValue([10], for: [0x01])
        try tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
        #expect(try await tx.getValue(for: [0x01]) == [15])

        try tx.clear(key: [0x01])
        #expect(try await tx.getValue(for: [0x01]) == nil)
        try await tx.commit()
    }

    @Test func rangeAppliesLocalWriteOverlay() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        try tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
        try tx.clear(key: [0x02])
        try tx.setValue([30], for: [0x03])

        let rows = try await tx.collectRange(begin: [0x01], end: [0x04])
        #expect(rows.map(\.0) == [[0x01], [0x03]])
        #expect(rows.map(\.1) == [[15], [30]])
        try await tx.commit()
    }

    @Test func rangeCoalescesRepeatedLocalMutationsForTheSameKey() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()

        try transaction.setValue([1], for: [0x01])
        try transaction.setValue([2], for: [0x01])
        try transaction.atomicOp(
            key: [0x01],
            param: [3],
            mutationType: .add
        )
        try transaction.setValue([9], for: [0x02])

        let rows = try await transaction.collectRange(
            begin: [0x01],
            end: [0x03]
        )

        #expect(rows.map(\.0) == [[0x01], [0x02]])
        #expect(rows.map(\.1) == [[5], [9]])
    }

    @Test func clearRangeParticipatesInReadYourWritesAndCommitPersistence() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        try tx.clearRange(beginKey: [0x02], endKey: [0x04])
        #expect(try await tx.getValue(for: [0x02]) == nil)
        #expect(try await tx.getValue(for: [0x03]) == nil)
        #expect(try await tx.getValue(for: [0x04]) == [4])
        let rows = try await tx.collectRange(begin: [0x01], end: [0x05], limit: 0)
        #expect(rows.map(\.0) == [[0x01], [0x04]])
        try await tx.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x02]) == nil)
        #expect(try await readTx.getValue(for: [0x03]) == nil)
        let committedRows = try await readTx.collectRange(begin: [0x01], end: [0x05], limit: 0)
        #expect(committedRows.map(\.0) == [[0x01], [0x04]])
    }

    @Test func pagedRangeMergesLocalWritesInKeyOrder() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 1,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([5], for: [0x05])
        }

        let tx = try engine.createTransaction()
        try tx.setValue([0], for: [0x00])
        try tx.setValue([4], for: [0x04])

        let rows = try await tx.collectRange(begin: [0x00], end: [0x06], limit: 0)

        #expect(rows.map(\.0) == [[0x00], [0x01], [0x03], [0x04], [0x05]])
    }

    @Test func unlimitedRangeFetchesAcrossHostPages() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 2,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(limits: limits)

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([4], for: [0x04])
            try tx.setValue([5], for: [0x05])
        }

        let tx = try engine.createTransaction()
        let rows = try await tx.collectRange(begin: [0x01], end: [0x06], limit: 0)

        #expect(rows.map(\.0) == [[0x01], [0x02], [0x03], [0x04], [0x05]])
    }

    @Test func rangeIteratorFetchesOnlyFirstHostPageForFirstElement() async throws {
        let pageCallCount = Mutex(0)
        let client = InMemoryCloudflareDurableObjectStorageClient(onRangeResponse: { _ in
            pageCallCount.withLock { $0 += 1 }
        })
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 2,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(client: client, limits: limits)

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        var cursor = tx.getRange(
            from: .firstGreaterOrEqual([0x01]),
            to: .firstGreaterOrEqual([0x05]),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        ).makeCursor()

        let first = try await cursor.next()

        #expect(first?.0 == [0x01])
        #expect(pageCallCount.withLock { $0 } == 1)
    }

    @Test func reverseRangeLimitReturnsLastKeys() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 2,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(limits: limits)

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        let rows = try await tx.collectRange(begin: [0x01], end: [0x05], limit: 2, reverse: true)

        #expect(rows.map(\.0) == [[0x04], [0x03]])
    }

    @Test func rangePaginationConflictsWhenVersionChangesBetweenPages() async throws {
        let didInterfere = Mutex(false)
        let clientHolder = Mutex<InMemoryCloudflareDurableObjectStorageClient?>(nil)
        let client = InMemoryCloudflareDurableObjectStorageClient(onRangeResponse: { request in
            guard request.cursorKey == nil else { return }
            let shouldInterfere = didInterfere.withLock { value in
                guard !value else { return false }
                value = true
                return true
            }
            guard shouldInterfere else { return }
            guard let client = clientHolder.withLock({ $0 }) else { return }
            _ = try client.commitForTesting(
                StorageWireCommitRequest(
                    partitionIdentity: request.partitionIdentity,
                    observedReadVersion: nil,
                    mutations: [
                        .set(
                            key: ByteString([0x09]),
                            value: ByteString([9])
                        )
                    ]
                )
            )
        })
        clientHolder.withLock { $0 = client }
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 1,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(client: client, limits: limits)

        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([2], for: [0x02])
            try tx.setValue([3], for: [0x03])
        }

        let tx = try engine.createTransaction()
        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x04], limit: 0)
        }
    }

    @Test func rangeSequenceAfterCommitThrowsInsteadOfReplayingCapturedWrites() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()
        try tx.setValue([1], for: [0x01])

        let range = tx.getRange(
            from: .firstGreaterOrEqual([0x01]),
            to: .firstGreaterOrEqual([0x02]),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        try await tx.commit()

        await #expect(throws: StorageError.self) {
            var cursor = range.makeCursor()
            _ = try await cursor.next()
        }
    }

    @Test func partitionIdentitiesAreIsolated() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let firstPartitionIdentity = try StoragePartitionIdentity(databaseID: "main", tenantID: "tenant-a")
        let secondPartitionIdentity = try StoragePartitionIdentity(databaseID: "main", tenantID: "tenant-b")
        let router = CloudflareDurableObjectSharedClientRouter(
            client: client,
            monotonicClock: SystemStorageClock()
        )
        let first = try await router.engine(for: firstPartitionIdentity)
        let second = try await router.engine(for: secondPartitionIdentity)

        try await first.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
        }
        try await second.withTransaction { tx in
            try tx.setValue([2], for: [0x01])
        }

        let firstTx = try first.createTransaction()
        let secondTx = try second.createTransaction()
        #expect(try await firstTx.getValue(for: [0x01]) == [1])
        #expect(try await secondTx.getValue(for: [0x01]) == [2])
    }

    @Test func versionstampMutationsMaterializeAndExposeCommittedStamp() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()
        let pendingVersionstamp = tx.requestVersionstamp()
        try tx.atomicOp(
            key: versionstampOperand(prefix: [0x10], suffix: [0x11]),
            param: [0x41],
            mutationType: .setVersionstampedKey
        )
        try tx.atomicOp(
            key: [0x20],
            param: versionstampOperand(prefix: [0x21], suffix: [0x22]),
            mutationType: .setVersionstampedValue
        )

        try await tx.commit()

        let stamp: ByteString = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]
        #expect(try await pendingVersionstamp.value.bytes == stamp)

        let read = try engine.createTransaction()
        #expect(
            try await read.getValue(
                for: ByteString([0x10] + stamp.copyBytes() + [0x11])
            ) == [0x41]
        )
        #expect(
            try await read.getValue(for: [0x20])
                == ByteString([0x21] + stamp.copyBytes() + [0x22])
        )
    }

    @Test func rangeMetricsAreExactAndSplitPointsIncludeEndpoints() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            try tx.setValue([0x10, 0x11], for: [0x01])
            try tx.setValue([0x20, 0x21], for: [0x02])
            try tx.setValue([0x30, 0x31, 0x32, 0x33], for: [0x03])
        }

        let tx = try engine.createTransaction()
        #expect(
            try await tx.getEstimatedRangeSizeBytes(
                beginKey: [0x01],
                endKey: [0x04]
            ) == 11
        )
        #expect(
            try await tx.getRangeSplitPoints(
                beginKey: [0x01],
                endKey: [0x04],
                chunkSize: 6
            ) == [[0x01], [0x03], [0x04]]
        )
    }

    @Test func rangeMetricsIncludePendingMutations() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0x10, 0x11], for: [0x01])
            try transaction.setValue([0x20, 0x21], for: [0x02])
        }

        let transaction = try engine.createTransaction()
        try transaction.clear(key: [0x01])
        try transaction.setValue([0x30, 0x31, 0x32, 0x33], for: [0x03])
        try transaction.setValue([0x40], for: [0x04])

        #expect(
            try await transaction.getEstimatedRangeSizeBytes(
                beginKey: [0x01],
                endKey: [0x05]
            ) == 10
        )
        #expect(
            try await transaction.getRangeSplitPoints(
                beginKey: [0x01],
                endKey: [0x05],
                chunkSize: 6
            ) == [[0x01], [0x03], [0x04], [0x05]]
        )
    }

    @Test func readConflictProgressDoesNotRetainCallerKeyStorage() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        let releaseRecorder = TransactionReleaseRecorder()
        var frame: ByteString? = makeTransactionOwnedBytes(
            [0x00, 0x10, 0x20, 0x30],
            releaseRecorder: releaseRecorder
        )
        var key = frame?[1..<2]

        _ = try await transaction.getValue(for: key!)
        frame = nil
        key = nil

        #expect(releaseRecorder.releaseCount == 1)
        try await transaction.cancel()
    }

    @Test func explicitConflictRangeDoesNotRetainCallerStorage() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        let releaseRecorder = TransactionReleaseRecorder()
        var frame: ByteString? = makeTransactionOwnedBytes(
            [0x00, 0x10, 0x20, 0x30],
            releaseRecorder: releaseRecorder
        )
        var begin = frame?[1..<2]
        var end = frame?[2..<3]

        try transaction.addConflictRange(
            beginKey: begin!,
            endKey: end!,
            type: .read
        )
        frame = nil
        begin = nil
        end = nil

        #expect(releaseRecorder.releaseCount == 1)
        try await transaction.cancel()
    }

    @Test func pendingMutationSplitPointsEnforceConfiguredLimitDuringGeneration() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 2,
            maxSplitPoints: 2,
        )
        let engine = try await makeEngine(limits: limits)
        let transaction = try engine.createTransaction()
        try transaction.setValue([0x10], for: [0x01])
        try transaction.setValue([0x20], for: [0x02])

        await #expect(throws: StorageError.self) {
            _ = try await transaction.getRangeSplitPoints(
                beginKey: [0x01],
                endKey: [0x03],
                chunkSize: 1
            )
        }
    }

    @Test func getKeySupportsKeysLexicographicallyAfterFF() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            try tx.setValue([0x41], for: [0xFF, 0x00])
        }

        let tx = try engine.createTransaction()
        #expect(
            try await tx.getKey(
                selector: .firstGreaterOrEqual([0xFF]),
                snapshot: true
            ) == [0xFF, 0x00]
        )
    }

    @Test func rangeMetricsRejectClosedTransactionsBeforeDispatch() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()
        try await tx.cancel()

        await #expect(throws: StorageError.self) {
            _ = try await tx.getEstimatedRangeSizeBytes(
                beginKey: [0x01],
                endKey: [0x02]
            )
        }
        await #expect(throws: StorageError.self) {
            _ = try await tx.getRangeSplitPoints(
                beginKey: [0x01],
                endKey: [0x02],
                chunkSize: 1
            )
        }
    }

    @Test func rangeSplitPointsRejectsResponsesAboveConfiguredLimit() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient(
            rangeSplitPointsResponseOverride: { request in
                StorageWireRangeSplitPointsResponse(
                    splitPoints: [
                        request.begin,
                        ByteString([0x02]),
                        request.end,
                    ],
                    currentCommitVersion: 0
                )
            }
        )
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 2,
            maxSplitPoints: 2,
        )
        let engine = try await makeEngine(client: client, limits: limits)
        let tx = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await tx.getRangeSplitPoints(
                beginKey: [0x01],
                endKey: [0x03],
                chunkSize: 1
            )
        }
    }

    private func versionstampOperand(
        prefix: ByteString,
        suffix: ByteString
    ) -> ByteString {
        let offset = UInt32(prefix.count)
        return ByteString(
            prefix.copyBytes()
                + Array(repeating: 0xFF, count: 10)
                + suffix.copyBytes()
                + [
                    UInt8(truncatingIfNeeded: offset),
                    UInt8(truncatingIfNeeded: offset >> 8),
                    UInt8(truncatingIfNeeded: offset >> 16),
                    UInt8(truncatingIfNeeded: offset >> 24),
                ]
        )
    }

    @Test func writeDuringCommitIsRejectedByTransactionLifecycle() async throws {
        let holder = Mutex<CloudflareDurableObjectStorageTransaction?>(nil)
        let rejection = Mutex<StorageError?>(nil)
        let client = InMemoryCloudflareDurableObjectStorageClient {
            holder.withLock { transaction in
                do {
                    try transaction?.setValue([99], for: [0x02])
                } catch let error as StorageError {
                    rejection.withLock { $0 = error }
                } catch {
                    Issue.record("Expected a StorageError, got \(error)")
                }
            }
        }
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()
        holder.withLock { $0 = tx }

        try tx.setValue([1], for: [0x01])
        try await tx.commit()
        #expect(rejection.withLock { $0?.code } == .invalidOperation)

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x01]) == [1])
        #expect(try await readTx.getValue(for: [0x02]) == nil)
    }

    @Test func observedReadVersionConflictFailsCommit() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01])
        _ = try await second.getValue(for: [0x01])

        try first.setValue([1], for: [0x01])
        try second.setValue([2], for: [0x02])

        try await first.commit()
        await #expect(throws: StorageError.self) {
            try await second.commit()
        }
    }

    @Test func unrelatedWriteAfterReadDoesNotConflictAtCommit() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01])

        let second = try engine.createTransaction()
        try second.setValue([2], for: [0x02])
        try await second.commit()

        try first.setValue([3], for: [0x03])
        try await first.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x02]) == [2])
        #expect(try await readTx.getValue(for: [0x03]) == [3])
    }

    @Test func commitUnknownResultLeavesTransactionNonReusable() async throws {
        let client = CloudflareDurableObjectStorageWireClient(
            transport: ConfiguredFailureCloudflareDurableObjectStorageTransport(
                error: StorageError(
                    code: .connectionFailure,
                    operation: .execute,
                    backend: .cloudflareDurableObject,
                    message: "Connection closed"
                )
            )
        )
        let partitionIdentity = try StoragePartitionIdentity(databaseID: "main")
        let tx = CloudflareDurableObjectStorageTransaction(
            partitionIdentity: partitionIdentity,
            client: client,
            limits: .default,
            monotonicClock: SystemStorageClock()
        )
        try tx.setValue([1], for: [0x01])

        do {
            try await tx.commit()
            Issue.record("Expected commit unknown result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
            #expect(error.retryDisposition == .requiresIdempotency)
            #expect(!error.isRetryable)
        }

        await #expect(throws: StorageError.self) {
            try await tx.commit()
        }
        await #expect(throws: StorageError.self) {
            _ = try await tx.getValue(for: [0x01])
        }
    }

    @Test func negativeCommittedVersionProducesUnknownOutcome() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient(
            commitResponseOverride: { _ in
                StorageWireCommitResponse(committedVersion: -1)
            }
        )
        let engine = try await makeEngine(client: client)
        let transaction = try engine.createTransaction()
        try transaction.setValue([1], for: [0x01])

        do {
            try await transaction.commit()
            Issue.record("Expected an unknown commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
        }

        await #expect(throws: StorageError.self) {
            try await transaction.commit()
        }
    }

    @Test func snapshotReadDoesNotParticipateInCommitConflict() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01], snapshot: true)

        let second = try engine.createTransaction()
        try second.setValue([1], for: [0x01])
        try await second.commit()

        try first.setValue([2], for: [0x02])
        try await first.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x01]) == [1])
        #expect(try await readTx.getValue(for: [0x02]) == [2])
    }

    @Test func keySelectorLastLessPatternsArePreservedByHostPagination() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 1,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([5], for: [0x05])
            try tx.setValue([7], for: [0x07])
        }

        let tx = try engine.createTransaction()
        let rows = try await tx.collectRange(
            from: .lastLessOrEqual([0x03]),
            to: .firstGreaterThan([0x05]),
            limit: 0
        )

        #expect(rows.map(\.0) == [[0x03], [0x05]])
    }

    @Test func allKeySelectorKindsArePreservedByPagedRangeScan() async throws {
        let limits = try CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxBoundaryBytes: 11,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxConflictRangesPerCommit: 20,
            maxRangeLimit: 1,
            maxSplitPoints: 20,
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            try tx.setValue([1], for: [0x01])
            try tx.setValue([3], for: [0x03])
            try tx.setValue([5], for: [0x05])
            try tx.setValue([7], for: [0x07])
        }

        let cases: [(KeySelector, KeySelector, [ByteString])] = [
            (
                .firstGreaterOrEqual([0x03]),
                .firstGreaterOrEqual([0x07]),
                [[0x03], [0x05]]
            ),
            (
                .firstGreaterThan([0x03]),
                .firstGreaterThan([0x05]),
                [[0x05]]
            ),
            (
                .lastLessOrEqual([0x05]),
                .firstGreaterThan([0x07]),
                [[0x05], [0x07]]
            ),
            (
                .lastLessThan([0x05]),
                .lastLessOrEqual([0x07]),
                [[0x03], [0x05]]
            ),
        ]

        for (begin, end, expectedKeys) in cases {
            let tx = try engine.createTransaction()
            let rows = try await tx.collectRange(from: begin, to: end, limit: 0)
            #expect(rows.map(\.0) == expectedKeys)
        }
    }

    @Test func rangeScanRejectsNonMonotonicHostRowsWithoutRememberingEveryEmittedKey() async throws
    {
        let client = InMemoryCloudflareDurableObjectStorageClient(rangeResponseOverride: {
            request in
            StorageWireRangeResponse(
                rows: [
                    StorageWireKeyValue(
                        key: ByteString([0x02]),
                        value: ByteString([2])
                    ),
                    StorageWireKeyValue(
                        key: ByteString([0x01]),
                        value: ByteString([1])
                    ),
                ],
                hasMore: false,
                currentCommitVersion: request.expectedReadVersion ?? 0
            )
        })
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x03], limit: 0)
        }
    }

    @Test func rangeScanRejectsEmptyHostPageWithCursorWithoutRememberingEveryCursor() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient(rangeResponseOverride: { _ in
            StorageWireRangeResponse(
                rows: [],
                hasMore: true,
                currentCommitVersion: 0
            )
        })
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x03], limit: 0)
        }
    }

    @Test func rangeScanRejectsPageThatDoesNotAdvancePastCursor() async throws {
        let client = InMemoryCloudflareDurableObjectStorageClient(rangeResponseOverride: {
            request in
            StorageWireRangeResponse(
                rows: [
                    StorageWireKeyValue(
                        key: ByteString([0x01]),
                        value: ByteString([1])
                    )
                ],
                hasMore: true,
                currentCommitVersion: 0
            )
        })
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x03], limit: 0)
        }
    }

    @Test func cloudflareTransactionOmitsUnsupportedPhysicalCompaction() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        #expect(
            !((transaction as Any) is any StorageCompactionTransaction)
        )
        try await transaction.cancel()
    }

    private func makeEngine(
        client: InMemoryCloudflareDurableObjectStorageClient =
            InMemoryCloudflareDurableObjectStorageClient(),
        limits: CloudflareDurableObjectLimits = .default
    ) async throws -> CloudflareDurableObjectStorageEngine {
        let partitionIdentity = try StoragePartitionIdentity(databaseID: "main")
        return try await CloudflareDurableObjectSharedClientRouter(
            client: client,
            limits: limits,
            monotonicClock: SystemStorageClock()
        ).engine(for: partitionIdentity)
    }
}

private func makeTransactionOwnedBytes(
    _ bytes: [UInt8],
    releaseRecorder: TransactionReleaseRecorder
) -> ByteString {
    ByteString(
        retaining: TransactionOwnedByteAllocation(
            bytes: bytes,
            releaseRecorder: releaseRecorder
        )
    )
}

private final class TransactionOwnedByteAllocation: ByteStringOwner {
    let count: Int

    private let address: UInt
    private let releaseRecorder: TransactionReleaseRecorder

    init(
        bytes: [UInt8],
        releaseRecorder: TransactionReleaseRecorder
    ) {
        let pointer = UnsafeMutableRawPointer.allocate(
            byteCount: bytes.count,
            alignment: MemoryLayout<UInt8>.alignment
        )
        bytes.withUnsafeBytes { source in
            if let sourceAddress = source.baseAddress {
                pointer.copyMemory(
                    from: sourceAddress,
                    byteCount: source.count
                )
            }
        }
        self.address = UInt(bitPattern: pointer)
        self.count = bytes.count
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        UnsafeMutableRawPointer(bitPattern: address)?.deallocate()
        releaseRecorder.recordRelease()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try body(
            UnsafeRawBufferPointer(
                start: UnsafeRawPointer(bitPattern: address),
                count: count
            )
        )
    }
}

private final class TransactionReleaseRecorder: Sendable {
    private let state = Mutex(0)

    var releaseCount: Int {
        state.withLock { $0 }
    }

    func recordRelease() {
        state.withLock { $0 += 1 }
    }
}
