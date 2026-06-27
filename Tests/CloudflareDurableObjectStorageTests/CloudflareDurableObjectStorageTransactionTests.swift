import Testing
import StorageKit
import Synchronization
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Storage Transaction Tests")
struct CloudflareDurableObjectStorageTransactionTests {
    @Test func committedWriteIsVisibleToFreshTransaction() async throws {
        let engine = try await makeEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
        }

        let tx = try engine.createTransaction()
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [10])
        try await tx.commit()
    }

    @Test func readYourWritesReplaysSetAtomicAndClear() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()

        tx.setValue([10], for: [0x01])
        tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
        #expect(try await tx.getValue(for: [0x01]) == [15])

        tx.clear(key: [0x01])
        #expect(try await tx.getValue(for: [0x01]) == nil)
        try await tx.commit()
    }

    @Test func rangeAppliesLocalWriteOverlay() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
        }

        let tx = try engine.createTransaction()
        tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
        tx.clear(key: [0x02])
        tx.setValue([30], for: [0x03])

        let rows = try await tx.collectRange(begin: [0x01], end: [0x04])
        #expect(rows.map(\.0) == [[0x01], [0x03]])
        #expect(rows.map(\.1) == [[15], [30]])
        try await tx.commit()
    }

    @Test func clearRangeParticipatesInReadYourWritesAndCommitPersistence() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        tx.clearRange(beginKey: [0x02], endKey: [0x04])
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
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 1,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([3], for: [0x03])
            tx.setValue([5], for: [0x05])
        }

        let tx = try engine.createTransaction()
        tx.setValue([0], for: [0x00])
        tx.setValue([4], for: [0x04])

        let rows = try await tx.collectRange(begin: [0x00], end: [0x06], limit: 0)

        #expect(rows.map(\.0) == [[0x00], [0x01], [0x03], [0x04], [0x05]])
    }

    @Test func unlimitedRangeFetchesAcrossHostPages() async throws {
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 2,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(limits: limits)

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
            tx.setValue([5], for: [0x05])
        }

        let tx = try engine.createTransaction()
        let rows = try await tx.collectRange(begin: [0x01], end: [0x06], limit: 0)

        #expect(rows.map(\.0) == [[0x01], [0x02], [0x03], [0x04], [0x05]])
    }

    @Test func rangeIteratorFetchesOnlyFirstHostPageForFirstElement() async throws {
        let pageCallCount = Mutex(0)
        let client = FakeCloudflareDurableObjectStorageClient(onRangeResponse: { _ in
            pageCallCount.withLock { $0 += 1 }
        })
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 2,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(client: client, limits: limits)

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        var iterator = tx.getRange(begin: [0x01], end: [0x05], limit: 0).makeAsyncIterator()

        let first = try await iterator.next()

        #expect(first?.0 == [0x01])
        #expect(pageCallCount.withLock { $0 } == 1)
    }

    @Test func reverseRangeLimitReturnsLastKeys() async throws {
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 2,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(limits: limits)

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
        }

        let tx = try engine.createTransaction()
        let rows = try await tx.collectRange(begin: [0x01], end: [0x05], limit: 2, reverse: true)

        #expect(rows.map(\.0) == [[0x04], [0x03]])
    }

    @Test func rangePaginationConflictsWhenVersionChangesBetweenPages() async throws {
        let didInterfere = Mutex(false)
        let clientHolder = Mutex<FakeCloudflareDurableObjectStorageClient?>(nil)
        let client = FakeCloudflareDurableObjectStorageClient(onRangeResponse: { request in
            guard request.cursor == nil else { return }
            let shouldInterfere = didInterfere.withLock { value in
                guard !value else { return false }
                value = true
                return true
            }
            guard shouldInterfere else { return }
            guard let client = clientHolder.withLock({ $0 }) else { return }
            _ = try client.commitForTesting(
                CloudflareDurableObjectCommitRequest(
                    scope: request.scope,
                    observedReadVersion: nil,
                    mutations: [
                        .set(
                            key: CloudflareDurableObjectBytes([0x09]),
                            value: CloudflareDurableObjectBytes([9])
                        )
                    ]
                )
            )
        })
        clientHolder.withLock { $0 = client }
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 1,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(client: client, limits: limits)

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
        }

        let tx = try engine.createTransaction()
        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x04], limit: 0)
        }
    }

    @Test func rangeSequenceAfterCommitThrowsInsteadOfReplayingCapturedWrites() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])

        let sequence = tx.getRange(begin: [0x01], end: [0x02])
        try await tx.commit()

        await #expect(throws: StorageError.self) {
            for try await _ in sequence {}
        }
    }

    @Test func scopesAreIsolated() async throws {
        let client = FakeCloudflareDurableObjectStorageClient()
        let firstScope = try CloudflareDurableObjectStorageScope(databaseID: "main", tenantID: "tenant-a")
        let secondScope = try CloudflareDurableObjectStorageScope(databaseID: "main", tenantID: "tenant-b")
        let factory = CloudflareDurableObjectStorageEngineFactory(client: client)
        let first = try await factory.engine(for: firstScope)
        let second = try await factory.engine(for: secondScope)

        try await first.withTransaction { tx in
            tx.setValue([1], for: [0x01])
        }
        try await second.withTransaction { tx in
            tx.setValue([2], for: [0x01])
        }

        let firstTx = try first.createTransaction()
        let secondTx = try second.createTransaction()
        #expect(try await firstTx.getValue(for: [0x01]) == [1])
        #expect(try await secondTx.getValue(for: [0x01]) == [2])
    }

    @Test func versionstampMutationFailsAtCommit() async throws {
        let engine = try await makeEngine()
        let tx = try engine.createTransaction()
        tx.atomicOp(key: [0x01], param: [0x01], mutationType: .setVersionstampedValue)

        await #expect(throws: StorageError.self) {
            try await tx.commit()
        }
    }

    @Test func writeDuringCommitIsRejectedByStateMachine() async throws {
        let holder = Mutex<CloudflareDurableObjectStorageTransaction?>(nil)
        let client = FakeCloudflareDurableObjectStorageClient {
            holder.withLock { transaction in
                transaction?.setValue([99], for: [0x02])
            }
        }
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()
        holder.withLock { $0 = tx }

        tx.setValue([1], for: [0x01])
        try await tx.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x01]) == [1])
        #expect(try await readTx.getValue(for: [0x02]) == nil)
    }

    @Test func observedReadVersionConflictFailsCommit() async throws {
        let client = FakeCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        let second = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01])
        _ = try await second.getValue(for: [0x01])

        first.setValue([1], for: [0x01])
        second.setValue([2], for: [0x02])

        try await first.commit()
        await #expect(throws: StorageError.self) {
            try await second.commit()
        }
    }

    @Test func unrelatedWriteAfterReadDoesNotConflictAtCommit() async throws {
        let client = FakeCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01])

        let second = try engine.createTransaction()
        second.setValue([2], for: [0x02])
        try await second.commit()

        first.setValue([3], for: [0x03])
        try await first.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x02]) == [2])
        #expect(try await readTx.getValue(for: [0x03]) == [3])
    }

    @Test func commitUnknownResultLeavesTransactionNonReusable() async throws {
        let client = CloudflareDurableObjectBinaryClient(
            transport: ThrowingCloudflareDurableObjectBinaryTransport(
                error: StorageError(
                    code: .connectionFailure,
                    operation: .execute,
                    backend: .cloudflareDurableObject,
                    message: "Connection closed"
                )
            )
        )
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let engine = try await CloudflareDurableObjectStorageEngineFactory(client: client).engine(for: scope)
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])

        do {
            try await tx.commit()
            Issue.record("Expected commit unknown result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
            #expect(error.isRetryable)
        }

        await #expect(throws: StorageError.self) {
            try await tx.commit()
        }
        await #expect(throws: StorageError.self) {
            _ = try await tx.getValue(for: [0x01])
        }
    }

    @Test func snapshotReadDoesNotParticipateInCommitConflict() async throws {
        let client = FakeCloudflareDurableObjectStorageClient()
        let engine = try await makeEngine(client: client)

        let first = try engine.createTransaction()
        _ = try await first.getValue(for: [0x01], snapshot: true)

        let second = try engine.createTransaction()
        second.setValue([1], for: [0x01])
        try await second.commit()

        first.setValue([2], for: [0x02])
        try await first.commit()

        let readTx = try engine.createTransaction()
        #expect(try await readTx.getValue(for: [0x01]) == [1])
        #expect(try await readTx.getValue(for: [0x02]) == [2])
    }

    @Test func keySelectorLastLessPatternsArePreservedByHostPagination() async throws {
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 1,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([3], for: [0x03])
            tx.setValue([5], for: [0x05])
            tx.setValue([7], for: [0x07])
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
        let limits = CloudflareDurableObjectLimits(
            maxKeyBytes: 10,
            maxValueBytes: 10,
            maxMutationsPerCommit: 20,
            maxRangeLimit: 1,
            maxNameBytes: 512
        )
        let engine = try await makeEngine(limits: limits)
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([3], for: [0x03])
            tx.setValue([5], for: [0x05])
            tx.setValue([7], for: [0x07])
        }

        let cases: [(KeySelector, KeySelector, [Bytes])] = [
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
            )
        ]

        for (begin, end, expectedKeys) in cases {
            let tx = try engine.createTransaction()
            let rows = try await tx.collectRange(from: begin, to: end, limit: 0)
            #expect(rows.map(\.0) == expectedKeys)
        }
    }

    @Test func rangeScanRejectsNonMonotonicHostRowsWithoutRememberingEveryEmittedKey() async throws {
        let client = FakeCloudflareDurableObjectStorageClient(rangeResponseOverride: { request in
            CloudflareDurableObjectRangeResponse(
                rows: [
                    CloudflareDurableObjectKeyValue(
                        key: CloudflareDurableObjectBytes([0x02]),
                        value: CloudflareDurableObjectBytes([2])
                    ),
                    CloudflareDurableObjectKeyValue(
                        key: CloudflareDurableObjectBytes([0x01]),
                        value: CloudflareDurableObjectBytes([1])
                    )
                ],
                nextCursor: nil,
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
        let client = FakeCloudflareDurableObjectStorageClient(rangeResponseOverride: { _ in
            CloudflareDurableObjectRangeResponse(
                rows: [],
                nextCursor: "stalled",
                currentCommitVersion: 0
            )
        })
        let engine = try await makeEngine(client: client)
        let tx = try engine.createTransaction()

        await #expect(throws: StorageError.self) {
            _ = try await tx.collectRange(begin: [0x01], end: [0x03], limit: 0)
        }
    }

    private func makeEngine(
        client: FakeCloudflareDurableObjectStorageClient = FakeCloudflareDurableObjectStorageClient(),
        limits: CloudflareDurableObjectLimits = .default
    ) async throws -> CloudflareDurableObjectStorageEngine {
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        return try await CloudflareDurableObjectStorageEngineFactory(
            client: client,
            limits: limits
        ).engine(for: scope)
    }
}
