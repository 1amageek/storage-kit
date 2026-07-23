import StorageKit
import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Storage Wire Client Tests")
struct CloudflareDurableObjectStorageWireClientTests {
    @Test func storageEngineRoundTripsThroughStorageWireClient() async throws {
        let engine = try await makeEngine()

        try await engine.withTransaction { tx in
            try tx.setValue([10], for: [0x01])
            try tx.setValue([20], for: [0x02])
            try tx.setValue([30], for: [0x03])
            try tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
            try tx.clear(key: [0x02])
        }

        let readTransaction = try engine.createTransaction()
        #expect(try await readTransaction.getValue(for: [0x01]) == [15])
        #expect(try await readTransaction.getValue(for: [0x02]) == nil)

        let rows = try await readTransaction.collectRange(begin: [0x01], end: [0x04], limit: 0)
        #expect(rows.map(\.0) == [[0x01], [0x03]])
        #expect(rows.map(\.1) == [[15], [30]])
    }

    @Test func storageWireClientPreservesArbitraryKeySelectorOffsets() async throws {
        let engine = try await makeEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([1], for: [0x01])
            try transaction.setValue([3], for: [0x03])
            try transaction.setValue([5], for: [0x05])
        }

        let transaction = try engine.createTransaction()
        let rows = try await transaction.collectRange(
            from: KeySelector(key: [0x05], orEqual: true, offset: -1),
            to: .firstGreaterThan([0x05]),
            limit: 0
        )

        #expect(rows.map(\.0) == [[0x03], [0x05]])
    }

    @Test func storageWireClientMaterializesVersionstampMutations() async throws {
        let engine = try await makeEngine()
        let transaction = try engine.createTransaction()
        try transaction.atomicOp(
            key: versionstampOperand(prefix: [0x10], suffix: [0x11]),
            param: [0x41],
            mutationType: .setVersionstampedKey
        )
        try transaction.atomicOp(
            key: [0x20],
            param: versionstampOperand(prefix: [0x21], suffix: [0x22]),
            mutationType: .setVersionstampedValue
        )

        try await transaction.commit()

        let stamp: Bytes = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]
        #expect(try await transaction.getVersionstamp() == stamp)
        let read = try engine.createTransaction()
        #expect(
            try await read.getValue(for: [0x10] + stamp + [0x11]) == [0x41]
        )
        #expect(
            try await read.getValue(for: [0x20]) == [0x21] + stamp + [0x22]
        )
    }

    @Test func commitTransportFailureMapsToCommitUnknownResult() async throws {
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
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")

        do {
            _ = try await client.commit(
                CloudflareDurableObjectCommitRequest(
                    scope: scope,
                    observedReadVersion: nil,
                    mutations: [
                        .set(
                            key: CloudflareDurableObjectBytes([0x01]),
                            value: CloudflareDurableObjectBytes([0x01])
                        )
                    ]
                )
            )
            Issue.record("Expected commit unknown result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
            #expect(error.retryDisposition == .requiresIdempotency)
            #expect(!error.isRetryable)
        }
    }

    @Test func commitDecodeFailureMapsToCommitUnknownResult() async throws {
        let client = CloudflareDurableObjectStorageWireClient(
            transport: TruncatedResponseCloudflareDurableObjectStorageTransport()
        )
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")

        do {
            _ = try await client.commit(
                CloudflareDurableObjectCommitRequest(
                    scope: scope,
                    observedReadVersion: nil,
                    mutations: [
                        .set(
                            key: CloudflareDurableObjectBytes([0x01]),
                            value: CloudflareDurableObjectBytes([0x01])
                        )
                    ]
                )
            )
            Issue.record("Expected commit unknown result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
            #expect(error.retryDisposition == .requiresIdempotency)
            #expect(!error.isRetryable)
        }
    }

    @Test func mismatchedCommitResponseOperationMapsToCommitUnknownResult() async throws {
        let client = CloudflareDurableObjectStorageWireClient(
            transport: MismatchedOperationCloudflareDurableObjectStorageTransport()
        )
        let scope = try CloudflareDurableObjectStorageScope(
            databaseID: "main"
        )

        do {
            _ = try await client.commit(
                CloudflareDurableObjectCommitRequest(
                    scope: scope,
                    observedReadVersion: nil,
                    mutations: [
                        .set(
                            key: CloudflareDurableObjectBytes([0x01]),
                            value: CloudflareDurableObjectBytes([0x01])
                        )
                    ]
                )
            )
            Issue.record("Expected commit unknown result")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
            #expect(error.operation == .commit)
        }
    }

    private func makeEngine() async throws -> CloudflareDurableObjectStorageEngine {
        let client = CloudflareDurableObjectStorageWireClient(
            transport: InMemoryCloudflareDurableObjectStorageTransport()
        )
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        return try await CloudflareDurableObjectSharedClientRouter(client: client).engine(for: scope)
    }

    private func versionstampOperand(
        prefix: Bytes,
        suffix: Bytes
    ) -> Bytes {
        let offset = UInt32(prefix.count)
        return prefix
            + Array(repeating: 0xFF, count: 10)
            + suffix
            + [
                UInt8(truncatingIfNeeded: offset),
                UInt8(truncatingIfNeeded: offset >> 8),
                UInt8(truncatingIfNeeded: offset >> 16),
                UInt8(truncatingIfNeeded: offset >> 24),
            ]
    }
}
