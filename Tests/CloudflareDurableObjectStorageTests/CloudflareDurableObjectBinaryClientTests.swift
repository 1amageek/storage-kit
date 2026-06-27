import StorageKit
import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Binary Client Tests")
struct CloudflareDurableObjectBinaryClientTests {
    @Test func storageEngineRoundTripsThroughBinaryClient() async throws {
        let engine = try await makeEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
            tx.atomicOp(key: [0x01], param: [5], mutationType: .add)
            tx.clear(key: [0x02])
        }

        let readTransaction = try engine.createTransaction()
        #expect(try await readTransaction.getValue(for: [0x01]) == [15])
        #expect(try await readTransaction.getValue(for: [0x02]) == nil)

        let rows = try await readTransaction.collectRange(begin: [0x01], end: [0x04], limit: 0)
        #expect(rows.map(\.0) == [[0x01], [0x03]])
        #expect(rows.map(\.1) == [[15], [30]])
    }

    @Test func binaryClientRejectsUnsupportedKeySelectorOffsets() async throws {
        let client = CloudflareDurableObjectBinaryClient(
            transport: FakeCloudflareDurableObjectBinaryTransport()
        )
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let request = CloudflareDurableObjectRangeRequest(
            scope: scope,
            begin: CloudflareDurableObjectKeySelector(
                key: CloudflareDurableObjectBytes([0x01]),
                orEqual: false,
                offset: 2
            ),
            end: CloudflareDurableObjectKeySelector(
                key: CloudflareDurableObjectBytes([0x02]),
                orEqual: false,
                offset: 1
            ),
            limit: 1,
            reverse: false,
            snapshot: false
        )

        await #expect(throws: StorageError.self) {
            _ = try await client.range(request)
        }
    }

    @Test func commitTransportFailureMapsToCommitUnknownResult() async throws {
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
            #expect(error.isRetryable)
        }
    }

    @Test func commitDecodeFailureMapsToCommitUnknownResult() async throws {
        let client = CloudflareDurableObjectBinaryClient(
            transport: CorruptingCloudflareDurableObjectBinaryTransport()
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
            #expect(error.isRetryable)
        }
    }

    private func makeEngine() async throws -> CloudflareDurableObjectStorageEngine {
        let client = CloudflareDurableObjectBinaryClient(
            transport: FakeCloudflareDurableObjectBinaryTransport()
        )
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        return try await CloudflareDurableObjectStorageEngineFactory(client: client).engine(for: scope)
    }
}
