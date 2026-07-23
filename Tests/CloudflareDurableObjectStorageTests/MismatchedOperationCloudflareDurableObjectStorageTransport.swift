import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore

struct MismatchedOperationCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes {
        _ = requestBytes
        return try CloudflareDurableObjectStorageWireCodec.encode(
            .readiness(
                CloudflareDurableObjectEmbeddedReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: 1,
                    metadataInitialized: true
                )
            )
        )
    }
}
