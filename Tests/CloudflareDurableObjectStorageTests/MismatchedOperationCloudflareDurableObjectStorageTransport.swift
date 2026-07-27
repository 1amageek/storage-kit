import DatabaseTypes
import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

struct MismatchedOperationCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(_ requestBytes: ByteString) async throws -> ByteString {
        _ = requestBytes
        return try StorageWire.encode(
            .readiness(
                StorageWireReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: 1,
                    metadataInitialized: true
                )
            )
        )
    }
}
