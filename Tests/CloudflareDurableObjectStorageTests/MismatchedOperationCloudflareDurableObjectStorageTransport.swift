import DatabaseTypes
import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

struct MismatchedOperationCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        _ = requestBytes
        return try encodeStorageTransportResponse(
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
