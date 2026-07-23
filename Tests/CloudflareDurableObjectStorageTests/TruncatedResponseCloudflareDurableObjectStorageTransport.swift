import CloudflareDurableObjectStorage
import StorageKitEmbeddedCore

final class TruncatedResponseCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport, Sendable {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes {
        _ = requestBytes
        return [0x01]
    }
}
