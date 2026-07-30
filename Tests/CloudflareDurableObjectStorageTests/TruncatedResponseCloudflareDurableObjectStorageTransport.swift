import DatabaseTypes
import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

final class TruncatedResponseCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport, Sendable {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(_ requestBytes: ByteString) async -> ByteString {
        _ = requestBytes
        return [0x01]
    }
}
