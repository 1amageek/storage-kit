import DatabaseTypes
import CloudflareDurableObjectStorage
import StorageKit
import CloudflareDurableObjectStorageWire

final class ConfiguredFailureCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport, Sendable {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private let error: StorageError

    init(error: StorageError) {
        self.error = error
    }

    func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        _ = requestBytes
        throw .storage(error)
    }
}
