import CloudflareDurableObjectStorage
import StorageKit
import StorageKitEmbeddedCore

final class ConfiguredFailureCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport, Sendable {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private let error: StorageError

    init(error: StorageError) {
        self.error = error
    }

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes {
        _ = requestBytes
        throw error
    }
}
