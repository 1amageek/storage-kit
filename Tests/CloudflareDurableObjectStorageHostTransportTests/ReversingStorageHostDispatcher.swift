import CloudflareDurableObjectStorageHostTransport
import StorageKitEmbeddedCore

struct ReversingStorageHostDispatcher:
    StorageHostDispatching {
    func dispatch(
        _ requestBytes: EmbeddedBytes,
        maximumResponseBytes: Int
    ) throws -> EmbeddedBytes {
        EmbeddedBytes(
            Array(requestBytes.reversed().prefix(maximumResponseBytes))
        )
    }
}
