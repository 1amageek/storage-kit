import DatabaseTypes
import CloudflareDurableObjectStorageHostTransport
import CloudflareDurableObjectStorageWire

struct ReversingStorageHostDispatcher:
    StorageHostDispatching {
    func dispatch(
        _ requestBytes: ByteString,
        maximumResponseBytes: Int
    ) throws -> ByteString {
        ByteString(
            Array(requestBytes.reversed().prefix(maximumResponseBytes))
        )
    }
}
