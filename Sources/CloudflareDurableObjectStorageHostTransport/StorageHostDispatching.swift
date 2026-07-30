import DatabaseTypes
import CloudflareDurableObjectStorageWire

public protocol StorageHostDispatching: Sendable {
    func dispatch(
        _ requestBytes: ByteString,
        maximumResponseBytes: Int
    ) throws(StorageHostTransportError) -> ByteString
}
