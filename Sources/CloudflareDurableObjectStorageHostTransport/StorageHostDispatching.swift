import DatabaseTypes
import StorageKitEmbeddedCore

public protocol StorageHostDispatching: Sendable {
    func dispatch(
        _ requestBytes: ByteString,
        maximumResponseBytes: Int
    ) throws -> ByteString
}
