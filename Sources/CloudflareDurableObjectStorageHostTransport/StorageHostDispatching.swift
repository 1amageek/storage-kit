import StorageKitEmbeddedCore

public protocol StorageHostDispatching: Sendable {
    func dispatch(
        _ requestBytes: EmbeddedBytes,
        maximumResponseBytes: Int
    ) throws -> EmbeddedBytes
}
