import DatabaseTypes
import CloudflareDurableObjectStorageWire

/// StorageKit Wire request transport for a Durable Object storage endpoint.
public protocol CloudflareDurableObjectStorageTransport: Sendable {
    var callExecution: CloudflareDurableObjectCallExecution { get }

    func send(_ requestBytes: ByteString) async throws -> ByteString
}
