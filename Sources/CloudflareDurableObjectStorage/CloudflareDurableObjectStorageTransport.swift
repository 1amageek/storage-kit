import StorageKitEmbeddedCore

/// StorageKit Wire request transport for a Durable Object storage endpoint.
public protocol CloudflareDurableObjectStorageTransport: Sendable {
    var callExecution: CloudflareDurableObjectCallExecution { get }

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes
}
