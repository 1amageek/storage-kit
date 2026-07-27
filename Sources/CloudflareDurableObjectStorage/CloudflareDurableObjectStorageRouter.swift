import CloudflareDurableObjectStorageWire

/// Routes logical scopes to Cloudflare Durable Object storage engines.
public protocol CloudflareDurableObjectStorageRouter: Sendable {
    func engine(for scope: StorageWireScope) async throws -> CloudflareDurableObjectStorageEngine
}
