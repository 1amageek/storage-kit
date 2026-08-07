import CloudflareDurableObjectStorageWire

/// Routes logical partition identities to Cloudflare Durable Object storage engines.
public protocol CloudflareDurableObjectStorageRouter: Sendable {
    func engine(for partitionIdentity: StoragePartitionIdentity) async throws -> CloudflareDurableObjectStorageEngine
}
