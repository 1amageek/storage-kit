import CloudflareDurableObjectStorageWire
import StorageKit

/// Configuration for one Cloudflare Durable Object storage engine.
public struct CloudflareDurableObjectStorageConfiguration: Sendable {
    public let partitionIdentity: StoragePartitionIdentity
    public let client: any CloudflareDurableObjectStorageClient
    public let limits: CloudflareDurableObjectLimits
    public let monotonicClock: any StorageMonotonicClock

    public init(
        partitionIdentity: StoragePartitionIdentity,
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits = .default,
        monotonicClock: any StorageMonotonicClock
    ) {
        self.partitionIdentity = partitionIdentity
        self.client = client
        self.limits = limits
        self.monotonicClock = monotonicClock
    }
}
