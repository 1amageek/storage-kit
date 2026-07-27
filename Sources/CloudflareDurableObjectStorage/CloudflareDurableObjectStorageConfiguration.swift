import CloudflareDurableObjectStorageWire
import StorageKit

/// Configuration for one Cloudflare Durable Object storage engine.
public struct CloudflareDurableObjectStorageConfiguration: Sendable {
    public let scope: StorageWireScope
    public let client: any CloudflareDurableObjectStorageClient
    public let limits: CloudflareDurableObjectLimits
    public let monotonicClock: any StorageMonotonicClock

    public init(
        scope: StorageWireScope,
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits = .default,
        monotonicClock: any StorageMonotonicClock = SystemStorageClock()
    ) {
        self.scope = scope
        self.client = client
        self.limits = limits
        self.monotonicClock = monotonicClock
    }
}
