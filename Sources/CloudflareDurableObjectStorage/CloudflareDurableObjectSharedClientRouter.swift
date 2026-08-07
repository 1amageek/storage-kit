import CloudflareDurableObjectStorageWire
import StorageKit

/// Routes every logical storage partition identity through one shared Durable Object client.
public struct CloudflareDurableObjectSharedClientRouter: CloudflareDurableObjectStorageRouter {
    public let client: any CloudflareDurableObjectStorageClient
    public let limits: CloudflareDurableObjectLimits
    public let monotonicClock: any StorageMonotonicClock

    public init(
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits = .default,
        monotonicClock: any StorageMonotonicClock
    ) {
        self.client = client
        self.limits = limits
        self.monotonicClock = monotonicClock
    }

    public func engine(for partitionIdentity: StoragePartitionIdentity) async throws
        -> CloudflareDurableObjectStorageEngine
    {
        try await CloudflareDurableObjectStorageEngine(
            configuration: CloudflareDurableObjectStorageConfiguration(
                partitionIdentity: partitionIdentity,
                client: client,
                limits: limits,
                monotonicClock: monotonicClock
            )
        )
    }
}
