import CloudflareDurableObjectStorageWire
import StorageKit

/// Routes every logical storage scope through one shared Durable Object client.
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

    public func engine(for scope: StorageWireScope) async throws
        -> CloudflareDurableObjectStorageEngine
    {
        try await CloudflareDurableObjectStorageEngine(
            configuration: CloudflareDurableObjectStorageConfiguration(
                scope: scope,
                client: client,
                limits: limits,
                monotonicClock: monotonicClock
            )
        )
    }
}
