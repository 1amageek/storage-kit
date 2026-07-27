import CloudflareDurableObjectStorageWire

/// Routes every logical storage scope through one shared Durable Object client.
public struct CloudflareDurableObjectSharedClientRouter: CloudflareDurableObjectStorageRouter {
    public let client: any CloudflareDurableObjectStorageClient
    public let limits: CloudflareDurableObjectLimits

    public init(
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits = .default
    ) {
        self.client = client
        self.limits = limits
    }

    public func engine(for scope: StorageWireScope) async throws
        -> CloudflareDurableObjectStorageEngine
    {
        try await CloudflareDurableObjectStorageEngine(
            configuration: CloudflareDurableObjectStorageConfiguration(
                scope: scope,
                client: client,
                limits: limits
            )
        )
    }
}
