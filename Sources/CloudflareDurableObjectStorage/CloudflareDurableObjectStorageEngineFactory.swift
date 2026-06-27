/// Simple engine factory for a single Cloudflare Durable Object storage client.
public struct CloudflareDurableObjectStorageEngineFactory: CloudflareDurableObjectStorageRouter {
    public let client: any CloudflareDurableObjectStorageClient
    public let nameCodec: any CloudflareDurableObjectNameCodec
    public let limits: CloudflareDurableObjectLimits

    public init(
        client: any CloudflareDurableObjectStorageClient,
        nameCodec: any CloudflareDurableObjectNameCodec = CloudflareDurableObjectV1NameCodec(),
        limits: CloudflareDurableObjectLimits = .default
    ) {
        self.client = client
        self.nameCodec = nameCodec
        self.limits = limits
    }

    public func engine(for scope: CloudflareDurableObjectStorageScope) async throws -> CloudflareDurableObjectStorageEngine {
        try await CloudflareDurableObjectStorageEngine(
            configuration: CloudflareDurableObjectStorageConfiguration(
                scope: scope,
                client: client,
                nameCodec: nameCodec,
                limits: limits
            )
        )
    }
}
