/// Configuration for one Cloudflare Durable Object storage engine.
public struct CloudflareDurableObjectStorageConfiguration: Sendable {
    public let scope: CloudflareDurableObjectStorageScope
    public let client: any CloudflareDurableObjectStorageClient
    public let nameCodec: any CloudflareDurableObjectNameCodec
    public let limits: CloudflareDurableObjectLimits

    public init(
        scope: CloudflareDurableObjectStorageScope,
        client: any CloudflareDurableObjectStorageClient,
        nameCodec: any CloudflareDurableObjectNameCodec = CloudflareDurableObjectV1NameCodec(),
        limits: CloudflareDurableObjectLimits = .default
    ) {
        self.scope = scope
        self.client = client
        self.nameCodec = nameCodec
        self.limits = limits
    }
}
