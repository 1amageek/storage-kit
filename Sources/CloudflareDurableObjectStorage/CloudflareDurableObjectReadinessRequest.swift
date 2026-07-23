/// Host readiness request.
public struct CloudflareDurableObjectReadinessRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectStorageScope

    public init(scope: CloudflareDurableObjectStorageScope) {
        self.scope = scope
    }
}
