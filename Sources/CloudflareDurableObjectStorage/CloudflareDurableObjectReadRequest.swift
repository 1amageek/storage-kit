/// Host read request.
public struct CloudflareDurableObjectReadRequest: Sendable, Hashable, Codable {
    public let scope: CloudflareDurableObjectStorageScope
    public let key: CloudflareDurableObjectBytes
    public let snapshot: Bool
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectStorageScope,
        key: CloudflareDurableObjectBytes,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.key = key
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
    }
}
