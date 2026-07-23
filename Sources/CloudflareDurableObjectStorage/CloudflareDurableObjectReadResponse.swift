/// Host read response.
public struct CloudflareDurableObjectReadResponse: Sendable, Hashable {
    public let value: CloudflareDurableObjectBytes?
    public let currentCommitVersion: Int64

    public init(value: CloudflareDurableObjectBytes?, currentCommitVersion: Int64) {
        self.value = value
        self.currentCommitVersion = currentCommitVersion
    }
}
