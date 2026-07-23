/// Host commit response.
public struct CloudflareDurableObjectCommitResponse: Sendable, Hashable {
    public let committedVersion: Int64

    public init(committedVersion: Int64) {
        self.committedVersion = committedVersion
    }
}
