/// Host commit response.
public struct CloudflareDurableObjectCommitResponse: Sendable, Hashable, Codable {
    public let committedVersion: Int64

    public init(committedVersion: Int64) {
        self.committedVersion = committedVersion
    }
}
