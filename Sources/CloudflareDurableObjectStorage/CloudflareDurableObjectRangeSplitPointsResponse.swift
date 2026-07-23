/// Ordered range boundaries returned for server-side chunk planning.
public struct CloudflareDurableObjectRangeSplitPointsResponse: Sendable, Hashable {
    public let splitPoints: [CloudflareDurableObjectBytes]
    public let currentCommitVersion: Int64

    public init(
        splitPoints: [CloudflareDurableObjectBytes],
        currentCommitVersion: Int64
    ) {
        self.splitPoints = splitPoints
        self.currentCommitVersion = currentCommitVersion
    }
}
