/// Host range response.
public struct CloudflareDurableObjectRangeResponse: Sendable, Hashable {
    public let rows: [CloudflareDurableObjectKeyValue]
    public let hasMore: Bool
    public let currentCommitVersion: Int64
    public let readConflictRanges: [CloudflareDurableObjectConflictRange]

    public init(
        rows: [CloudflareDurableObjectKeyValue],
        hasMore: Bool,
        currentCommitVersion: Int64,
        readConflictRanges: [CloudflareDurableObjectConflictRange] = []
    ) {
        self.rows = rows
        self.hasMore = hasMore
        self.currentCommitVersion = currentCommitVersion
        self.readConflictRanges = readConflictRanges
    }
}
