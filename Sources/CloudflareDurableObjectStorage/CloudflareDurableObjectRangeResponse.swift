/// Host range response.
public struct CloudflareDurableObjectRangeResponse: Sendable, Hashable, Codable {
    public let rows: [CloudflareDurableObjectKeyValue]
    public let nextCursor: String?
    public let currentCommitVersion: Int64
    public let conflictRange: CloudflareDurableObjectConflictRange?

    public init(
        rows: [CloudflareDurableObjectKeyValue],
        nextCursor: String? = nil,
        currentCommitVersion: Int64,
        conflictRange: CloudflareDurableObjectConflictRange? = nil
    ) {
        self.rows = rows
        self.nextCursor = nextCursor
        self.currentCommitVersion = currentCommitVersion
        self.conflictRange = conflictRange
    }
}
