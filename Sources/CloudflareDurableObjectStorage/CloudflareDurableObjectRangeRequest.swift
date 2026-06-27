/// Host range request.
public struct CloudflareDurableObjectRangeRequest: Sendable, Hashable, Codable {
    public let scope: CloudflareDurableObjectStorageScope
    public let begin: CloudflareDurableObjectKeySelector
    public let end: CloudflareDurableObjectKeySelector
    public let limit: Int
    public let reverse: Bool
    public let snapshot: Bool
    public let expectedReadVersion: Int64?
    public let cursor: String?

    public init(
        scope: CloudflareDurableObjectStorageScope,
        begin: CloudflareDurableObjectKeySelector,
        end: CloudflareDurableObjectKeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursor: String? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.limit = limit
        self.reverse = reverse
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
        self.cursor = cursor
    }
}
