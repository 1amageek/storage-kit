/// Host range request.
public struct CloudflareDurableObjectRangeRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectStorageScope
    public let begin: CloudflareDurableObjectRangeBoundary
    public let end: CloudflareDurableObjectRangeBoundary
    public let limit: Int
    public let reverse: Bool
    public let snapshot: Bool
    public let expectedReadVersion: Int64?
    public let cursorKey: CloudflareDurableObjectBytes?

    public init(
        scope: CloudflareDurableObjectStorageScope,
        begin: CloudflareDurableObjectRangeBoundary,
        end: CloudflareDurableObjectRangeBoundary,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursorKey: CloudflareDurableObjectBytes? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.limit = limit
        self.reverse = reverse
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
        self.cursorKey = cursorKey
    }

    public init(
        scope: CloudflareDurableObjectStorageScope,
        begin: CloudflareDurableObjectKeySelector,
        end: CloudflareDurableObjectKeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursorKey: CloudflareDurableObjectBytes? = nil
    ) {
        self.init(
            scope: scope,
            begin: .selector(begin),
            end: .selector(end),
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            expectedReadVersion: expectedReadVersion,
            cursorKey: cursorKey
        )
    }
}
