/// Split-point request for one committed key range.
public struct CloudflareDurableObjectRangeSplitPointsRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectStorageScope
    public let begin: CloudflareDurableObjectBytes
    public let end: CloudflareDurableObjectBytes
    public let chunkSize: Int64
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectStorageScope,
        begin: CloudflareDurableObjectBytes,
        end: CloudflareDurableObjectBytes,
        chunkSize: Int64,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.chunkSize = chunkSize
        self.expectedReadVersion = expectedReadVersion
    }
}
