import StorageKit

/// Exact stored-byte count request for one committed key range.
public struct CloudflareDurableObjectRangeSizeRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectStorageScope
    public let begin: CloudflareDurableObjectBytes
    public let end: CloudflareDurableObjectBytes
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectStorageScope,
        begin: CloudflareDurableObjectBytes,
        end: CloudflareDurableObjectBytes,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.expectedReadVersion = expectedReadVersion
    }
}
