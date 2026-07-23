/// Exact stored-byte count response for one committed key range.
public struct CloudflareDurableObjectRangeSizeResponse: Sendable, Hashable {
    public let byteCount: Int64
    public let currentCommitVersion: Int64

    public init(byteCount: Int64, currentCommitVersion: Int64) {
        self.byteCount = byteCount
        self.currentCommitVersion = currentCommitVersion
    }
}
