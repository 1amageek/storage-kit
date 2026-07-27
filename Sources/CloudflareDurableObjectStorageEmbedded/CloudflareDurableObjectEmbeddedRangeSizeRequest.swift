import DatabaseTypes
import StorageKitEmbeddedCore

/// Exact stored-byte count request for one committed key range.
public struct CloudflareDurableObjectEmbeddedRangeSizeRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let begin: ByteString
    public let end: ByteString
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        begin: ByteString,
        end: ByteString,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        try Self.validate(begin: begin, end: end)
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedError.writeBytes(
            begin,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes,
            into: &writer
        )
        try CloudflareDurableObjectEmbeddedError.writeBytes(
            end,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes,
            into: &writer
        )
        try CloudflareDurableObjectEmbeddedReadRequest.writeOptionalVersion(
            expectedReadVersion,
            into: &writer
        )
    }

    init(
        from reader: inout EmbeddedWireReader
    ) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.begin = try CloudflareDurableObjectEmbeddedError.readBytes(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.end = try CloudflareDurableObjectEmbeddedError.readBytes(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.expectedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest
            .readOptionalVersion(from: &reader)
        try Self.validate(begin: begin, end: end)
    }

    static func validate(
        begin: ByteString,
        end: ByteString
    ) throws(CloudflareDurableObjectEmbeddedError) {
        guard EmbeddedByteOrdering.compare(begin, end) <= 0 else {
            throw .wire(.invalidRangeBoundaries)
        }
    }
}
