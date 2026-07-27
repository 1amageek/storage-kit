import DatabaseTypes
import StorageKitEmbeddedCore

/// Server-side chunk-boundary request for one committed key range.
public struct CloudflareDurableObjectEmbeddedRangeSplitPointsRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let begin: ByteString
    public let end: ByteString
    public let chunkSize: Int64
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        begin: ByteString,
        end: ByteString,
        chunkSize: Int64,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.chunkSize = chunkSize
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        try CloudflareDurableObjectEmbeddedRangeSizeRequest.validate(
            begin: begin,
            end: end
        )
        guard chunkSize > 0 else {
            throw .wire(.invalidChunkSize(chunkSize))
        }
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
        writer.writeInt64(chunkSize)
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
        self.chunkSize = try CloudflareDurableObjectEmbeddedError.readInt64(
            from: &reader
        )
        self.expectedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest
            .readOptionalVersion(from: &reader)
        try CloudflareDurableObjectEmbeddedRangeSizeRequest.validate(
            begin: begin,
            end: end
        )
        guard chunkSize > 0 else {
            throw .wire(.invalidChunkSize(chunkSize))
        }
    }
}
