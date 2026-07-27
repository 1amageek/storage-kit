import DatabaseTypes

/// Exact stored-byte count request for one committed key range.
public struct StorageWireRangeSizeRequest: Sendable, Hashable {
    public let scope: StorageWireScope
    public let begin: ByteString
    public let end: ByteString
    public let expectedReadVersion: Int64?

    public init(
        scope: StorageWireScope,
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
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try Self.validate(begin: begin, end: end)
        try scope.encode(into: &writer)
        try StorageWireProtocolError.writeBytes(
            begin,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes,
            into: &writer
        )
        try StorageWireProtocolError.writeBytes(
            end,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes,
            into: &writer
        )
        try StorageWireReadRequest.writeOptionalVersion(
            expectedReadVersion,
            into: &writer
        )
    }

    init(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) {
        self.scope = try StorageWireScope(from: &reader)
        self.begin = try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.end = try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.expectedReadVersion = try StorageWireReadRequest
            .readOptionalVersion(from: &reader)
        try Self.validate(begin: begin, end: end)
    }

    static func validate(
        begin: ByteString,
        end: ByteString
    ) throws(StorageWireProtocolError) {
        guard StorageWireByteOrdering.compare(begin, end) <= 0 else {
            throw .wire(.invalidRangeBoundaries)
        }
    }
}
