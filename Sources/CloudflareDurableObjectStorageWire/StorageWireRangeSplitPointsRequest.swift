import DatabaseTypes

/// Server-side chunk-boundary request for one committed key range.
public struct StorageWireRangeSplitPointsRequest: Sendable, Hashable {
    public let partitionIdentity: StoragePartitionIdentity
    public let begin: ByteString
    public let end: ByteString
    public let chunkSize: Int64
    public let expectedReadVersion: Int64?

    public init(
        partitionIdentity: StoragePartitionIdentity,
        begin: ByteString,
        end: ByteString,
        chunkSize: Int64,
        expectedReadVersion: Int64? = nil
    ) {
        self.partitionIdentity = partitionIdentity
        self.begin = begin
        self.end = end
        self.chunkSize = chunkSize
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try StorageWireRangeSizeRequest.validate(
            begin: begin,
            end: end
        )
        guard chunkSize > 0 else {
            throw .wire(.invalidChunkSize(chunkSize))
        }
        try partitionIdentity.encode(into: &writer)
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
        writer.writeInt64(chunkSize)
        try StorageWireReadRequest.writeOptionalVersion(
            expectedReadVersion,
            into: &writer
        )
    }

    init(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) {
        self.partitionIdentity = try StoragePartitionIdentity(from: &reader)
        self.begin = try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.end = try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
        )
        self.chunkSize = try StorageWireProtocolError.readInt64(
            from: &reader
        )
        self.expectedReadVersion = try StorageWireReadRequest
            .readOptionalVersion(from: &reader)
        try StorageWireRangeSizeRequest.validate(
            begin: begin,
            end: end
        )
        guard chunkSize > 0 else {
            throw .wire(.invalidChunkSize(chunkSize))
        }
    }
}
