import DatabaseTypes

public struct StorageWireRangeRequest: Sendable, Hashable {
    public let scope: StorageWireScope
    public let begin: StorageWireRangeBoundary
    public let end: StorageWireRangeBoundary
    public let limit: Int
    public let reverse: Bool
    public let snapshot: Bool
    public let expectedReadVersion: Int64?
    public let cursorKey: ByteString?

    public init(
        scope: StorageWireScope,
        begin: StorageWireRangeBoundary,
        end: StorageWireRangeBoundary,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursorKey: ByteString? = nil
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
        scope: StorageWireScope,
        begin: StorageWireKeySelector,
        end: StorageWireKeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursorKey: ByteString? = nil
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

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        guard limit > 0, limit <= StorageWireLimits.cloudflareDurableObject.maxRangeLimit else {
            throw .wire(.invalidRangeLimit)
        }
        try scope.encode(into: &writer)
        try StorageWireProtocolError.encode(begin, into: &writer)
        try StorageWireProtocolError.encode(end, into: &writer)
        writer.writeInt32(Int32(limit))
        writer.writeBool(reverse)
        writer.writeBool(snapshot)
        try StorageWireReadRequest.writeOptionalVersion(expectedReadVersion, into: &writer)
        try Self.writeOptionalCursorKey(cursorKey, into: &writer)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.scope = try StorageWireScope(from: &reader)
        self.begin = try StorageWireProtocolError.readRangeBoundary(
            from: &reader
        )
        self.end = try StorageWireProtocolError.readRangeBoundary(
            from: &reader
        )
        let limit = Int(try StorageWireProtocolError.readInt32(from: &reader))
        guard limit > 0, limit <= StorageWireLimits.cloudflareDurableObject.maxRangeLimit else {
            throw .wire(.invalidRangeLimit)
        }
        self.limit = limit
        self.reverse = try StorageWireProtocolError.readBool(from: &reader)
        self.snapshot = try StorageWireProtocolError.readBool(from: &reader)
        self.expectedReadVersion = try StorageWireReadRequest.readOptionalVersion(from: &reader)
        self.cursorKey = try Self.readOptionalCursorKey(from: &reader)
    }

    static func writeOptionalCursorKey(
        _ value: ByteString?,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        if let value {
            writer.writeBool(true)
            try StorageWireProtocolError.writeBytes(
                value,
                maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes,
                into: &writer
            )
        } else {
            writer.writeBool(false)
        }
    }

    static func readOptionalCursorKey(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> ByteString? {
        let hasValue = try StorageWireProtocolError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        return try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes
        )
    }
}
