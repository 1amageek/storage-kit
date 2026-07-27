import DatabaseTypes
import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedRangeRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let begin: EmbeddedRangeBoundary
    public let end: EmbeddedRangeBoundary
    public let limit: Int
    public let reverse: Bool
    public let snapshot: Bool
    public let expectedReadVersion: Int64?
    public let cursorKey: ByteString?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        begin: EmbeddedRangeBoundary,
        end: EmbeddedRangeBoundary,
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
        scope: CloudflareDurableObjectEmbeddedScope,
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
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

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        guard limit > 0, limit <= EmbeddedLimits.cloudflareDurableObject.maxRangeLimit else {
            throw .wire(.invalidRangeLimit)
        }
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedError.encode(begin, into: &writer)
        try CloudflareDurableObjectEmbeddedError.encode(end, into: &writer)
        writer.writeInt32(Int32(limit))
        writer.writeBool(reverse)
        writer.writeBool(snapshot)
        try CloudflareDurableObjectEmbeddedReadRequest.writeOptionalVersion(expectedReadVersion, into: &writer)
        try Self.writeOptionalCursorKey(cursorKey, into: &writer)
    }

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.begin = try CloudflareDurableObjectEmbeddedError.readRangeBoundary(
            from: &reader
        )
        self.end = try CloudflareDurableObjectEmbeddedError.readRangeBoundary(
            from: &reader
        )
        let limit = Int(try CloudflareDurableObjectEmbeddedError.readInt32(from: &reader))
        guard limit > 0, limit <= EmbeddedLimits.cloudflareDurableObject.maxRangeLimit else {
            throw .wire(.invalidRangeLimit)
        }
        self.limit = limit
        self.reverse = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.snapshot = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.expectedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest.readOptionalVersion(from: &reader)
        self.cursorKey = try Self.readOptionalCursorKey(from: &reader)
    }

    static func writeOptionalCursorKey(
        _ value: ByteString?,
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.writeBytes(
                value,
                maximum: EmbeddedLimits.cloudflareDurableObject.maxKeyBytes,
                into: &writer
            )
        } else {
            writer.writeBool(false)
        }
    }

    static func readOptionalCursorKey(
        from reader: inout EmbeddedWireReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> ByteString? {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        return try CloudflareDurableObjectEmbeddedError.readBytes(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxKeyBytes
        )
    }
}
