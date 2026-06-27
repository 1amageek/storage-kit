import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedRangeRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let begin: EmbeddedKeySelector
    public let end: EmbeddedKeySelector
    public let limit: Int
    public let reverse: Bool
    public let snapshot: Bool
    public let expectedReadVersion: Int64?
    public let cursor: String?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil,
        cursor: String? = nil
    ) {
        self.scope = scope
        self.begin = begin
        self.end = end
        self.limit = limit
        self.reverse = reverse
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
        self.cursor = cursor
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedError.encode(begin, into: &writer)
        try CloudflareDurableObjectEmbeddedError.encode(end, into: &writer)
        writer.writeInt32(Int32(limit))
        writer.writeBool(reverse)
        writer.writeBool(snapshot)
        try CloudflareDurableObjectEmbeddedReadRequest.writeOptionalVersion(expectedReadVersion, into: &writer)
        try Self.writeOptionalString(cursor, into: &writer)
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.begin = try CloudflareDurableObjectEmbeddedError.readKeySelector(from: &reader)
        self.end = try CloudflareDurableObjectEmbeddedError.readKeySelector(from: &reader)
        self.limit = Int(try CloudflareDurableObjectEmbeddedError.readInt32(from: &reader))
        self.reverse = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.snapshot = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.expectedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest.readOptionalVersion(from: &reader)
        self.cursor = try Self.readOptionalString(from: &reader)
    }

    static func writeOptionalString(
        _ value: String?,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.writeString(value, into: &writer)
        } else {
            writer.writeBool(false)
        }
    }

    static func readOptionalString(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> String? {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        return try CloudflareDurableObjectEmbeddedError.readString(from: &reader)
    }
}
