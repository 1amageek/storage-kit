import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedReadRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let key: [UInt8]
    public let snapshot: Bool
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        key: [UInt8],
        snapshot: Bool,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.key = key
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedError.writeBytes(key, into: &writer)
        writer.writeBool(snapshot)
        try Self.writeOptionalVersion(expectedReadVersion, into: &writer)
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.key = try CloudflareDurableObjectEmbeddedError.readBytes(from: &reader)
        self.snapshot = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.expectedReadVersion = try Self.readOptionalVersion(from: &reader)
    }
}

extension CloudflareDurableObjectEmbeddedReadRequest {
    static func writeOptionalVersion(
        _ value: Int64?,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            writer.writeInt64(value)
        } else {
            writer.writeBool(false)
        }
    }

    static func readOptionalVersion(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> Int64? {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        return try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
    }
}
