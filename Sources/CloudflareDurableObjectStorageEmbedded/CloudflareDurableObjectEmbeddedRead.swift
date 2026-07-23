import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedReadRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let key: EmbeddedBytes
    public let snapshot: Bool
    public let expectedReadVersion: Int64?

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        key: EmbeddedBytes,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.key = key
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedError.writeBytes(
            key,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxKeyBytes,
            into: &writer
        )
        writer.writeBool(snapshot)
        try Self.writeOptionalVersion(expectedReadVersion, into: &writer)
    }

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.key = try CloudflareDurableObjectEmbeddedError.readBytes(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxKeyBytes
        )
        self.snapshot = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        self.expectedReadVersion = try Self.readOptionalVersion(from: &reader)
    }
}

extension CloudflareDurableObjectEmbeddedReadRequest {
    static func writeOptionalVersion(
        _ value: Int64?,
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            guard value >= 0 else {
                throw .invalidVersion(value)
            }
            writer.writeBool(true)
            writer.writeInt64(value)
        } else {
            writer.writeBool(false)
        }
    }

    static func readOptionalVersion(
        from reader: inout EmbeddedWireReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> Int64? {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        let value = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
        guard value >= 0 else {
            throw .invalidVersion(value)
        }
        return value
    }
}
