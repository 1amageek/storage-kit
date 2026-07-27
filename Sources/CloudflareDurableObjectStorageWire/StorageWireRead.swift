import DatabaseTypes

public struct StorageWireReadRequest: Sendable, Hashable {
    public let scope: StorageWireScope
    public let key: ByteString
    public let snapshot: Bool
    public let expectedReadVersion: Int64?

    public init(
        scope: StorageWireScope,
        key: ByteString,
        snapshot: Bool,
        expectedReadVersion: Int64? = nil
    ) {
        self.scope = scope
        self.key = key
        self.snapshot = snapshot
        self.expectedReadVersion = expectedReadVersion
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        try scope.encode(into: &writer)
        try StorageWireProtocolError.writeBytes(
            key,
            maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes,
            into: &writer
        )
        writer.writeBool(snapshot)
        try Self.writeOptionalVersion(expectedReadVersion, into: &writer)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.scope = try StorageWireScope(from: &reader)
        self.key = try StorageWireProtocolError.readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes
        )
        self.snapshot = try StorageWireProtocolError.readBool(from: &reader)
        self.expectedReadVersion = try Self.readOptionalVersion(from: &reader)
    }
}

extension StorageWireReadRequest {
    static func writeOptionalVersion(
        _ value: Int64?,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
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
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> Int64? {
        let hasValue = try StorageWireProtocolError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        let value = try StorageWireProtocolError.readInt64(from: &reader)
        guard value >= 0 else {
            throw .invalidVersion(value)
        }
        return value
    }
}
