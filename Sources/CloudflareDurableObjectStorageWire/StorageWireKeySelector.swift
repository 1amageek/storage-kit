import DatabaseTypes

/// Key-selector representation carried by the Cloudflare storage protocol.
public struct StorageWireKeySelector: Sendable, Hashable {
    public enum Kind: UInt8, Sendable, Hashable {
        case firstGreaterOrEqual = 1
        case firstGreaterThan = 2
        case lastLessOrEqual = 3
        case lastLessThan = 4
    }

    public let key: ByteString
    public let orEqual: Bool
    public let offset: Int

    public init(key: ByteString, kind: Kind) {
        self.key = key
        switch kind {
        case .firstGreaterOrEqual:
            self.orEqual = false
            self.offset = 1
        case .firstGreaterThan:
            self.orEqual = true
            self.offset = 1
        case .lastLessOrEqual:
            self.orEqual = true
            self.offset = 0
        case .lastLessThan:
            self.orEqual = false
            self.offset = 0
        }
    }

    public init(key: ByteString, orEqual: Bool, offset: Int) {
        self.key = key
        self.orEqual = orEqual
        self.offset = offset
    }

    public func encode(into writer: inout StorageWireWriter) throws(StorageWireError) {
        try writer.writeBytes(key)
        writer.writeBool(orEqual)
        guard let encodedOffset = Int64(exactly: offset) else {
            throw StorageWireError.keySelectorOffsetOverflow
        }
        writer.writeInt64(encodedOffset)
    }

    public init(from reader: inout StorageWireReader) throws(StorageWireError) {
        self.key = try reader.readByteRegion()
        self.orEqual = try reader.readBool()
        let encodedOffset = try reader.readInt64()
        guard let offset = Int(exactly: encodedOffset) else {
            throw StorageWireError.keySelectorOffsetOverflow
        }
        self.offset = offset
    }

}
