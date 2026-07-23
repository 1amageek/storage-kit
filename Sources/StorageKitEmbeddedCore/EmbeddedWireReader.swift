/// Bounded little-endian reader for StorageKit Wire messages.
public struct EmbeddedWireReader: Sendable {
    private let bytes: EmbeddedBytes
    private var offset: Int

    public init(_ bytes: [UInt8]) {
        self.bytes = EmbeddedBytes(bytes)
        self.offset = 0
    }

    public init(_ bytes: EmbeddedBytes) {
        self.bytes = bytes
        self.offset = 0
    }

    public var remainingCount: Int {
        bytes.count - offset
    }

    public mutating func readUInt8() throws(EmbeddedWireError) -> UInt8 {
        guard offset < bytes.count else {
            throw EmbeddedWireError.truncated
        }
        let value = bytes[offset]
        offset += 1
        return value
    }

    public mutating func readBool() throws(EmbeddedWireError) -> Bool {
        let rawValue = try readUInt8()
        switch rawValue {
        case 0:
            return false
        case 1:
            return true
        default:
            throw EmbeddedWireError.invalidBool(rawValue)
        }
    }

    public mutating func readUInt32() throws(EmbeddedWireError) -> UInt32 {
        guard bytes.count - offset >= 4 else {
            throw EmbeddedWireError.truncated
        }
        let value = bytes.withUnsafeBytes { storage in
            UInt32(storage[offset])
                | (UInt32(storage[offset + 1]) << 8)
                | (UInt32(storage[offset + 2]) << 16)
                | (UInt32(storage[offset + 3]) << 24)
        }
        offset += 4
        return value
    }

    public mutating func readInt32() throws(EmbeddedWireError) -> Int32 {
        Int32(bitPattern: try readUInt32())
    }

    public mutating func readUInt64() throws(EmbeddedWireError) -> UInt64 {
        guard bytes.count - offset >= 8 else {
            throw EmbeddedWireError.truncated
        }
        let value = bytes.withUnsafeBytes { storage in
            UInt64(storage[offset])
                | (UInt64(storage[offset + 1]) << 8)
                | (UInt64(storage[offset + 2]) << 16)
                | (UInt64(storage[offset + 3]) << 24)
                | (UInt64(storage[offset + 4]) << 32)
                | (UInt64(storage[offset + 5]) << 40)
                | (UInt64(storage[offset + 6]) << 48)
                | (UInt64(storage[offset + 7]) << 56)
        }
        offset += 8
        return value
    }

    public mutating func readInt64() throws(EmbeddedWireError) -> Int64 {
        Int64(bitPattern: try readUInt64())
    }

    public mutating func readByteRegion() throws(EmbeddedWireError) -> EmbeddedBytes {
        try readByteRegion(maximum: Int.max)
    }

    public mutating func readByteRegion(
        maximum: Int
    ) throws(EmbeddedWireError) -> EmbeddedBytes {
        let intCount = try readCount()
        guard intCount <= maximum else {
            throw EmbeddedWireError.byteCountExceedsLimit(
                count: intCount,
                maximum: maximum
            )
        }
        guard intCount <= remainingCount else {
            throw EmbeddedWireError.truncated
        }
        let value = bytes.slice(offset..<(offset + intCount))
        offset += intCount
        return value
    }

    public mutating func readString() throws(EmbeddedWireError) -> String {
        try readString(maximum: Int.max)
    }

    public mutating func readString(maximum: Int) throws(EmbeddedWireError) -> String {
        let bytes = try readByteRegion(maximum: maximum)
        guard let value = String(validating: bytes, as: UTF8.self) else {
            throw EmbeddedWireError.invalidUTF8
        }
        return value
    }

    public mutating func readCount() throws(EmbeddedWireError) -> Int {
        let count = try readUInt32()
        guard UInt64(count) <= UInt64(Int.max) else {
            throw EmbeddedWireError.byteCountOverflow
        }
        return Int(count)
    }

    public func ensureFullyRead() throws(EmbeddedWireError) {
        guard remainingCount == 0 else {
            throw EmbeddedWireError.trailingBytes
        }
    }
}
