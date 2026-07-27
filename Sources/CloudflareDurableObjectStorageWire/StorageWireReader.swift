import DatabaseTypes
/// Bounded little-endian reader for StorageKit Wire messages.
public struct StorageWireReader: Sendable {
    private let bytes: ByteString
    private var offset: Int

    public init(_ bytes: [UInt8]) {
        self.bytes = ByteString(bytes)
        self.offset = 0
    }

    public init(_ bytes: ByteString) {
        self.bytes = bytes
        self.offset = bytes.startIndex
    }

    public var remainingCount: Int {
        bytes.endIndex - offset
    }

    public mutating func readUInt8() throws(StorageWireError) -> UInt8 {
        guard offset < bytes.endIndex else {
            throw StorageWireError.truncated
        }
        let value = bytes[offset]
        offset += 1
        return value
    }

    public mutating func readBool() throws(StorageWireError) -> Bool {
        let rawValue = try readUInt8()
        switch rawValue {
        case 0:
            return false
        case 1:
            return true
        default:
            throw StorageWireError.invalidBool(rawValue)
        }
    }

    public mutating func readUInt32() throws(StorageWireError) -> UInt32 {
        guard bytes.endIndex - offset >= 4 else {
            throw StorageWireError.truncated
        }
        let value = bytes.withUnsafeBytes { storage in
            let relativeOffset = offset - bytes.startIndex
            return UInt32(storage[relativeOffset])
                | (UInt32(storage[relativeOffset + 1]) << 8)
                | (UInt32(storage[relativeOffset + 2]) << 16)
                | (UInt32(storage[relativeOffset + 3]) << 24)
        }
        offset += 4
        return value
    }

    public mutating func readInt32() throws(StorageWireError) -> Int32 {
        Int32(bitPattern: try readUInt32())
    }

    public mutating func readUInt64() throws(StorageWireError) -> UInt64 {
        guard bytes.endIndex - offset >= 8 else {
            throw StorageWireError.truncated
        }
        let value = bytes.withUnsafeBytes { storage in
            let relativeOffset = offset - bytes.startIndex
            return UInt64(storage[relativeOffset])
                | (UInt64(storage[relativeOffset + 1]) << 8)
                | (UInt64(storage[relativeOffset + 2]) << 16)
                | (UInt64(storage[relativeOffset + 3]) << 24)
                | (UInt64(storage[relativeOffset + 4]) << 32)
                | (UInt64(storage[relativeOffset + 5]) << 40)
                | (UInt64(storage[relativeOffset + 6]) << 48)
                | (UInt64(storage[relativeOffset + 7]) << 56)
        }
        offset += 8
        return value
    }

    public mutating func readInt64() throws(StorageWireError) -> Int64 {
        Int64(bitPattern: try readUInt64())
    }

    public mutating func readByteRegion() throws(StorageWireError) -> ByteString {
        try readByteRegion(maximum: Int.max)
    }

    public mutating func readByteRegion(
        maximum: Int
    ) throws(StorageWireError) -> ByteString {
        let intCount = try readCount()
        guard intCount <= maximum else {
            throw StorageWireError.byteCountExceedsLimit(
                count: intCount,
                maximum: maximum
            )
        }
        guard intCount <= remainingCount else {
            throw StorageWireError.truncated
        }
        let value = bytes[offset..<(offset + intCount)]
        offset += intCount
        return value
    }

    public mutating func readString() throws(StorageWireError) -> String {
        try readString(maximum: Int.max)
    }

    public mutating func readString(maximum: Int) throws(StorageWireError) -> String {
        let bytes = try readByteRegion(maximum: maximum)
        guard let value = String(validating: bytes, as: UTF8.self) else {
            throw StorageWireError.invalidUTF8
        }
        return value
    }

    public mutating func readCount() throws(StorageWireError) -> Int {
        let count = try readUInt32()
        guard UInt64(count) <= UInt64(Int.max) else {
            throw StorageWireError.byteCountOverflow
        }
        return Int(count)
    }

    public func ensureFullyRead() throws(StorageWireError) {
        guard remainingCount == 0 else {
            throw StorageWireError.trailingBytes
        }
    }
}
