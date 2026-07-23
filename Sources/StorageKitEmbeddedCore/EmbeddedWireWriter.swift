/// Bounded little-endian writer for StorageKit Wire messages.
public struct EmbeddedWireWriter: Sendable {
    public private(set) var bytes: [UInt8]
    private let isMeasuring: Bool
    private var measuredByteCount: Int

    public init() {
        self.bytes = []
        self.isMeasuring = false
        self.measuredByteCount = 0
    }

    public init(capacity: Int) {
        self.bytes = []
        self.bytes.reserveCapacity(capacity)
        self.isMeasuring = false
        self.measuredByteCount = 0
    }

    private init(measuring: Void) {
        self.bytes = []
        self.isMeasuring = true
        self.measuredByteCount = 0
    }

    public var writtenByteCount: Int {
        isMeasuring ? measuredByteCount : bytes.count
    }

    public static func encode<Failure: Error>(
        _ encode: (inout EmbeddedWireWriter) throws(Failure) -> Void
    ) throws(Failure) -> EmbeddedBytes {
        var measuringWriter = EmbeddedWireWriter(measuring: ())
        try encode(&measuringWriter)
        let byteCount = measuringWriter.writtenByteCount
        var writer = EmbeddedWireWriter(capacity: byteCount)
        try encode(&writer)
        precondition(
            writer.writtenByteCount == byteCount,
            "StorageKit Wire encoding changed between sizing and writing"
        )
        return EmbeddedBytes(exactBytes: writer.bytes)
    }

    public mutating func writeUInt8(_ value: UInt8) {
        if isMeasuring {
            recordMeasuredBytes(1)
        } else {
            bytes.append(value)
        }
    }

    public mutating func writeBool(_ value: Bool) {
        writeUInt8(value ? 1 : 0)
    }

    public mutating func writeUInt32(_ value: UInt32) {
        if isMeasuring {
            recordMeasuredBytes(4)
            return
        }
        bytes.append(UInt8(truncatingIfNeeded: value))
        bytes.append(UInt8(truncatingIfNeeded: value >> 8))
        bytes.append(UInt8(truncatingIfNeeded: value >> 16))
        bytes.append(UInt8(truncatingIfNeeded: value >> 24))
    }

    public mutating func writeInt32(_ value: Int32) {
        writeUInt32(UInt32(bitPattern: value))
    }

    public mutating func writeUInt64(_ value: UInt64) {
        if isMeasuring {
            recordMeasuredBytes(8)
            return
        }
        bytes.append(UInt8(truncatingIfNeeded: value))
        bytes.append(UInt8(truncatingIfNeeded: value >> 8))
        bytes.append(UInt8(truncatingIfNeeded: value >> 16))
        bytes.append(UInt8(truncatingIfNeeded: value >> 24))
        bytes.append(UInt8(truncatingIfNeeded: value >> 32))
        bytes.append(UInt8(truncatingIfNeeded: value >> 40))
        bytes.append(UInt8(truncatingIfNeeded: value >> 48))
        bytes.append(UInt8(truncatingIfNeeded: value >> 56))
    }

    public mutating func writeInt64(_ value: Int64) {
        writeUInt64(UInt64(bitPattern: value))
    }

    public mutating func writeBytes(
        _ value: [UInt8]
    ) throws(EmbeddedWireError) {
        try writeBytes(EmbeddedBytes(value))
    }

    public mutating func writeBytes(
        _ value: EmbeddedBytes
    ) throws(EmbeddedWireError) {
        try writeCount(value.count)
        if isMeasuring {
            recordMeasuredBytes(value.count)
        } else {
            value.withUnsafeBytes {
                bytes.append(contentsOf: $0)
            }
        }
    }

    public mutating func writeString(
        _ value: String
    ) throws(EmbeddedWireError) {
        try writeCount(value.utf8.count)
        if isMeasuring {
            recordMeasuredBytes(value.utf8.count)
        } else {
            bytes.append(contentsOf: value.utf8)
        }
    }

    public mutating func writeCount(
        _ count: Int
    ) throws(EmbeddedWireError) {
        guard count >= 0, UInt64(count) <= UInt64(UInt32.max) else {
            throw EmbeddedWireError.byteCountOverflow
        }
        writeUInt32(UInt32(count))
    }

    private mutating func recordMeasuredBytes(_ count: Int) {
        let result = measuredByteCount.addingReportingOverflow(count)
        precondition(
            !result.overflow && count >= 0,
            "StorageKit Wire byte count overflow"
        )
        measuredByteCount = result.partialValue
    }
}
