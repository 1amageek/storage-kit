import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// MARK: - Float

extension Float: TupleElement {
    /// IEEE 754 big-endian encoding.
    ///
    /// Positive values: flip the sign bit so lexicographic order matches numeric order.
    /// Negative values: flip all bits so negative lexicographic order is correct.
    ///
    /// FDB spec: flip sign bit for positive, flip all bits for negative.
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.float.rawValue)
        let encoded = sign == .minus
            ? ~bitPattern
            : bitPattern ^ 0x8000_0000
        sink.writeByte(UInt8(truncatingIfNeeded: encoded >> 24))
        sink.writeByte(UInt8(truncatingIfNeeded: encoded >> 16))
        sink.writeByte(UInt8(truncatingIfNeeded: encoded >> 8))
        sink.writeByte(UInt8(truncatingIfNeeded: encoded))
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Float {
        guard offset + 4 <= bytes.endIndex else { throw TupleError.unexpectedEndOfData }
        var encoded = UInt32(bytes[offset]) << 24
            | UInt32(bytes[offset + 1]) << 16
            | UInt32(bytes[offset + 2]) << 8
            | UInt32(bytes[offset + 3])
        offset += 4
        if encoded & 0x8000_0000 != 0 {
            encoded ^= 0x8000_0000
        } else {
            encoded = ~encoded
        }
        return Float(bitPattern: encoded)
    }
}

// MARK: - Double

extension Double: TupleElement {
    /// IEEE 754 big-endian encoding (same algorithm as Float, 8 bytes).
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.double.rawValue)
        let encoded = sign == .minus
            ? ~bitPattern
            : bitPattern ^ 0x8000_0000_0000_0000
        for shift in stride(from: 56, through: 0, by: -8) {
            sink.writeByte(UInt8(truncatingIfNeeded: encoded >> UInt64(shift)))
        }
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Double {
        guard offset + 8 <= bytes.endIndex else { throw TupleError.unexpectedEndOfData }
        var encoded: UInt64 = 0
        for index in 0..<8 {
            encoded = (encoded << 8) | UInt64(bytes[offset + index])
        }
        offset += 8
        if encoded & 0x8000_0000_0000_0000 != 0 {
            encoded ^= 0x8000_0000_0000_0000
        } else {
            encoded = ~encoded
        }
        return Double(bitPattern: encoded)
    }
}
