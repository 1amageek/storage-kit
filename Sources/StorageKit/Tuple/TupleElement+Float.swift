import Foundation

// MARK: - Float

extension Float: TupleElement {
    /// IEEE 754 big-endian encoding.
    ///
    /// Positive values: flip the sign bit so lexicographic order matches numeric order.
    /// Negative values: flip all bits so negative lexicographic order is correct.
    ///
    /// FDB spec: flip sign bit for positive, flip all bits for negative.
    public func encodeTuple() -> Bytes {
        var bits = self.bitPattern.bigEndian
        var rawBytes = withUnsafeBytes(of: &bits) { Array($0) }
        if self.sign == .minus {
            // Negative values (including -0.0): flip all bits
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        } else {
            // Positive values (including +0.0, +Inf, NaN): flip sign bit only
            rawBytes[0] ^= 0x80
        }
        return [TupleTypeCode.float.rawValue] + rawBytes
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Float {
        guard offset + 4 <= bytes.count else { throw TupleError.unexpectedEndOfData }
        var rawBytes = Array(bytes[offset..<(offset + 4)])
        offset += 4

        if rawBytes[0] & 0x80 != 0 {
            // Positive value: restore sign bit only
            rawBytes[0] ^= 0x80
        } else {
            // Negative value: restore by flipping all bits back
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        }
        let bits = rawBytes.withUnsafeBytes { $0.load(as: UInt32.self) }
        return Float(bitPattern: UInt32(bigEndian: bits))
    }
}

// MARK: - Double

extension Double: TupleElement {
    /// IEEE 754 big-endian encoding (same algorithm as Float, 8 bytes).
    public func encodeTuple() -> Bytes {
        var bits = self.bitPattern.bigEndian
        var rawBytes = withUnsafeBytes(of: &bits) { Array($0) }
        if self.sign == .minus {
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        } else {
            rawBytes[0] ^= 0x80
        }
        return [TupleTypeCode.double.rawValue] + rawBytes
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Double {
        guard offset + 8 <= bytes.count else { throw TupleError.unexpectedEndOfData }
        var rawBytes = Array(bytes[offset..<(offset + 8)])
        offset += 8

        if rawBytes[0] & 0x80 != 0 {
            rawBytes[0] ^= 0x80
        } else {
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        }
        let bits = rawBytes.withUnsafeBytes { $0.load(as: UInt64.self) }
        return Double(bitPattern: UInt64(bigEndian: bits))
    }
}
