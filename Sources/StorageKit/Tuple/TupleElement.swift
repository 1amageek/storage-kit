import DatabaseTypes
/// Type codes fully compliant with the FDB Tuple Layer binary encoding specification.
///
/// Reference: https://github.com/apple/foundationdb/blob/main/design/tuple.md
public enum TupleTypeCode: UInt8, Sendable {
    case null           = 0x00
    case bytes          = 0x01
    case string         = 0x02
    case nested         = 0x05
    // 0x0B - 0x13: negative integers (variable length)
    case negativeInt8   = 0x0C
    case negativeInt7   = 0x0D
    case negativeInt6   = 0x0E
    case negativeInt5   = 0x0F
    case negativeInt4   = 0x10
    case negativeInt3   = 0x11
    case negativeInt2   = 0x12
    case negativeInt1   = 0x13
    case intZero        = 0x14
    case positiveInt1   = 0x15
    case positiveInt2   = 0x16
    case positiveInt3   = 0x17
    case positiveInt4   = 0x18
    case positiveInt5   = 0x19
    case positiveInt6   = 0x1A
    case positiveInt7   = 0x1B
    case positiveInt8   = 0x1C
    // 0x1D is positiveInt9 (for full UInt64 range)
    case float          = 0x20
    case double         = 0x21
    case boolFalse      = 0x26
    case boolTrue       = 0x27
    case uuid           = 0x30
    case versionstamp   = 0x33
}

/// strinc algorithm: returns the next prefix in lexicographic order.
///
/// Strips trailing 0xFF bytes and increments the last byte.
/// Used for generating end keys in range scans.
///
/// Reference: FoundationDB strinc specification
public func strinc(_ bytes: ByteString) throws -> ByteString {
    var resultCount = bytes.count
    bytes.withUnsafeBytes { source in
        while resultCount > 0, source[resultCount - 1] == 0xFF {
            resultCount -= 1
        }
    }
    guard resultCount > 0 else {
        throw TupleError.cannotIncrementKey
    }
    return ByteString.copying(count: resultCount) { destination in
        bytes.withUnsafeBytes { source in
            destination.copyMemory(
                from: UnsafeRawBufferPointer(
                    rebasing: source[0..<resultCount]
                )
            )
        }
        destination[resultCount - 1] &+= 1
    }
}

/// Protocol for encoding/decoding with the Tuple Layer.
///
/// Converts each type to/from byte arrays following the FDB Tuple Layer binary format.
/// The encoded result preserves lexicographic order matching the logical order of values.
public protocol TupleElement: Sendable, Hashable {
    /// The canonical decoded representation, when this is a built-in tuple value.
    ///
    /// Encoding-only adapters may return `nil`. Values produced by `Tuple`'s
    /// decoder always provide a representation.
    var tupleValue: TupleValue? { get }

    /// Encode this value directly into an FDB Tuple Layer sink.
    func encodeTuple(to sink: inout TupleEncodingSink)

    /// Decode a value of this type from a byte array at the specified position.
    ///
    /// - Parameters:
    ///   - bytes: The encoded byte array.
    ///   - offset: The read start position (the byte after the type code). Updated after decoding.
    static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Self
}

extension TupleElement {
    public var tupleValue: TupleValue? { nil }

    /// Encode into one exactly-sized owned allocation.
    public func encodeTuple() -> ByteString {
        var measuringSink = TupleEncodingSink(measuringFrom: 0)
        encodeTuple(to: &measuringSink)
        let byteCount = measuringSink.byteCount
        return ByteString.copying(count: byteCount) { buffer in
            var sink = TupleEncodingSink(buffer: buffer)
            encodeTuple(to: &sink)
            sink.validateFinalByteCount(byteCount)
        }
    }
}

/// Error type for the Tuple Layer.
public enum TupleError: Error, Sendable {
    case unexpectedEndOfData
    case invalidElementRange(lowerBound: Int, upperBound: Int, count: Int)
    case invalidTypeCode(UInt8)
    case integerOverflow
    case invalidUTF8
    case invalidNullEscape
    case cannotIncrementKey
    case prefixMismatch
    case elementHasNoCanonicalValue
    case decodedStorageOverflow
}

/// Byte count limit table for each type (used in variable-length integer encoding).
///
/// sizeLimits[n] = 2^(8*(n+1)) - 1
/// Returns the maximum value representable in n bytes.
package let sizeLimits: [UInt64] = [
    0xFF,                       // 1 byte
    0xFFFF,                     // 2 bytes
    0xFFFF_FF,                  // 3 bytes
    0xFFFF_FFFF,                // 4 bytes
    0xFFFF_FFFF_FF,             // 5 bytes
    0xFFFF_FFFF_FFFF,           // 6 bytes
    0xFFFF_FFFF_FFFF_FF,        // 7 bytes
    0xFFFF_FFFF_FFFF_FFFF,      // 8 bytes
]
