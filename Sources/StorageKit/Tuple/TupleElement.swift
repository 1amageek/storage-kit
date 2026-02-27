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
}

/// Byte array type for StorageKit (equivalent to FDB.Bytes).
public typealias Bytes = [UInt8]

/// strinc algorithm: returns the next prefix in lexicographic order.
///
/// Strips trailing 0xFF bytes and increments the last byte.
/// Used for generating end keys in range scans.
///
/// Reference: FoundationDB strinc specification
public func strinc(_ bytes: Bytes) throws -> Bytes {
    var result = bytes
    while result.last == 0xFF {
        result.removeLast()
    }
    guard !result.isEmpty else {
        throw TupleError.cannotIncrementKey
    }
    result[result.count - 1] &+= 1
    return result
}

/// Protocol for encoding/decoding with the Tuple Layer.
///
/// Converts each type to/from byte arrays following the FDB Tuple Layer binary format.
/// The encoded result preserves lexicographic order matching the logical order of values.
public protocol TupleElement: Sendable, Hashable {
    /// Encode this value into a byte array in FDB Tuple Layer format.
    func encodeTuple() -> Bytes

    /// Decode a value of this type from a byte array at the specified position.
    ///
    /// - Parameters:
    ///   - bytes: The encoded byte array.
    ///   - offset: The read start position (the byte after the type code). Updated after decoding.
    static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Self
}

/// Error type for the Tuple Layer.
public enum TupleError: Error, Sendable {
    case unexpectedEndOfData
    case invalidTypeCode(UInt8)
    case integerOverflow
    case invalidUTF8
    case invalidNullEscape
    case cannotIncrementKey
    case prefixMismatch
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
