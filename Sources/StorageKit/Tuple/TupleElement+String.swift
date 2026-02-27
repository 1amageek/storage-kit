import Foundation

// MARK: - String

extension String: TupleElement {
    /// Null-terminated encoding with 0x00 escaped as 0x00 0xFF.
    public func encodeTuple() -> Bytes {
        var result: Bytes = [TupleTypeCode.string.rawValue]
        for byte in self.utf8 {
            if byte == 0x00 {
                result.append(0x00)
                result.append(0xFF)
            } else {
                result.append(byte)
            }
        }
        result.append(0x00) // terminator
        return result
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> String {
        let raw = try decodeNullTerminated(from: bytes, at: &offset)
        guard let str = String(bytes: raw, encoding: .utf8) else {
            throw TupleError.invalidUTF8
        }
        return str
    }
}

// MARK: - Bytes ([UInt8])

extension Array: TupleElement where Element == UInt8 {
    /// Null-terminated encoding with 0x00 escaped as 0x00 0xFF (same algorithm as String).
    public func encodeTuple() -> Bytes {
        var result: Bytes = [TupleTypeCode.bytes.rawValue]
        for byte in self {
            if byte == 0x00 {
                result.append(0x00)
                result.append(0xFF)
            } else {
                result.append(byte)
            }
        }
        result.append(0x00) // terminator
        return result
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> [UInt8] {
        try decodeNullTerminated(from: bytes, at: &offset)
    }
}

/// Common decoding logic for null-terminated + null-escaped byte arrays.
///
/// - 0x00 followed by 0xFF: escaped 0x00 (a null byte contained in the data).
/// - 0x00 not followed by 0xFF: terminator.
package func decodeNullTerminated(from bytes: Bytes, at offset: inout Int) throws -> Bytes {
    var result = Bytes()
    while offset < bytes.count {
        let byte = bytes[offset]
        offset += 1
        if byte == 0x00 {
            if offset < bytes.count && bytes[offset] == 0xFF {
                result.append(0x00)
                offset += 1
            } else {
                return result
            }
        } else {
            result.append(byte)
        }
    }
    throw TupleError.unexpectedEndOfData
}
