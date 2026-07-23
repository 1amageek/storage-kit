#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// MARK: - String

extension String: TupleElement {
    /// Null-terminated encoding with 0x00 escaped as 0x00 0xFF.
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.string.rawValue)
        for byte in self.utf8 {
            if byte == 0x00 {
                sink.writeByte(0x00)
                sink.writeByte(0xff)
            } else {
                sink.writeByte(byte)
            }
        }
        sink.writeByte(0x00)
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> String {
        let raw = try decodeNullTerminated(from: bytes, at: &offset)
        guard let str = String(bytes: raw, encoding: .utf8) else {
            throw TupleError.invalidUTF8
        }
        return str
    }
}

// MARK: - Bytes

extension Bytes: TupleElement {
    /// Null-terminated encoding with 0x00 escaped as 0x00 0xFF (same algorithm as String).
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.bytes.rawValue)
        withUnsafeBytes { bytes in
            var chunkStart = 0
            for index in bytes.indices where bytes[index] == 0 {
                if chunkStart < index {
                    sink.writeBytes(
                        UnsafeRawBufferPointer(
                            rebasing: bytes[chunkStart..<index]
                        )
                    )
                }
                sink.writeByte(0)
                sink.writeByte(0xff)
                chunkStart = index + 1
            }
            if chunkStart < bytes.count {
                sink.writeBytes(
                    UnsafeRawBufferPointer(
                        rebasing: bytes[chunkStart..<bytes.count]
                    )
                )
            }
        }
        sink.writeByte(0)
    }

    public static func decodeTuple(
        from bytes: Bytes,
        at offset: inout Int
    ) throws -> Bytes {
        try decodeNullTerminated(from: bytes, at: &offset)
    }
}

/// Common decoding logic for null-terminated + null-escaped byte arrays.
///
/// - 0x00 followed by 0xFF: escaped 0x00 (a null byte contained in the data).
/// - 0x00 not followed by 0xFF: terminator.
package func decodeNullTerminated(from bytes: Bytes, at offset: inout Int) throws -> Bytes {
    let start = offset
    var cursor = offset
    var decodedCount = 0
    var containsEscape = false
    while cursor < bytes.count {
        let byte = bytes[cursor]
        cursor += 1
        if byte == 0x00 {
            if cursor < bytes.count && bytes[cursor] == 0xFF {
                containsEscape = true
                decodedCount += 1
                cursor += 1
            } else {
                let end = cursor - 1
                offset = cursor
                guard containsEscape else {
                    return bytes[start..<end]
                }
                return Bytes.copying(count: decodedCount) { output in
                    var source = start
                    var destination = 0
                    while source < end {
                        let value = bytes[source]
                        output[destination] = value
                        destination += 1
                        source += value == 0 ? 2 : 1
                    }
                }
            }
        } else {
            decodedCount += 1
        }
    }
    throw TupleError.unexpectedEndOfData
}
