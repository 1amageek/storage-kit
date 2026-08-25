import DatabaseTypes

// MARK: - String

extension String: TupleElement {
    public var tupleValue: TupleValue? { .string(self) }

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

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> String {
        try decodeTuple(
            from: bytes,
            at: &offset,
            admitting: nil
        )
    }

    static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int,
        admitting allocation: ((Int) throws -> Void)?
    ) throws -> String {
        let raw = try decodeNullTerminated(
            from: bytes,
            at: &offset,
            admitting: allocation
        )
        guard raw.hasValidUTF8Encoding else {
            throw TupleError.invalidUTF8
        }
        let (retainedBytes, overflow) = raw.count.addingReportingOverflow(32)
        guard !overflow else {
            throw TupleError.decodedStorageOverflow
        }
        try allocation?(retainedBytes)
        return raw.withUnsafeBytes { source in
            String(
                decoding: source.bindMemory(to: UInt8.self),
                as: UTF8.self
            )
        }
    }
}

private extension ByteString {
    var hasValidUTF8Encoding: Bool {
        withUnsafeBytes { bytes in
            var index = 0
            while index < bytes.count {
                let first = bytes[index]
                if first <= 0x7f {
                    index += 1
                    continue
                }

                if first >= 0xc2 && first <= 0xdf {
                    guard index + 1 < bytes.count,
                          Self.isUTF8Continuation(bytes[index + 1]) else {
                        return false
                    }
                    index += 2
                    continue
                }

                if first >= 0xe0 && first <= 0xef {
                    guard index + 2 < bytes.count else { return false }
                    let second = bytes[index + 1]
                    let validSecond = switch first {
                    case 0xe0: second >= 0xa0 && second <= 0xbf
                    case 0xed: second >= 0x80 && second <= 0x9f
                    default: Self.isUTF8Continuation(second)
                    }
                    guard validSecond,
                          Self.isUTF8Continuation(bytes[index + 2]) else {
                        return false
                    }
                    index += 3
                    continue
                }

                if first >= 0xf0 && first <= 0xf4 {
                    guard index + 3 < bytes.count else { return false }
                    let second = bytes[index + 1]
                    let validSecond = switch first {
                    case 0xf0: second >= 0x90 && second <= 0xbf
                    case 0xf4: second >= 0x80 && second <= 0x8f
                    default: Self.isUTF8Continuation(second)
                    }
                    guard validSecond,
                          Self.isUTF8Continuation(bytes[index + 2]),
                          Self.isUTF8Continuation(bytes[index + 3]) else {
                        return false
                    }
                    index += 4
                    continue
                }

                return false
            }
            return true
        }
    }

    static func isUTF8Continuation(_ byte: UInt8) -> Bool {
        byte >= 0x80 && byte <= 0xbf
    }
}

// MARK: - ByteString

extension ByteString: TupleElement {
    public var tupleValue: TupleValue? { .bytes(self) }

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
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> ByteString {
        try decodeNullTerminated(
            from: bytes,
            at: &offset,
            admitting: nil
        )
    }

    static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int,
        admitting allocation: ((Int) throws -> Void)?
    ) throws -> ByteString {
        try decodeNullTerminated(
            from: bytes,
            at: &offset,
            admitting: allocation
        )
    }
}

/// Common decoding logic for null-terminated + null-escaped byte arrays.
///
/// - 0x00 followed by 0xFF: escaped 0x00 (a null byte contained in the data).
/// - 0x00 not followed by 0xFF: terminator.
package func decodeNullTerminated(
    from bytes: ByteString,
    at offset: inout Int,
    admitting allocation: ((Int) throws -> Void)? = nil
) throws -> ByteString {
    let start = offset
    var cursor = offset
    var decodedCount = 0
    var containsEscape = false
    while cursor < bytes.endIndex {
        let byte = bytes[cursor]
        cursor += 1
        if byte == 0x00 {
            if cursor < bytes.endIndex && bytes[cursor] == 0xFF {
                containsEscape = true
                decodedCount += 1
                cursor += 1
            } else {
                let end = cursor - 1
                offset = cursor
                guard containsEscape else {
                    return bytes[start..<end]
                }
                let (retainedBytes, overflow) = decodedCount
                    .addingReportingOverflow(32)
                guard !overflow else {
                    throw TupleError.decodedStorageOverflow
                }
                try allocation?(retainedBytes)
                return ByteString.copying(count: decodedCount) { output in
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
