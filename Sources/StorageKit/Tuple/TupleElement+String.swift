import Foundation

// MARK: - String

extension String: TupleElement {
    /// null-terminated + 0x00 → 0x00 0xFF エスケープ
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
    /// null-terminated + 0x00 → 0x00 0xFF エスケープ（String と同一アルゴリズム）
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

/// null-terminated + null-escape バイト列のデコード共通処理
///
/// - 0x00 の後に 0xFF が続く場合: エスケープされた 0x00（データに含まれる null バイト）
/// - 0x00 の後に 0xFF が続かない場合: 終端（terminator）
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
