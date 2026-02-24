import Foundation

// MARK: - Bool

extension Bool: TupleElement {
    public func encodeTuple() -> Bytes {
        [self ? TupleTypeCode.boolTrue.rawValue : TupleTypeCode.boolFalse.rawValue]
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Bool {
        guard offset > 0 else { throw TupleError.unexpectedEndOfData }
        let typeCode = bytes[offset - 1]
        switch typeCode {
        case TupleTypeCode.boolTrue.rawValue:
            return true
        case TupleTypeCode.boolFalse.rawValue:
            return false
        default:
            throw TupleError.invalidTypeCode(typeCode)
        }
    }
}

// MARK: - UUID

extension UUID: TupleElement {
    /// 型コード 0x30 + 16 バイト (canonical byte order)
    public func encodeTuple() -> Bytes {
        let u = self.uuid
        return [TupleTypeCode.uuid.rawValue,
                u.0, u.1, u.2, u.3, u.4, u.5, u.6, u.7,
                u.8, u.9, u.10, u.11, u.12, u.13, u.14, u.15]
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> UUID {
        guard offset + 16 <= bytes.count else { throw TupleError.unexpectedEndOfData }
        let uuid = UUID(uuid: (
            bytes[offset],    bytes[offset+1],  bytes[offset+2],  bytes[offset+3],
            bytes[offset+4],  bytes[offset+5],  bytes[offset+6],  bytes[offset+7],
            bytes[offset+8],  bytes[offset+9],  bytes[offset+10], bytes[offset+11],
            bytes[offset+12], bytes[offset+13], bytes[offset+14], bytes[offset+15]
        ))
        offset += 16
        return uuid
    }
}

// MARK: - Date

extension Date: TupleElement {
    /// Double (timeIntervalSince1970) としてエンコード
    public func encodeTuple() -> Bytes {
        self.timeIntervalSince1970.encodeTuple()
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Date {
        let interval = try Double.decodeTuple(from: bytes, at: &offset)
        return Date(timeIntervalSince1970: interval)
    }
}
