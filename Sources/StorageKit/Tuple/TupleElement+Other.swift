#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// MARK: - Bool

extension Bool: TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(
            self
                ? TupleTypeCode.boolTrue.rawValue
                : TupleTypeCode.boolFalse.rawValue
        )
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
    /// Type code 0x30 + 16 bytes (canonical byte order).
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        let u = self.uuid
        sink.writeByte(TupleTypeCode.uuid.rawValue)
        sink.writeByte(u.0)
        sink.writeByte(u.1)
        sink.writeByte(u.2)
        sink.writeByte(u.3)
        sink.writeByte(u.4)
        sink.writeByte(u.5)
        sink.writeByte(u.6)
        sink.writeByte(u.7)
        sink.writeByte(u.8)
        sink.writeByte(u.9)
        sink.writeByte(u.10)
        sink.writeByte(u.11)
        sink.writeByte(u.12)
        sink.writeByte(u.13)
        sink.writeByte(u.14)
        sink.writeByte(u.15)
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
    /// Encoded as a Double (timeIntervalSince1970).
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        timeIntervalSince1970.encodeTuple(to: &sink)
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Date {
        let interval = try Double.decodeTuple(from: bytes, at: &offset)
        return Date(timeIntervalSince1970: interval)
    }
}
