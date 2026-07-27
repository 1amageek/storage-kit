import DatabaseTypes

extension Bool: TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(
            self
                ? TupleTypeCode.boolTrue.rawValue
                : TupleTypeCode.boolFalse.rawValue
        )
    }

    public static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> Bool {
        guard offset > bytes.startIndex else {
            throw TupleError.unexpectedEndOfData
        }
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
