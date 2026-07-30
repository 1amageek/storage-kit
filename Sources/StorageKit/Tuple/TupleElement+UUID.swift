import DatabaseTypes

extension DatabaseTypes.UUID: TupleElement {
    public var tupleValue: TupleValue? { .uuid(self) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.uuid.rawValue)
        sink.writeBytes(self)
    }

    public static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> DatabaseTypes.UUID {
        guard offset <= bytes.endIndex - 16 else {
            throw TupleError.unexpectedEndOfData
        }
        let value = DatabaseTypes.UUID(
            bytes: bytes[offset..<(offset + 16)]
        )
        guard let value else {
            throw TupleError.unexpectedEndOfData
        }
        offset += 16
        return value
    }
}
