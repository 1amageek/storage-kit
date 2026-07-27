import DatabaseTypes
/// Type representing a null value within a Tuple.
///
/// In the FDB Tuple Layer, null is encoded as a single byte with type code 0x00.
public struct TupleNil: TupleElement, Sendable {
    public init() {}

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.null.rawValue)
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> TupleNil {
        TupleNil()
    }

    public static func == (lhs: TupleNil, rhs: TupleNil) -> Bool {
        true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(TupleTypeCode.null.rawValue)
    }
}
