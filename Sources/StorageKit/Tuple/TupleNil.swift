/// Tuple 内の null 値を表現する型
///
/// FDB Tuple Layer では null は型コード 0x00 の 1 バイトでエンコードされる。
public struct TupleNil: TupleElement, Sendable {
    public init() {}

    public func encodeTuple() -> Bytes {
        [TupleTypeCode.null.rawValue]
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> TupleNil {
        TupleNil()
    }

    public static func == (lhs: TupleNil, rhs: TupleNil) -> Bool {
        true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(TupleTypeCode.null.rawValue)
    }
}
