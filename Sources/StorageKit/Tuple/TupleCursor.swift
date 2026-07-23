/// Single-pass tuple decoder that retains the source buffer and returns byte
/// elements as constant-time views into it.
///
/// A cursor avoids the intermediate element arrays created by `Tuple.unpack`.
/// It is intended for storage hot paths that know their physical tuple layout.
public struct TupleCursor {
    private let bytes: Bytes
    private var offset: Int

    public init(bytes: Bytes) {
        self.bytes = bytes
        self.offset = 0
    }

    public var isAtEnd: Bool {
        offset == bytes.count
    }

    public var consumedByteCount: Int {
        offset
    }

    public mutating func next() throws -> (any TupleElement)? {
        guard offset < bytes.count else { return nil }
        let typeCode = bytes[offset]
        offset += 1
        return try Tuple.decodeElement(
            typeCode: typeCode,
            bytes: bytes,
            at: &offset
        )
    }

    public mutating func requireNext() throws -> any TupleElement {
        guard let element = try next() else {
            throw TupleError.unexpectedEndOfData
        }
        return element
    }

    /// Decodes one bytes element as a view into the retained tuple storage.
    ///
    /// The returned value keeps the source owner alive. Canonical payloads that
    /// contain no escaped NUL bytes therefore require no allocation or copy.
    public mutating func requireBytes() throws -> Bytes {
        guard offset < bytes.count else {
            throw TupleError.unexpectedEndOfData
        }
        let typeCode = bytes[offset]
        guard typeCode == TupleTypeCode.bytes.rawValue else {
            throw TupleError.invalidTypeCode(typeCode)
        }
        offset += 1
        return try Bytes.decodeTuple(from: bytes, at: &offset)
    }
}
