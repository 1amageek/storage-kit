import DatabaseTypes
/// Single-pass tuple decoder that retains the source buffer and returns byte
/// elements as constant-time views into it.
///
/// A cursor avoids the intermediate element arrays created by `Tuple.unpack`.
/// It is intended for storage hot paths that know their physical tuple layout.
public struct TupleCursor {
    private let bytes: ByteString
    private var offset: Int

    public init(bytes: ByteString) {
        self.bytes = bytes
        self.offset = bytes.startIndex
    }

    public var isAtEnd: Bool {
        offset == bytes.endIndex
    }

    public var consumedByteCount: Int {
        offset - bytes.startIndex
    }

    public mutating func next() throws -> (any TupleElement)? {
        try next(admitting: nil)
    }

    /// Decodes one element while admitting retained allocations before they
    /// are created.
    public mutating func next(
        admitting allocation: @escaping (Int) throws -> Void
    ) throws -> (any TupleElement)? {
        let optionalAllocation: ((Int) throws -> Void)? = allocation
        return try next(admitting: optionalAllocation)
    }

    private mutating func next(
        admitting allocation: ((Int) throws -> Void)?
    ) throws -> (any TupleElement)? {
        guard offset < bytes.endIndex else { return nil }
        let typeCode = bytes[offset]
        offset += 1
        return try Tuple.decodeElement(
            typeCode: typeCode,
            bytes: bytes,
            at: &offset,
            admitting: allocation
        )
    }

    public mutating func requireNext() throws -> any TupleElement {
        guard let element = try next() else {
            throw TupleError.unexpectedEndOfData
        }
        return element
    }

    /// Decodes every remaining element directly into one tuple owner.
    public mutating func remainingTuple() throws -> Tuple {
        try remainingTuple(admitting: nil)
    }

    /// Decodes every remaining element directly into one tuple owner while
    /// admitting retained allocations before they are created.
    public mutating func remainingTuple(
        admitting allocation: @escaping (Int) throws -> Void
    ) throws -> Tuple {
        let optionalAllocation: ((Int) throws -> Void)? = allocation
        return try remainingTuple(admitting: optionalAllocation)
    }

    private mutating func remainingTuple(
        admitting allocation: ((Int) throws -> Void)?
    ) throws -> Tuple {
        var elements: [any TupleElement] = []
        var accountedCapacity = 0
        while let element = try next(admitting: allocation) {
            try Tuple.appendDecoded(
                element,
                to: &elements,
                accountedCapacity: &accountedCapacity,
                admitting: allocation
            )
        }
        return Tuple(decodedElements: elements)
    }

    /// Decodes one signed integer without materializing an existential or
    /// decoding a payload of an unexpected tuple type.
    public mutating func requireInt64() throws -> Int64 {
        guard offset < bytes.endIndex else {
            throw TupleError.unexpectedEndOfData
        }
        let typeCode = bytes[offset]
        let intZero = TupleTypeCode.intZero.rawValue
        guard typeCode >= intZero - 9, typeCode <= intZero + 9 else {
            throw TupleError.invalidTypeCode(typeCode)
        }
        offset += 1
        return try Int64.decodeTuple(from: bytes, at: &offset)
    }

    /// Decodes one bytes element as a view into the retained tuple storage.
    ///
    /// The returned value keeps the source owner alive. Canonical payloads that
    /// contain no escaped NUL bytes therefore require no allocation or copy.
    public mutating func requireBytes() throws -> ByteString {
        guard offset < bytes.endIndex else {
            throw TupleError.unexpectedEndOfData
        }
        let typeCode = bytes[offset]
        guard typeCode == TupleTypeCode.bytes.rawValue else {
            throw TupleError.invalidTypeCode(typeCode)
        }
        offset += 1
        return try ByteString.decodeTuple(from: bytes, at: &offset)
    }
}
