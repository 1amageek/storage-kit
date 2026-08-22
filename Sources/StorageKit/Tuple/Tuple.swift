import DatabaseTypes

/// Composite key struct compatible with the FDB Tuple Layer.
///
/// Encodes multiple typed values into byte arrays, producing a binary format where
/// lexicographic order matches the logical order of each element.
///
/// ## Usage example
/// ```swift
/// let tuple = Tuple("users", Int64(42), "profile")
/// let packed = tuple.pack()
/// let unpacked = try Tuple.unpack(from: packed)
/// ```
///
/// ## Equality comparison
/// Compared via encoded byte arrays (FDB semantics compliant):
/// - +0.0 != -0.0 (different bit patterns)
/// - NaN == NaN (same bit pattern)
public struct Tuple: Sendable, Hashable, Equatable {
    private let storedElements: [any TupleElement]

    /// Number of elements.
    public var count: Int { storedElements.count }

    /// Whether the tuple is empty.
    public var isEmpty: Bool { storedElements.isEmpty }

    // MARK: - Initializers

    public init(_ elements: any TupleElement...) {
        storedElements = elements
    }

    public init(_ elements: [any TupleElement]) {
        storedElements = elements
    }

    /// Decode packed tuple bytes directly into owned tuple storage.
    /// Byte-backed element payloads remain views over the input byte owner.
    public init(packed bytes: ByteString) throws {
        self = try Self.unpackTuple(from: bytes)
    }

    /// Construct directly from decoded elements.
    private init(decodedElements: [any TupleElement]) {
        storedElements = decodedElements
    }

    // MARK: - Element Access

    /// Access an element by index with error propagation.
    ///
    /// - Parameter index: The element index.
    /// - Throws: `TupleError` if the index is out of bounds or decoding fails.
    public func element(at index: Int) throws -> any TupleElement {
        guard index >= 0 && index < storedElements.count else {
            throw TupleError.unexpectedEndOfData
        }
        return storedElements[index]
    }

    /// Access a decoded element without runtime type casting.
    public func value(at index: Int) throws -> TupleValue {
        let element = try element(at: index)
        guard let value = element.tupleValue else {
            throw TupleError.elementHasNoCanonicalValue
        }
        return value
    }

    /// Materializes a validated range of tuple elements without re-encoding them.
    ///
    /// Byte-backed elements remain views over their original owners. The returned
    /// array contains existential references only; element payloads are not copied.
    public func elements(
        in range: Range<Int>? = nil
    ) throws -> [any TupleElement] {
        let requestedRange: Range<Int>
        if let range {
            requestedRange = range
        } else {
            requestedRange = 0..<storedElements.count
        }
        guard requestedRange.lowerBound >= 0,
              requestedRange.upperBound >= requestedRange.lowerBound,
              requestedRange.upperBound <= storedElements.count else {
            throw TupleError.invalidElementRange(
                lowerBound: requestedRange.lowerBound,
                upperBound: requestedRange.upperBound,
                count: storedElements.count
            )
        }

        var result: [any TupleElement] = []
        result.reserveCapacity(requestedRange.count)
        for index in requestedRange {
            result.append(storedElements[index])
        }
        return result
    }

    /// Access an element by index (returns nil if out of bounds or decoding fails).
    ///
    /// Prefer `element(at:)` when error details are important.
    public subscript(index: Int) -> (any TupleElement)? {
        do {
            return try element(at: index)
        } catch {
            return nil
        }
    }

    // MARK: - Pack

    /// Encode all elements into a byte array.
    public func pack() -> ByteString {
        var measuringSink = TupleEncodingSink(measuringFrom: 0)
        encodePacked(to: &measuringSink)
        let byteCount = measuringSink.byteCount
        return ByteString.copying(count: byteCount) { buffer in
            var sink = TupleEncodingSink(buffer: buffer)
            encodePacked(to: &sink)
            sink.validateFinalByteCount(byteCount)
        }
    }

    package func encodePacked(to sink: inout TupleEncodingSink) {
        for element in storedElements {
            element.encodeTuple(to: &sink)
        }
    }

    package static func encodePacked<Elements: Collection>(
        _ elements: Elements,
        appending finalElement: (any TupleElement)?,
        to sink: inout TupleEncodingSink
    ) where Elements.Element == any TupleElement {
        for element in elements {
            element.encodeTuple(to: &sink)
        }
        if let finalElement {
            finalElement.encodeTuple(to: &sink)
        }
    }

    /// Number of bytes produced by `pack()` without allocating the packed
    /// representation.
    public var packedByteCount: Int {
        var sink = TupleEncodingSink(measuringFrom: 0)
        encodePacked(to: &sink)
        return sink.byteCount
    }

    // MARK: - Unpack

    /// Decode an array of elements from a byte array.
    ///
    /// Same single-pass approach as the FDB implementation: each decoder directly updates the inout offset.
    public static func unpack(from bytes: ByteString) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = bytes.startIndex

        while offset < bytes.endIndex {
            let typeCode = bytes[offset]
            offset += 1

            let element = try decodeElement(typeCode: typeCode, bytes: bytes, at: &offset)
            elements.append(element)
        }

        return elements
    }

    /// Decode directly into owned tuple storage without first materializing a
    /// separate existential array. Used by subspace scans on the hot key path.
    static func unpackTuple(from bytes: ByteString) throws -> Tuple {
        var decodedElements: [any TupleElement] = []
        var offset = bytes.startIndex

        while offset < bytes.endIndex {
            let typeCode = bytes[offset]
            offset += 1
            decodedElements.append(
                try decodeElement(
                    typeCode: typeCode,
                    bytes: bytes,
                    at: &offset
                )
            )
        }

        return Tuple(decodedElements: decodedElements)
    }

    /// Decode a single element based on the type code and update the offset.
    ///
    /// - Parameters:
    ///   - typeCode: The already-read type code byte.
    ///   - bytes: The full byte array.
    ///   - offset: The byte position after the type code (updated after decoding).
    package static func decodeElement(typeCode: UInt8, bytes: ByteString, at offset: inout Int) throws -> any TupleElement {
        let intZero = TupleTypeCode.intZero.rawValue

        switch typeCode {
        case TupleTypeCode.null.rawValue:
            return TupleNil()

        case TupleTypeCode.bytes.rawValue:
            return try ByteString.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.string.rawValue:
            return try String.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.nested.rawValue:
            return try decodeNestedTuple(from: bytes, at: &offset)

        case intZero:
            return Int64(0)

        case 0x0B..<intZero:
            // Negative integers: always Int64
            return try Int64.decodeTuple(from: bytes, at: &offset)

        case (intZero + 1)...0x1D:
            let value = try UInt64.decodeTuple(from: bytes, at: &offset)
            if value <= UInt64(Int64.max) {
                return Int64(value)
            }
            return value

        case TupleTypeCode.float.rawValue:
            return try Float.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.double.rawValue:
            return try Double.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.boolFalse.rawValue:
            return false

        case TupleTypeCode.boolTrue.rawValue:
            return true

        case TupleTypeCode.uuid.rawValue:
            return try DatabaseTypes.UUID.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.versionstamp.rawValue:
            return try Versionstamp.decodeTuple(from: bytes, at: &offset)

        default:
            throw TupleError.invalidTypeCode(typeCode)
        }
    }

    // MARK: - Nested Tuple

    /// Encode a Nested Tuple (type code 0x05).
    ///
    /// Encodes internal elements, escapes 0x00 bytes in the result as 0x00 0xFF,
    /// and appends a 0x00 terminator at the end.
    public func encodeNested() -> ByteString {
        encodeTuple()
    }

    /// Decode a Nested Tuple.
    ///
    /// Collects internal bytes while restoring the null-escape pattern (0x00 + 0xFF),
    /// and detects termination at a non-escaped 0x00. No depth tracking is needed.
    private static func decodeNestedTuple(from bytes: ByteString, at offset: inout Int) throws -> Tuple {
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
                    let innerBytes: ByteString
                    if containsEscape {
                        innerBytes = ByteString.copying(count: decodedCount) { output in
                            var source = start
                            var destination = 0
                            while source < end {
                                let value = bytes[source]
                                output[destination] = value
                                destination += 1
                                source += value == 0 ? 2 : 1
                            }
                        }
                    } else {
                        innerBytes = bytes[start..<end]
                    }
                    return Tuple(try unpack(from: innerBytes))
                }
            } else {
                decodedCount += 1
            }
        }
        throw TupleError.unexpectedEndOfData
    }
}

// MARK: - TupleElement conformance for Tuple (nested)

extension Tuple: TupleElement {
    public var tupleValue: TupleValue? { .nested(self) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.nested.rawValue)
        sink.withNullEscaping { nestedSink in
            encodePacked(to: &nestedSink)
        }
        sink.writeByte(0x00)
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Tuple {
        try decodeNestedTuple(from: bytes, at: &offset)
    }
}

extension Tuple {
    public static func == (lhs: Tuple, rhs: Tuple) -> Bool {
        lhs.pack() == rhs.pack()
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(pack())
    }
}

// MARK: - Append

extension Tuple {
    /// Return a new Tuple with an element appended.
    public func appending(_ element: any TupleElement) -> Tuple {
        var appendedElements = storedElements
        appendedElements.append(element)
        return Tuple(decodedElements: appendedElements)
    }

    /// Return a new Tuple with all elements of another Tuple appended.
    public func appending(_ other: Tuple) -> Tuple {
        Tuple(decodedElements: storedElements + other.storedElements)
    }
}
