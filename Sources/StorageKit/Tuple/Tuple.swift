import Foundation

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

    /// Internal wrapper that holds type-erased elements.
    private struct AnyElement: Sendable, Hashable {
        let encoded: Bytes

        init(_ element: any TupleElement) {
            self.encoded = element.encodeTuple()
        }

        static func == (lhs: AnyElement, rhs: AnyElement) -> Bool {
            lhs.encoded == rhs.encoded
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(encoded)
        }
    }

    private let storage: [AnyElement]

    /// Number of elements.
    public var count: Int { storage.count }

    /// Whether the tuple is empty.
    public var isEmpty: Bool { storage.isEmpty }

    // MARK: - Initializers

    public init(_ elements: any TupleElement...) {
        self.storage = elements.map { AnyElement($0) }
    }

    public init(_ elements: [any TupleElement]) {
        self.storage = elements.map { AnyElement($0) }
    }

    /// Internal: construct directly from an AnyElement array.
    private init(storage: [AnyElement]) {
        self.storage = storage
    }

    // MARK: - Subscript

    /// Access an element by index (returns nil if out of bounds).
    public subscript(index: Int) -> (any TupleElement)? {
        guard index >= 0 && index < storage.count else { return nil }
        let encoded = storage[index].encoded
        guard let first = encoded.first else { return nil }
        var offset = 1
        do {
            return try Self.decodeElement(typeCode: first, bytes: encoded, at: &offset)
        } catch {
            return nil
        }
    }

    // MARK: - Pack

    /// Encode all elements into a byte array.
    public func pack() -> Bytes {
        var result = Bytes()
        for element in storage {
            result.append(contentsOf: element.encoded)
        }
        return result
    }

    // MARK: - Unpack

    /// Decode an array of elements from a byte array.
    ///
    /// Same single-pass approach as the FDB implementation: each decoder directly updates the inout offset.
    public static func unpack(from bytes: Bytes) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = 0

        while offset < bytes.count {
            let typeCode = bytes[offset]
            offset += 1

            let element = try decodeElement(typeCode: typeCode, bytes: bytes, at: &offset)
            elements.append(element)
        }

        return elements
    }

    /// Decode a single element based on the type code and update the offset.
    ///
    /// - Parameters:
    ///   - typeCode: The already-read type code byte.
    ///   - bytes: The full byte array.
    ///   - offset: The byte position after the type code (updated after decoding).
    private static func decodeElement(typeCode: UInt8, bytes: Bytes, at offset: inout Int) throws -> any TupleElement {
        let intZero = TupleTypeCode.intZero.rawValue

        switch typeCode {
        case TupleTypeCode.null.rawValue:
            return TupleNil()

        case TupleTypeCode.bytes.rawValue:
            return try Bytes.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.string.rawValue:
            return try String.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.nested.rawValue:
            return try decodeNestedTuple(from: bytes, at: &offset)

        case intZero:
            return Int64(0)

        case 0x0B..<intZero, (intZero + 1)...0x1D:
            // Int64.decodeTuple reads bytes[offset - 1] as the type code
            return try Int64.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.float.rawValue:
            return try Float.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.double.rawValue:
            return try Double.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.boolFalse.rawValue:
            return false

        case TupleTypeCode.boolTrue.rawValue:
            return true

        case TupleTypeCode.uuid.rawValue:
            return try UUID.decodeTuple(from: bytes, at: &offset)

        default:
            throw TupleError.invalidTypeCode(typeCode)
        }
    }

    // MARK: - Nested Tuple

    /// Encode a Nested Tuple (type code 0x05).
    ///
    /// Encodes internal elements, escapes 0x00 bytes in the result as 0x00 0xFF,
    /// and appends a 0x00 terminator at the end.
    public func encodeNested() -> Bytes {
        var result: Bytes = [TupleTypeCode.nested.rawValue]
        for element in storage {
            let encoded = element.encoded
            for byte in encoded {
                if byte == 0x00 {
                    result.append(0x00)
                    result.append(0xFF)
                } else {
                    result.append(byte)
                }
            }
        }
        result.append(0x00) // terminator
        return result
    }

    /// Decode a Nested Tuple.
    ///
    /// Collects internal bytes while restoring the null-escape pattern (0x00 + 0xFF),
    /// and detects termination at a non-escaped 0x00. No depth tracking is needed.
    private static func decodeNestedTuple(from bytes: Bytes, at offset: inout Int) throws -> Tuple {
        var innerBytes = Bytes()
        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1
            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    innerBytes.append(0x00)
                    offset += 1
                } else {
                    // terminator found
                    break
                }
            } else {
                innerBytes.append(byte)
            }
        }
        let elements = try unpack(from: innerBytes)
        return Tuple(elements)
    }
}

// MARK: - TupleElement conformance for Tuple (nested)

extension Tuple: TupleElement {
    public func encodeTuple() -> Bytes {
        encodeNested()
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Tuple {
        try decodeNestedTuple(from: bytes, at: &offset)
    }
}

// MARK: - Append

extension Tuple {
    /// Return a new Tuple with an element appended.
    public func appending(_ element: any TupleElement) -> Tuple {
        var newStorage = storage
        newStorage.append(AnyElement(element))
        return Tuple(storage: newStorage)
    }

    /// Return a new Tuple with all elements of another Tuple appended.
    public func appending(_ other: Tuple) -> Tuple {
        Tuple(storage: storage + other.storage)
    }
}
