import DatabaseTypes

/// Tuple-based key space prefix management.
///
/// Subspace manages groups of keys sharing a common prefix.
/// Identical semantics to FoundationDB's Subspace.
///
/// ## Usage example
/// ```swift
/// let root = Subspace(prefix: [])
/// let users = root.subspace("users")
/// let user42 = users.subspace(Int64(42))
/// let key = user42.pack(Tuple("email"))
/// ```
public struct Subspace: Sendable, Hashable, Equatable {

    /// The prefix byte array of this subspace.
    public let prefix: ByteString

    // MARK: - Initializers

    public init(prefix: ByteString = []) {
        self.prefix = prefix
    }

    /// Use the encoded result of a Tuple as the prefix.
    public init(_ tuple: Tuple) {
        self.prefix = tuple.pack()
    }

    /// Build a prefix from Tuple elements using variadic arguments.
    public init(_ elements: any TupleElement...) {
        self.prefix = Tuple(elements).pack()
    }

    // MARK: - Subspace nesting

    /// Create a nested subspace with additional elements.
    public func subspace(_ elements: any TupleElement...) -> Subspace {
        Subspace(prefix: append(Tuple(elements)))
    }

    /// Nest via subscript (alias for subspace).
    public subscript(_ elements: any TupleElement...) -> Subspace {
        Subspace(prefix: append(Tuple(elements)))
    }

    // MARK: - Pack / Unpack

    /// Encode a Tuple with this subspace's prefix prepended.
    public func pack(_ tuple: Tuple) -> ByteString {
        append(tuple)
    }

    /// Encode borrowed tuple elements directly into one final key allocation.
    public func pack<Elements: Collection>(
        elements: Elements
    ) -> ByteString where Elements.Element == any TupleElement {
        pack(elements: elements, appending: nil)
    }

    /// Encode borrowed tuple elements and a final marker directly into one
    /// final key allocation.
    public func pack<Elements: Collection>(
        elements: Elements,
        appending finalElement: (any TupleElement)?
    ) -> ByteString where Elements.Element == any TupleElement {
        var measuringSink = TupleEncodingSink(measuringFrom: 0)
        Tuple.encodePacked(
            elements,
            appending: finalElement,
            to: &measuringSink
        )
        let tupleByteCount = measuringSink.byteCount
        let (byteCount, overflow) = prefix.count.addingReportingOverflow(
            tupleByteCount
        )
        precondition(!overflow, "Subspace key byte count overflow")
        return ByteString.copying(count: byteCount) { buffer in
            prefix.withUnsafeBytes { source in
                let destination = UnsafeMutableRawBufferPointer(
                    start: buffer.baseAddress,
                    count: prefix.count
                )
                destination.copyMemory(from: source)
            }
            var sink = TupleEncodingSink(
                buffer: buffer,
                startingAt: prefix.count
            )
            Tuple.encodePacked(
                elements,
                appending: finalElement,
                to: &sink
            )
            sink.validateFinalByteCount(byteCount)
        }
    }

    /// Builds a pre-measured tuple suffix directly in one final key allocation.
    ///
    /// The encoder closure is synchronous. Any borrowed pointer exposed through
    /// the sink must not escape the closure.
    public func pack<Failure: Error>(
        encodedTupleByteCount: Int,
        encode: (inout TupleEncodingSink) throws(Failure) -> Void
    ) throws(Failure) -> ByteString {
        precondition(encodedTupleByteCount >= 0)
        let (byteCount, overflow) = prefix.count.addingReportingOverflow(
            encodedTupleByteCount
        )
        precondition(!overflow, "Subspace key byte count overflow")
        let initialize: (
            UnsafeMutableRawBufferPointer
        ) throws(Failure) -> Void = { buffer in
            prefix.withUnsafeBytes { source in
                let destination = UnsafeMutableRawBufferPointer(
                    start: buffer.baseAddress,
                    count: prefix.count
                )
                destination.copyMemory(from: source)
            }
            var sink = TupleEncodingSink(
                buffer: buffer,
                startingAt: prefix.count
            )
            try encode(&sink)
            sink.validateFinalByteCount(byteCount)
        }
        return try ByteString.copying(count: byteCount, initialize)
    }

    /// Strip the prefix and decode a Tuple.
    public func unpack(_ key: ByteString) throws -> Tuple {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        let remaining = key[
            (key.startIndex + prefix.count)..<key.endIndex
        ]
        return try Tuple(packed: remaining)
    }

    /// Opens a single-pass decoder over the key suffix without copying it.
    public func tupleCursor(for key: ByteString) throws -> TupleCursor {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        return TupleCursor(
            bytes: key[(key.startIndex + prefix.count)..<key.endIndex]
        )
    }

    // MARK: - Contains

    /// Check whether a key is contained within this subspace.
    public func contains(_ key: ByteString) -> Bool {
        guard key.count >= prefix.count else { return false }
        return key.prefix(prefix.count) == prefix[...]
    }

    // MARK: - Range

    /// Returns the full key range of this subspace [prefix + 0x00, strinc(prefix)).
    ///
    /// Does not include the prefix itself. Only keys that have at least 1 byte of additional data after the prefix.
    public func range() -> (begin: ByteString, end: ByteString) {
        let begin = appending(0x00, to: prefix)
        let end: ByteString
        if prefix.isEmpty {
            end = [0xFF]
        } else {
            do {
                end = try strinc(prefix)
            } catch {
                end = appending(0xFF, to: prefix)
            }
        }
        return (begin: begin, end: end)
    }

    /// Generate a key range from a Tuple range.
    public func range(from start: Tuple, to end: Tuple) -> (begin: ByteString, end: ByteString) {
        let beginKey = append(start)
        let endKey = append(end)
        return (begin: beginKey, end: endKey)
    }

    private func append(_ tuple: Tuple) -> ByteString {
        let tupleByteCount = tuple.packedByteCount
        let (byteCount, overflow) = prefix.count.addingReportingOverflow(
            tupleByteCount
        )
        precondition(!overflow)
        return ByteString.copying(count: byteCount) { buffer in
            prefix.withUnsafeBytes { source in
                let destination = UnsafeMutableRawBufferPointer(
                    start: buffer.baseAddress,
                    count: prefix.count
                )
                destination.copyMemory(from: source)
            }
            var sink = TupleEncodingSink(
                buffer: buffer,
                startingAt: prefix.count
            )
            tuple.encodePacked(to: &sink)
            sink.validateFinalByteCount(byteCount)
        }
    }

    /// Prefix-based range [prefix, strinc(prefix)).
    ///
    /// Targets all keys including the prefix itself.
    public func prefixRange() throws -> (begin: ByteString, end: ByteString) {
        let end = try strinc(prefix)
        return (begin: prefix, end: end)
    }

    private func appending(
        _ byte: UInt8,
        to bytes: ByteString
    ) -> ByteString {
        ByteString.copying(count: bytes.count + 1) { destination in
            bytes.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            destination[bytes.count] = byte
        }
    }
}
