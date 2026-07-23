#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

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
    public let prefix: Bytes

    // MARK: - Initializers

    public init(prefix: Bytes = []) {
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
    public func pack(_ tuple: Tuple) -> Bytes {
        append(tuple)
    }

    /// Encode borrowed tuple elements directly into one final key allocation.
    public func pack<Elements: Collection>(
        elements: Elements
    ) -> Bytes where Elements.Element == any TupleElement {
        pack(elements: elements, appending: nil)
    }

    /// Encode borrowed tuple elements and a final marker directly into one
    /// final key allocation.
    public func pack<Elements: Collection>(
        elements: Elements,
        appending finalElement: (any TupleElement)?
    ) -> Bytes where Elements.Element == any TupleElement {
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
        return Bytes.copying(count: byteCount) { buffer in
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
    ) throws(Failure) -> Bytes {
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
        return try Bytes.copying(count: byteCount, initialize)
    }

    /// Strip the prefix and decode a Tuple.
    public func unpack(_ key: Bytes) throws -> Tuple {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        let remaining = key[prefix.count..<key.count]
        return try Tuple(packed: remaining)
    }

    /// Opens a single-pass decoder over the key suffix without copying it.
    public func tupleCursor(for key: Bytes) throws -> TupleCursor {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        return TupleCursor(bytes: key[prefix.count..<key.count])
    }

    // MARK: - Contains

    /// Check whether a key is contained within this subspace.
    public func contains(_ key: Bytes) -> Bool {
        guard key.count >= prefix.count else { return false }
        return key.prefix(prefix.count) == prefix[...]
    }

    // MARK: - Range

    /// Returns the full key range of this subspace [prefix + 0x00, strinc(prefix)).
    ///
    /// Does not include the prefix itself. Only keys that have at least 1 byte of additional data after the prefix.
    public func range() -> (begin: Bytes, end: Bytes) {
        let begin = prefix + [0x00]
        let end: Bytes
        if prefix.isEmpty {
            end = [0xFF]
        } else {
            do {
                end = try strinc(prefix)
            } catch TupleError.cannotIncrementKey {
                end = prefix + [0xFF]
            } catch {
                end = prefix + [0xFF]
            }
        }
        return (begin: begin, end: end)
    }

    /// Generate a key range from a Tuple range.
    public func range(from start: Tuple, to end: Tuple) -> (begin: Bytes, end: Bytes) {
        let beginKey = append(start)
        let endKey = append(end)
        return (begin: beginKey, end: endKey)
    }

    private func append(_ tuple: Tuple) -> Bytes {
        let tupleByteCount = tuple.packedByteCount
        let (byteCount, overflow) = prefix.count.addingReportingOverflow(
            tupleByteCount
        )
        precondition(!overflow)
        return Bytes.copying(count: byteCount) { buffer in
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
    public func prefixRange() throws -> (begin: Bytes, end: Bytes) {
        let end = try strinc(prefix)
        return (begin: prefix, end: end)
    }
}
