import Foundation

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
        Subspace(prefix: prefix + Tuple(elements).pack())
    }

    /// Nest via subscript (alias for subspace).
    public subscript(_ elements: any TupleElement...) -> Subspace {
        Subspace(prefix: prefix + Tuple(elements).pack())
    }

    // MARK: - Pack / Unpack

    /// Encode a Tuple with this subspace's prefix prepended.
    public func pack(_ tuple: Tuple) -> Bytes {
        prefix + tuple.pack()
    }

    /// Strip the prefix and decode a Tuple.
    public func unpack(_ key: Bytes) throws -> Tuple {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        let remaining = Array(key[prefix.count...])
        let elements = try Tuple.unpack(from: remaining)
        return Tuple(elements)
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
            end = (try? strinc(prefix)) ?? (prefix + [0xFF])
        }
        return (begin: begin, end: end)
    }

    /// Generate a key range from a Tuple range.
    public func range(from start: Tuple, to end: Tuple) -> (begin: Bytes, end: Bytes) {
        let beginKey = prefix + start.pack()
        let endKey = prefix + end.pack()
        return (begin: beginKey, end: endKey)
    }

    /// Prefix-based range [prefix, strinc(prefix)).
    ///
    /// Targets all keys including the prefix itself.
    public func prefixRange() throws -> (begin: Bytes, end: Bytes) {
        let end = try strinc(prefix)
        return (begin: prefix, end: end)
    }
}
