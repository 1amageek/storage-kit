/// Ordered exact name components from the root Directory; empty is the root.
///
/// This is StorageKit's logical path value. No normalization and no separator
/// parsing is applied: a component is the exact string the caller supplied.
///
/// Two addresses are the same address when their components hold the same
/// UTF-8 bytes. Swift compares and hashes `String` by canonical equivalence,
/// so two canonically equivalent spellings are one `String` value while every
/// backend this package targets encodes them to two distinct keys. Deciding
/// identity on the bytes the store holds is what keeps one address naming one
/// node, and the same rule orders ancestry.
public struct StorageAddress: Sendable, Hashable {
    public let components: [String]

    public static let root = StorageAddress()

    public init() {
        self.components = []
    }

    public init(_ components: [String]) throws(DirectoryAddressError) {
        guard components.count <= DirectoryLimits.maximumDepth else {
            throw .depthExceeded(depth: components.count)
        }
        for component in components {
            try Self.validateComponent(component)
        }
        self.components = components
    }

    private init(unchecked components: [String]) {
        self.components = components
    }

    public var isRoot: Bool { components.isEmpty }

    public var depth: Int { components.count }

    public func appending(
        _ component: String
    ) throws(DirectoryAddressError) -> StorageAddress {
        try Self.validateComponent(component)
        guard components.count < DirectoryLimits.maximumDepth else {
            throw .depthExceeded(depth: components.count + 1)
        }
        return StorageAddress(unchecked: components + [component])
    }

    /// Whether `other` is this address or lies below it.
    public func isAncestorOrSelf(of other: StorageAddress) -> Bool {
        guard other.components.count >= components.count else {
            return false
        }
        for (mine, theirs) in zip(components, other.components)
        where !Self.componentsAreEqual(mine, theirs) {
            return false
        }
        return true
    }

    /// Whether the subtree rooted at this address and the subtree rooted at
    /// `other` share at least one node.
    ///
    /// Both subtrees are ancestor-closed, so a shared node forces its two
    /// roots onto one root-to-node path, which orders them by depth. The
    /// relation is therefore exactly "one root is an ancestor-or-self of the
    /// other", and it is symmetric: neither address is the subject.
    public func subtreeIntersects(_ other: StorageAddress) -> Bool {
        isAncestorOrSelf(of: other) || other.isAncestorOrSelf(of: self)
    }

    /// Whether two name components are one storage identity.
    ///
    /// A component's identity is its UTF-8 bytes, because those bytes are what
    /// a backend stores and orders. `String` equality would merge two distinct
    /// stored nodes into one address.
    package static func componentsAreEqual(_ left: String, _ right: String) -> Bool {
        left.utf8.elementsEqual(right.utf8)
    }

    public static func == (lhs: StorageAddress, rhs: StorageAddress) -> Bool {
        guard lhs.components.count == rhs.components.count else {
            return false
        }
        for (left, right) in zip(lhs.components, rhs.components)
        where !componentsAreEqual(left, right) {
            return false
        }
        return true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(components.count)
        for component in components {
            for byte in component.utf8 {
                hasher.combine(byte)
            }
            // 0xFF never occurs in UTF-8, so terminating each component with it
            // keeps ["ab", "c"] and ["a", "bc"] apart without a length prefix.
            hasher.combine(UInt8(0xFF))
        }
    }

    /// Validates one name component against `DirectoryLimits`.
    package static func validateComponent(
        _ component: String
    ) throws(DirectoryAddressError) {
        let byteCount = component.utf8.count
        guard byteCount > 0 else {
            throw .emptyComponent
        }
        guard byteCount <= DirectoryLimits.maximumComponentByteCount else {
            throw .componentTooLong(byteCount: byteCount)
        }
    }
}
