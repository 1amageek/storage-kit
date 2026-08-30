/// Ordered exact name components from the root Directory; empty is the root.
///
/// This is StorageKit's logical path value. No normalization and no separator
/// parsing is applied: a component is the exact string the caller supplied.
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
        other.components.count >= components.count
            && other.components.starts(with: components)
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
