/// Ordered exact name components from the root Directory; empty is the root.
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
            try DirectoryPath.validateComponent(component)
        }
        self.components = components
    }

    public init(_ path: DirectoryPath) {
        self.components = path.components
    }

    private init(unchecked components: [String]) {
        self.components = components
    }

    public var isRoot: Bool { components.isEmpty }

    public var depth: Int { components.count }

    public var lastComponent: String? { components.last }

    public var parent: StorageAddress? {
        guard !components.isEmpty else {
            return nil
        }
        return StorageAddress(unchecked: Array(components.dropLast()))
    }

    public func appending(
        _ component: String
    ) throws(DirectoryAddressError) -> StorageAddress {
        try DirectoryPath.validateComponent(component)
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
}
