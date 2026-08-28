/// Ordered, nonempty sequence of exact UTF-8 Directory name components.
///
/// No normalization and no separator parsing is applied: a component is the
/// exact string the caller supplied.
public struct DirectoryPath: Sendable, Hashable {
    public let components: [String]

    public init(_ components: [String]) throws(DirectoryAddressError) {
        guard !components.isEmpty else {
            throw .emptyPath
        }
        guard components.count <= DirectoryLimits.maximumDepth else {
            throw .depthExceeded(depth: components.count)
        }
        for component in components {
            try Self.validateComponent(component)
        }
        self.components = components
    }

    public init(_ components: String...) throws(DirectoryAddressError) {
        try self.init(components)
    }

    public var depth: Int { components.count }

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
