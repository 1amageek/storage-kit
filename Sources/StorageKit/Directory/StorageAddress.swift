/// Ordered steps from the root Directory; the empty address is the root.
public struct StorageAddress: Sendable, Hashable {
    public let steps: [StorageAddressStep]

    public static let root = StorageAddress()

    public init() {
        self.steps = []
    }

    public init(_ steps: [StorageAddressStep]) throws(DirectoryAddressError) {
        guard steps.count <= DirectoryLimits.maximumDepth else {
            throw .depthExceeded(depth: steps.count)
        }
        for step in steps {
            try step.validate()
        }
        self.steps = steps
    }

    public init(_ path: DirectoryPath) {
        self.steps = path.components.map { .directory($0) }
    }

    private init(unchecked steps: [StorageAddressStep]) {
        self.steps = steps
    }

    public var isRoot: Bool { steps.isEmpty }

    public var depth: Int { steps.count }

    public var lastStep: StorageAddressStep? { steps.last }

    public var parent: StorageAddress? {
        guard !steps.isEmpty else {
            return nil
        }
        return StorageAddress(unchecked: Array(steps.dropLast()))
    }

    public func appending(
        _ step: StorageAddressStep
    ) throws(DirectoryAddressError) -> StorageAddress {
        try step.validate()
        guard steps.count < DirectoryLimits.maximumDepth else {
            throw .depthExceeded(depth: steps.count + 1)
        }
        return StorageAddress(unchecked: steps + [step])
    }

    /// Whether `other` is this address or lies below it.
    public func isAncestorOrSelf(of other: StorageAddress) -> Bool {
        other.steps.count >= steps.count && other.steps.starts(with: steps)
    }
}
