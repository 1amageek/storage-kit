/// One row of a child listing: the exact name and the node's layer tag.
public struct DirectoryEntry: Sendable, Hashable {
    public let name: String
    public let layer: LayerTag

    public init(name: String, layer: LayerTag) {
        self.name = name
        self.layer = layer
    }

    public var isPartition: Bool { layer.isPartition }
}
