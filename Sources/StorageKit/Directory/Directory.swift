import DatabaseTypes

/// A resolved Directory node: its engine domain, address, layer tag, and the
/// opaque prefixes that place it in the keyspace.
///
/// `keyspacePrefix` is the node's whole keyspace start and is its generation:
/// a node removed and recreated at the same address receives a different one.
/// `root` is the Subspace a caller derives keys from. For a plain Directory the
/// two prefixes are identical. For a Partition the node prefix also carries the
/// nested Directory Layer, so the data root is offset by one reserved byte that
/// no allocated child prefix and no nested metadata key can start with.
public struct Directory: Sendable, Hashable {
    /// Offset of a Partition's nested Directory Layer node subspace, above
    /// every content byte a nested node or a Subspace can start with.
    package static let nodeSubspaceByte: UInt8 = 0xFE

    /// Data-root offset of a Partition, below the reserved node-subspace byte
    /// and above every Tuple type code.
    package static let partitionDataByte: UInt8 = 0xFD

    public let domain: StorageTransactionDomain
    public let address: StorageAddress
    public let layer: LayerTag
    public let keyspacePrefix: ByteString
    public let root: Subspace

    /// Content base of the Directory Layer that contains this node.
    package let layerRoot: ByteString

    package init(
        domain: StorageTransactionDomain,
        address: StorageAddress,
        layer: LayerTag,
        keyspacePrefix: ByteString,
        layerRoot: ByteString
    ) {
        self.domain = domain
        self.address = address
        self.layer = layer
        self.keyspacePrefix = keyspacePrefix
        self.layerRoot = layerRoot
        self.root = Subspace(
            prefix: layer.isPartition
                ? keyspacePrefix.appending(Self.partitionDataByte)
                : keyspacePrefix
        )
    }

    /// Opaque generation identity of this resolution.
    public var generation: ByteString { keyspacePrefix }

    public var isPartition: Bool { layer.isPartition }

    /// Content base of the Directory Layer that holds this node's children.
    package var childLayerRoot: ByteString {
        layer.isPartition ? keyspacePrefix : layerRoot
    }

    public static func == (lhs: Directory, rhs: Directory) -> Bool {
        lhs.domain === rhs.domain
            && lhs.address == rhs.address
            && lhs.layer == rhs.layer
            && lhs.keyspacePrefix == rhs.keyspacePrefix
            && lhs.layerRoot == rhs.layerRoot
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(domain))
        hasher.combine(address)
        hasher.combine(layer)
        hasher.combine(keyspacePrefix)
        hasher.combine(layerRoot)
    }
}
