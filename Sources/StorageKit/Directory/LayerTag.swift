import DatabaseTypes

/// Opaque tag bytes that type one Directory node.
///
/// A node whose tag is `.partition` roots a nested Directory Layer over one
/// contiguous keyspace. The empty tag (`.default`) types a plain Directory and
/// is the tag a node receives when the caller states no other type.
public struct LayerTag: Sendable, Hashable {
    public let bytes: ByteString

    public init(_ bytes: ByteString) throws(DirectoryAddressError) {
        guard bytes.count <= DirectoryLimits.maximumLayerTagByteCount else {
            throw .layerTagTooLong(byteCount: bytes.count)
        }
        self.bytes = bytes
    }

    public init(utf8 string: String) throws(DirectoryAddressError) {
        try self.init(ByteString(utf8: string))
    }

    private init(unchecked bytes: ByteString) {
        self.bytes = bytes
    }

    /// Plain Directory: the empty tag, matching an unspecified layer.
    public static let `default` = LayerTag(unchecked: ByteString())

    /// Partition: the same tag bytes the FoundationDB Directory Layer uses.
    public static let partition = LayerTag(unchecked: ByteString(utf8: "partition"))

    public var isDefault: Bool { bytes.isEmpty }

    public var isPartition: Bool { bytes == Self.partition.bytes }
}
