import DatabaseTypes
import FoundationDB
import StorageKit

/// Mapping between StorageKit Directory values and native Directory Layer
/// values (SPEC §7.3).
///
/// The mapping is one to one and adds no encoding of its own. A StorageKit
/// address component is a native path component, a plain Directory is a native
/// node that carries no layer value, and a Partition is a native node with the
/// `partition` layer, which makes the Partition a nested Directory Layer whose
/// node subspace and content subspace lie inside the Partition prefix. Custom
/// layer types and kind-prefixed names are nonconforming, so this type does not
/// provide them.
///
/// StorageKit nodes live below the catalog's configured root path, so one
/// cluster can hold unrelated native directories beside them and two catalogs
/// with distinct root paths never observe each other's nodes.
enum FDBDirectoryLayout {
    /// Byte, inside the root node's own content prefix, that separates this
    /// adapter's root record from everything a caller can address (FD-1).
    ///
    /// `Directory.nodeSubspaceByte` is the byte the Directory component
    /// reserves above every Tuple type code, and every key a caller derives
    /// from a `Subspace` is Tuple-encoded, so no `StorageAddress` resolves into
    /// this region.
    private static let rootRecordByte: UInt8 = 0x72

    /// Key of the record that witnesses an initialized storage root.
    ///
    /// The native layer never hands out a prefix already in use, so the root
    /// node's content prefix belongs to this catalog alone. Existence of the
    /// node itself is not the witness: the native layer creates the ancestors
    /// of a path as ordinary untyped Directories, so a node at the configured
    /// root path can exist because an unrelated engine opened a path through
    /// it. Only root initialization writes this key.
    static func rootRecordKey(rootPrefix: ByteString) -> ByteString {
        rootPrefix
            .appending(Directory.nodeSubspaceByte)
            .appending(rootRecordByte)
    }

    /// Value this catalog writes at `rootRecordKey(rootPrefix:)`.
    ///
    /// The record is adjudicated by what it holds rather than by its presence,
    /// because a raw Layer 0 transaction can write anywhere. A key holding
    /// anything else is foreign data in the root, not an initialized root.
    ///
    /// The encoding is the StorageKit Tuple, the same one the key-value
    /// catalogs witness their roots with, and not the native layer's own.
    static var rootRecordValue: ByteString {
        StorageKit.Tuple(rootRecordName).pack()
    }

    private static let rootRecordName = "storage-kit"

    /// Whether `value` is the record this catalog writes for a storage root.
    ///
    /// Bytes that are not a Tuple, a Tuple of another shape, or a Tuple naming
    /// anything else are all foreign: the caller reports
    /// `incompatibleStorageLayout` rather than adopting or overwriting them.
    static func isRootRecord(_ value: ByteString) -> Bool {
        let elements: [any StorageKit.TupleElement]
        do {
            elements = try StorageKit.Tuple.unpack(from: value)
        } catch {
            return false
        }
        guard elements.count == 1, let name = elements[0] as? String else {
            return false
        }
        return name == rootRecordName
    }

    /// Native path of `address` below `rootPath`.
    static func nativePath(rootPath: [String], address: StorageAddress) -> [String] {
        rootPath + address.components
    }

    /// Native layer type for `layer`.
    ///
    /// `nil` types a plain Directory: the native layer stores no layer value
    /// for such a node, and `LayerTag.default` is the empty tag. Every other
    /// tag is application-opaque bytes (SPEC §4) and the native layer stores it
    /// verbatim. No value is reserved: the root record of FD-1 lives in the
    /// root node's content prefix, not in any node's layer value, so a caller
    /// tag round-trips here exactly as it does on a key-value backend.
    static func nativeType(for layer: LayerTag) -> DirectoryType? {
        if layer.isDefault {
            return nil
        }
        if layer.isPartition {
            return .partition
        }
        return DirectoryType(rawValue: Array(layer.bytes))
    }

    /// StorageKit layer tag of a resolved native node.
    ///
    /// The only failure is a native layer value longer than a `LayerTag`
    /// admits, which no StorageKit write can produce.
    static func layerTag(
        for type: DirectoryType?,
        operation: StorageOperation,
        backend: StorageBackend
    ) throws -> LayerTag {
        guard let type else {
            return .default
        }
        if case .partition = type {
            return .partition
        }
        switch Result(catching: { () throws(DirectoryAddressError) in
            try LayerTag(ByteString(type.rawValue))
        }) {
        case .success(let tag):
            return tag
        case .failure(let error):
            throw StorageError.invalidDirectoryAddress(
                error,
                operation: operation,
                backend: backend
            )
        }
    }
}
