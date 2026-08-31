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
    /// Identifier this catalog records in the witness slot of its root node
    /// (SPEC 8.7, FD-1).
    ///
    /// The slot itself belongs to the native Directory Layer: the bindings own
    /// its physical coordinate and resolve it from a path, so this adapter
    /// contributes only the value's meaning. Existence of the node is not the
    /// witness, because the native layer creates the ancestors of a path as
    /// ordinary untyped Directories, so a node at the configured root path can
    /// exist because an unrelated engine opened a path through it.
    ///
    /// The slot is adjudicated by what it holds rather than by its presence,
    /// because a raw Layer 0 transaction can write anywhere. No legitimate
    /// content key or range reaches the slot, so a caller's `clearRange` over
    /// the root's own content prefix leaves it intact; bytes that are not this
    /// exact identifier are corruption, not an initialized root.
    ///
    /// The encoding is the StorageKit Tuple, the same one the key-value
    /// catalogs witness their roots with, and not the native layer's own.
    static var rootWitnessIdentifier: ByteString {
        StorageKit.Tuple(rootWitnessName).pack()
    }

    private static let rootWitnessName = "storage-kit"

    /// Native path of `address` below `rootPath`.
    static func nativePath(rootPath: [String], address: StorageAddress) -> [String] {
        rootPath + address.components
    }

    /// Native layer type for `layer`.
    ///
    /// `nil` types a plain Directory: the native layer stores no layer value
    /// for such a node, and `LayerTag.default` is the empty tag. Every other
    /// tag is application-opaque bytes (SPEC §4) and the native layer stores it
    /// verbatim. No value is reserved: the root witness of FD-1 lives in the
    /// slot the native layer reserves in a node's own metadata, not in any
    /// node's layer value, so a caller tag round-trips here exactly as it does
    /// on a key-value backend.
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
