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
    /// Native layer value of a node this catalog initialized as a storage root
    /// (FD-1).
    ///
    /// The native layer creates the ancestors of a path as ordinary
    /// empty-layer Directories, so a node at the configured root path can
    /// exist because an unrelated engine opened a path through it. Only root
    /// initialization writes this value, which is what makes it the witness
    /// that the node is a storage root rather than such an ancestor.
    ///
    /// The value is reserved in both directions of this mapping, so it never
    /// enters the tag space a caller works with: `nativeType(for:)` refuses a
    /// caller tag equal to it, and `layerTag(for:)` refuses a node that
    /// carries it.
    static let rootLayer = DirectoryType.custom("storage-kit")

    /// Native path of `address` below `rootPath`.
    static func nativePath(rootPath: [String], address: StorageAddress) -> [String] {
        rootPath + address.components
    }

    /// Native layer type for `layer`.
    ///
    /// `nil` types a plain Directory: the native layer stores no layer value
    /// for such a node, and `LayerTag.default` is the empty tag. Every other
    /// tag is application-opaque bytes (SPEC §4) and the native layer stores
    /// it verbatim, so the root marker is the only rejected tag: writing it
    /// onto a child would place a node that every engine reads as a storage
    /// root below one that already is.
    static func nativeType(
        for layer: LayerTag,
        operation: StorageOperation,
        backend: StorageBackend
    ) throws -> DirectoryType? {
        if layer.isDefault {
            return nil
        }
        if layer.isPartition {
            return .partition
        }
        let type = DirectoryType(rawValue: Array(layer.bytes))
        guard type != rootLayer else {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: backend,
                message: "Layer tag '\(rootLayer.description)' is reserved for the storage root"
            )
        }
        return type
    }

    /// StorageKit layer tag of a resolved native node.
    ///
    /// A node carrying the root marker is a storage root, and this call
    /// resolves nodes that are not one, so the marker is refused here rather
    /// than handed back as a tag no caller could write again.
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
        guard type != rootLayer else {
            throw StorageError.incompatibleStorageLayout(
                "a node below this root carries the storage root marker",
                operation: operation,
                backend: backend
            )
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
