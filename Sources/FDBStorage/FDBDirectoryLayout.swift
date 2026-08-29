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
    /// Native path of `address` below `rootPath`.
    static func nativePath(rootPath: [String], address: StorageAddress) -> [String] {
        rootPath + address.components
    }

    /// Native layer type for `layer`.
    ///
    /// `nil` types a plain Directory: the native layer stores no layer value
    /// for such a node, and `LayerTag.default` is the empty tag.
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
        guard let type = DirectoryType(rawValue: Array(layer.bytes)) else {
            throw StorageError(
                code: .invalidDirectoryAddress,
                operation: operation,
                backend: backend,
                message: "Layer tag is not valid UTF-8 and cannot be stored by the native Directory Layer"
            )
        }
        return type
    }

    /// StorageKit layer tag of a resolved native node.
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
