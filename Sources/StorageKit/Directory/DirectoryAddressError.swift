/// Validation failure of a Directory name, layer tag, or address.
///
/// `DirectoryAccess` operations convert this error to
/// `StorageError.Code.invalidDirectoryAddress`.
public enum DirectoryAddressError: Error, Sendable, Hashable, CustomStringConvertible {
    case emptyComponent
    case componentTooLong(byteCount: Int)
    case layerTagTooLong(byteCount: Int)
    case depthExceeded(depth: Int)
    case targetInsideMovedSubtree

    public var description: String {
        switch self {
        case .emptyComponent:
            return "Directory name component must not be empty"
        case .componentTooLong(let byteCount):
            return "Directory name component of \(byteCount) bytes exceeds \(DirectoryLimits.maximumComponentByteCount) bytes"
        case .layerTagTooLong(let byteCount):
            return "Layer tag of \(byteCount) bytes exceeds \(DirectoryLimits.maximumLayerTagByteCount) bytes"
        case .depthExceeded(let depth):
            return "Address depth \(depth) exceeds \(DirectoryLimits.maximumDepth)"
        case .targetInsideMovedSubtree:
            return "Move target lies inside the moved node's own subtree"
        }
    }
}
