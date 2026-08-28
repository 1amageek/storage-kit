/// Validation failure of a Directory name, Partition identifier, or address.
///
/// `DirectoryAccess` operations convert this error to
/// `StorageError.Code.invalidDirectoryAddress`.
public enum DirectoryAddressError: Error, Sendable, Hashable, CustomStringConvertible {
    case emptyPath
    case emptyComponent
    case componentTooLong(byteCount: Int)
    case emptyPartitionID
    case partitionIDTooLong(byteCount: Int)
    case depthExceeded(depth: Int)
    case partitionStepRequired
    case targetInsideMovedSubtree

    public var description: String {
        switch self {
        case .emptyPath:
            return "Directory path must contain at least one component"
        case .emptyComponent:
            return "Directory name component must not be empty"
        case .componentTooLong(let byteCount):
            return "Directory name component of \(byteCount) bytes exceeds \(DirectoryLimits.maximumComponentByteCount) bytes"
        case .emptyPartitionID:
            return "Partition identifier must not be empty"
        case .partitionIDTooLong(let byteCount):
            return "Partition identifier of \(byteCount) bytes exceeds \(DirectoryLimits.maximumPartitionIDByteCount) bytes"
        case .depthExceeded(let depth):
            return "Address depth \(depth) exceeds \(DirectoryLimits.maximumDepth)"
        case .partitionStepRequired:
            return "Partition address must end with the Partition's own identifier step"
        case .targetInsideMovedSubtree:
            return "Move target lies inside the moved Directory's own subtree"
        }
    }
}
