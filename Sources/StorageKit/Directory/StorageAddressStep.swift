/// One step of a `StorageAddress`: a child Directory name or a Partition.
public enum StorageAddressStep: Sendable, Hashable {
    case directory(String)
    case partition(PartitionID)

    package func validate() throws(DirectoryAddressError) {
        switch self {
        case .directory(let name):
            try DirectoryPath.validateComponent(name)
        case .partition:
            return
        }
    }
}
