/// An isolated Partition: its identifier and the Directory that is its root.
///
/// `root.address` ends with `.partition(id)`.
public struct Partition: Sendable, Hashable {
    public let id: PartitionID
    public let root: Directory

    public init(id: PartitionID, root: Directory) {
        self.id = id
        self.root = root
    }

    public var domain: StorageTransactionDomain { root.domain }

    /// Whether `root.address` names this Partition as its last step.
    public var hasConsistentAddress: Bool {
        root.address.lastStep == .partition(id)
    }
}
