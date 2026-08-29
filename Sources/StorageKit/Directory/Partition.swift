import DatabaseTypes

/// A Directory node whose layer tag is `LayerTag.partition`.
///
/// A Partition owns one contiguous keyspace: every descendant Directory,
/// Partition, Subspace, and key lies inside `[keyspacePrefix, strinc(...))`.
public struct Partition: Sendable, Hashable {
    public let root: Directory

    /// Fails when `root` is not tagged as a Partition.
    public init?(_ root: Directory) {
        guard root.layer.isPartition else {
            return nil
        }
        self.root = root
    }

    public var domain: StorageTransactionDomain { root.domain }

    public var address: StorageAddress { root.address }

    /// Exact name of this Partition in its parent Directory Layer.
    public var name: String? { root.address.lastComponent }

    /// Start of the whole Partition keyspace, including its nested layer.
    public var keyspacePrefix: ByteString { root.keyspacePrefix }
}
