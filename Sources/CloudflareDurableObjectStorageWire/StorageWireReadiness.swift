
public struct StorageWireReadinessRequest: Sendable, Hashable {
    public let partitionIdentity: StoragePartitionIdentity

    public init(partitionIdentity: StoragePartitionIdentity) {
        self.partitionIdentity = partitionIdentity
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        try partitionIdentity.encode(into: &writer)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.partitionIdentity = try StoragePartitionIdentity(from: &reader)
    }
}
