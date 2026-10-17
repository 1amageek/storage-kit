/// Explicit resource limits for one checkpointable transaction attempt.
public struct TransactionCheckpointLimits: Sendable, Hashable {
    public let maximumEntries: Int
    public let maximumDepth: Int
    public let maximumRetainedBytes: Int
    public let maximumWorkUnits: Int
    public let maximumReadPageRows: Int

    public init(
        maximumEntries: Int,
        maximumDepth: Int,
        maximumRetainedBytes: Int,
        maximumWorkUnits: Int,
        maximumReadPageRows: Int
    ) throws(TransactionCheckpointError) {
        guard maximumEntries > 0, maximumDepth > 0,
              maximumRetainedBytes > 0, maximumWorkUnits > 0,
              maximumReadPageRows > 0 else {
            throw .invalidLimits
        }
        self.maximumEntries = maximumEntries
        self.maximumDepth = maximumDepth
        self.maximumRetainedBytes = maximumRetainedBytes
        self.maximumWorkUnits = maximumWorkUnits
        self.maximumReadPageRows = maximumReadPageRows
    }
}
