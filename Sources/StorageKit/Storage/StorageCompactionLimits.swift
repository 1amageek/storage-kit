/// Backend limits for one physical storage compaction slice.
public struct StorageCompactionLimits: Sendable, Hashable {
    public let maximumWorkUnitsPerSlice: UInt64

    public init(maximumWorkUnitsPerSlice: UInt64) {
        precondition(maximumWorkUnitsPerSlice > 0)
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }
}
