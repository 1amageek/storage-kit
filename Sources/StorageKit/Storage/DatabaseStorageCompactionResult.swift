/// Result of one bounded storage compaction slice.
public struct DatabaseStorageCompactionResult: Sendable, Hashable {
    /// Physical work completed by this slice, expressed in backend work units.
    public let workUnitsConsumed: UInt64

    /// Backend work units known to remain immediately after this slice.
    public let remainingWorkUnits: UInt64

    /// Non-nil only when the caller must run another slice.
    public let continuation: DatabaseStorageCompactionContinuation?

    public init(
        workUnitsConsumed: UInt64,
        remainingWorkUnits: UInt64,
        continuation: DatabaseStorageCompactionContinuation?
    ) {
        self.workUnitsConsumed = workUnitsConsumed
        self.remainingWorkUnits = remainingWorkUnits
        self.continuation = continuation
    }

    public var isComplete: Bool {
        continuation == nil
    }
}
