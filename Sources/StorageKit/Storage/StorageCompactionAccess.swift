/// Transaction-scoped access to physical storage compaction.
///
/// A concrete backend creates this value so callers do not need runtime casts
/// after a transaction crosses the storage abstraction boundary.
public struct StorageCompactionAccess: Sendable {
    public let limits: StorageCompactionLimits

    private let stageSliceOperation: @Sendable (
        UInt64,
        StorageCompactionContinuation?
    ) async throws -> StorageCompactionResult

    public init(
        limits: StorageCompactionLimits,
        stageSlice: @escaping @Sendable (
            _ maximumWorkUnits: UInt64,
            _ continuation: StorageCompactionContinuation?
        ) async throws -> StorageCompactionResult
    ) {
        self.limits = limits
        self.stageSliceOperation = stageSlice
    }

    public func stageSlice(
        maximumWorkUnits: UInt64,
        continuation: StorageCompactionContinuation?
    ) async throws -> StorageCompactionResult {
        try await stageSliceOperation(maximumWorkUnits, continuation)
    }
}
