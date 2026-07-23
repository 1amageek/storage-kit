/// Backend limits for one physical storage compaction slice.
public struct DatabaseStorageCompactionLimits: Sendable, Hashable {
    public let maximumWorkUnitsPerSlice: UInt64

    public init(maximumWorkUnitsPerSlice: UInt64) {
        precondition(maximumWorkUnitsPerSlice > 0)
        self.maximumWorkUnitsPerSlice = maximumWorkUnitsPerSlice
    }
}

/// Optional transaction capability for reclaiming unused physical storage.
///
/// Compaction is transaction-scoped so the physical backend work, framework job
/// state, result encoding, and idempotency record share one commit boundary.
/// Callers discover support by casting the active top-level transaction. A
/// conformer must reject nested transactions and must not commit or roll back
/// from inside `stageCompactionSlice`. Validation and backend capability
/// failures use `DatabaseStorageCompactionError`; transactional backend failures
/// retain `StorageError`, and task cancellation retains `CancellationError` so
/// the transaction owner can apply the correct retry policy.
public protocol DatabaseStorageCompactionTransaction: TransactionAccess {
    var compactionLimits: DatabaseStorageCompactionLimits { get }

    /// Stages physical work and returns a provisional result that is authoritative
    /// only after the surrounding transaction commits.
    func stageCompactionSlice(
        maximumWorkUnits: UInt64,
        continuation: DatabaseStorageCompactionContinuation?
    ) async throws -> DatabaseStorageCompactionResult
}
