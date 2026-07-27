/// Optional transaction capability for reclaiming unused physical storage.
///
/// Compaction is transaction-scoped so physical maintenance and the caller's
/// storage writes share one commit boundary. Callers discover support by
/// casting the active top-level transaction. A conformer must reject nested
/// transactions and must not commit or roll back from inside
/// `stageCompactionSlice`. Validation and backend capability failures use
/// `StorageCompactionError`; transactional backend failures retain
/// `StorageError`, and task cancellation retains `CancellationError` so the
/// transaction owner can apply the correct retry policy.
public protocol StorageCompactionTransaction: TransactionAccess {
    var compactionLimits: StorageCompactionLimits { get }

    /// Stages physical work and returns a provisional result that is authoritative
    /// only after the surrounding transaction commits.
    func stageCompactionSlice(
        maximumWorkUnits: UInt64,
        continuation: StorageCompactionContinuation?
    ) async throws -> StorageCompactionResult
}
