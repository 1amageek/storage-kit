/// Describes whether replaying an entire transaction is safe after a failure.
public enum StorageRetryDisposition: Sendable, Hashable {
    /// The backend guarantees that replaying the transaction cannot duplicate a
    /// successful commit.
    case safe

    /// The commit outcome is unknown. Replay is only valid when a higher layer
    /// provides an idempotency protocol that can resolve the original outcome.
    case requiresIdempotency

    /// Replaying the transaction is not valid for this error.
    case never
}
