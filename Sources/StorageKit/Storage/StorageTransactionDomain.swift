/// Identifies the storage engine instance that owns a transaction.
///
/// Active transaction reuse is valid only when both transactions belong to
/// this exact domain. Reference identity intentionally defines equality.
public final class StorageTransactionDomain: Sendable {
    public init() {}
}
