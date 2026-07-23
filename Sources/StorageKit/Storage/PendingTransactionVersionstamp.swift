/// A transaction versionstamp requested before the transaction commits.
public protocol PendingTransactionVersionstamp: Sendable {
    var value: TransactionVersionstamp { get async throws }
}
