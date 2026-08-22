import DatabaseTypes
/// Complete read, mutation, and control access to one storage transaction.
///
/// Higher-level runtimes use this capability without acquiring authority to
/// commit or cancel the owning transaction.
///
/// ## Zero-copy design
/// Consumers that need less authority should depend on `TransactionReadAccess`,
/// `TransactionMutationAccess`, or `TransactionControlAccess` instead.
///
/// ## Backend implementation guide
/// Required methods: `getValue`, `rangeCursor`, `setValue`, `clear`,
/// `clearRange`, and `atomicOp`.
/// Others have default implementations provided via extension (non-FDB backends are automatically covered).
/// Every production backend must own one `TransactionMutationByteMeter` per
/// transaction attempt and record accepted writes before buffering or
/// dispatching them.
///
/// `atomicOp` must be implemented by every backend. Backends that support
/// versionstamp mutations materialize them with the transaction's commit version.
public protocol TransactionAccess:
    TransactionReadAccess,
    TransactionMutationAccess,
    TransactionControlAccess
{}

/// Owns the commit and cancellation lifecycle of one storage transaction.
///
/// Only the component coordinating retries and transaction completion should
/// receive this capability. Database semantics and index implementations
/// should depend on `TransactionAccess`.
public protocol Transaction: TransactionAccess {
    /// The storage engine instance that owns this transaction.
    var transactionDomain: StorageTransactionDomain { get }

    /// The authoritative storage failure recorded by this transaction.
    ///
    /// Higher layers use this state after an arbitrary operation error has
    /// crossed an untyped application boundary. Backends must record every
    /// `StorageError` before it escapes a transaction operation.
    var storageFailure: StorageError? { get }

    /// The configured portable logical mutation limit, if this transaction is
    /// bounded. A non-nil value remains attached to the owned transaction when
    /// it crosses task boundaries.
    var mutationByteLimit: Int? { get }

    /// Configures mutation admission before the transaction accepts writes.
    /// The lifecycle owner calls this before exposing transaction access.
    func configureMutationByteLimit(maximumBytes: Int?) throws

    /// Commit the transaction.
    func commit() async throws

    /// Cancel the transaction and wait until backend cleanup is authoritative.
    func cancel() async throws

    /// Get the committed version after a successful commit.
    func getCommittedVersion() throws -> Int64
}

extension Transaction {
    /// Custom backends fail closed when a bounded transaction is requested but
    /// they have not implemented transaction-owned mutation admission.
    public var mutationByteLimit: Int? { nil }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        guard maximumBytes == nil else {
            throw StorageError.unsupportedOperation(
                "Transaction backend does not implement mutation byte admission",
                operation: .beginTransaction
            )
        }
    }

    /// Default: the backend does not expose a committed version.
    public func getCommittedVersion() throws -> Int64 {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a committed version",
            operation: .read
        )
    }
}
