/// Abstract protocol for KV storage backends.
///
/// Each backend (FoundationDB, SQLite, PostgreSQL, Cloudflare Durable Object,
/// InMemory) conforms to this protocol. It provides transaction creation,
/// the engine's Directory capability, and low-level execution hooks.
///
/// ## Initialization
///
/// All engines use a unified `Configuration`-based initialization:
/// ```swift
/// let engine = try await SomeEngine(configuration: .init(...))
/// ```
/// Non-async backends satisfy the `async throws` requirement
/// without actually suspending or throwing.
public protocol StorageEngine: AnyObject, Sendable {
    /// Backend-specific configuration type.
    associatedtype Configuration: Sendable

    associatedtype TransactionType: Transaction

    /// Create an engine with the given configuration.
    ///
    /// `async` because some backends (e.g. FDB) require asynchronous
    /// library initialization. Non-async implementations satisfy this
    /// requirement without suspending.
    init(configuration: Configuration) async throws

    /// Create a new transaction.
    func createTransaction() throws -> TransactionType

    /// Create a transaction whose commit and cancellation authority is exposed
    /// through the non-generic runtime boundary.
    ///
    /// Concrete backend APIs retain `createTransaction()` and its zero-copy
    /// associated result type. Runtime composition uses this requirement so an
    /// Embedded caller never invokes an associated-type-returning method on an
    /// existential engine.
    func createOwnedTransaction() throws -> any Transaction

    /// Identity of this engine instance.
    ///
    /// Every transaction, Directory, and Partition the engine produces belongs
    /// to this domain, and every Directory operation and lease rejects
    /// participants from another domain before performing I/O.
    var transactionDomain: StorageTransactionDomain { get }

    /// The engine's Directory capability and sole existence authority for its
    /// Directories and Partitions.
    var directoryAccess: any DirectoryAccess { get }

    /// Atomically closes admission for new work and starts backend cleanup.
    ///
    /// This synchronous entry point is suitable for `deinit` and other
    /// boundaries that cannot suspend. It is idempotent, but it does not claim
    /// that asynchronous backend cleanup has completed. Work admitted before
    /// the lifecycle transition may finish and may retain resources whose
    /// ownership was transferred to its transaction.
    func requestShutdown()

    /// Waits until the engine's backend-specific shutdown work has completed.
    ///
    /// Implementations must also request shutdown when no prior request exists,
    /// so this method is safe to call directly. Concurrent callers share one
    /// authoritative completion. Transaction-owned resources follow the
    /// transaction and cursor lifetime contracts rather than the engine object.
    func waitUntilShutdown() async

    /// Execute one transaction and own its commit or cancellation lifecycle.
    ///
    /// The result-free requirement is intentional: it remains callable through
    /// an existential `StorageEngine` in Embedded Swift. Typed result transport
    /// is provided by `StorageTransactionExecutor` above this boundary without
    /// copying database payloads.
    func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws
}

extension StorageEngine {
    /// Requests shutdown and awaits authoritative backend cleanup.
    public func shutdown() async {
        requestShutdown()
        await waitUntilShutdown()
    }

    public func createOwnedTransaction() throws -> any Transaction {
        try createTransaction()
    }

    /// Default lifecycle: one owned transaction, committed on success and
    /// cancelled on failure by `TransactionLifecycleOwner`.
    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let owner = TransactionLifecycleOwner(transaction: try createTransaction())
        try await owner.execute(operation)
    }

    /// Execute a transaction once.
    ///
    /// Automatically commits when the closure completes successfully.
    /// Higher-level frameworks own retry policy and should create a fresh
    /// transaction for each attempt.
    public func withTransaction<T: Sendable>(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> T
    ) async throws -> T {
        try await StorageTransactionExecutor(engine: self).withTransaction(operation)
    }

    /// Issues a lease for `partition` after resolving it in `transaction`.
    ///
    /// Validation walks the Partition's address through the catalog inside
    /// `transaction`: a missing node, a node that is no longer a Partition, or
    /// a different keyspace prefix means the Partition value is stale. The same
    /// walk runs again every time the lease is bound to a transaction, so a
    /// lease that outlives the transaction that issued it cannot carry a
    /// superseded Partition into a later one.
    public func leasePartition(
        _ partition: Partition,
        transaction: any TransactionReadAccess
    ) async throws -> PartitionLease {
        let backend = directoryAccess.backend
        guard partition.domain === transactionDomain else {
            throw StorageError.storageDomainMismatch(
                "Partition belongs to a different storage engine",
                operation: .open,
                backend: backend
            )
        }
        guard transaction.transactionDomain === transactionDomain else {
            throw StorageError.storageDomainMismatch(
                "Transaction belongs to a different storage engine",
                operation: .open,
                backend: backend
            )
        }
        try transactionDomain.requireLeaseIssuance(backend: backend)
        try await directoryAccess.requirePartitionGeneration(
            partition,
            operation: .open,
            transaction: transaction
        )
        return PartitionLease(
            partition: partition,
            directoryAccess: directoryAccess,
            registration: LeaseRegistration(address: partition.root.address),
            bounds: PartitionKeyBounds(partition: partition, backend: backend)
        )
    }
}
