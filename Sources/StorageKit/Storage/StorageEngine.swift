/// Abstract protocol for KV storage backends.
///
/// Each backend (FoundationDB, SQLite, InMemory) conforms to this protocol.
/// Provides transaction creation and low-level execution hooks.
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

    /// Hierarchical namespace resolution capability.
    var namespaceResolver: any NamespaceResolver { get }

    /// Administrative namespace catalog when the backend stores independent
    /// namespace metadata.
    var namespaceCatalog: (any NamespaceCatalog)? { get }

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

    /// Resolves or creates one logical namespace in an owned transaction.
    func resolveOrCreateNamespace(path: [String]) async throws -> Subspace

    /// Resolves one existing logical namespace in an owned transaction.
    func resolveExistingNamespace(path: [String]) async throws -> Subspace

    /// Lists direct children of one logical namespace.
    func listNamespaces(path: [String]) async throws -> [String]

    /// Removes one logical namespace.
    func removeNamespace(path: [String]) async throws

    /// Tests whether one logical namespace exists.
    func namespaceExists(path: [String]) async throws -> Bool
}

extension StorageEngine {
    /// Default deterministic namespace resolution.
    ///
    /// Non-FDB backends use this default. The deterministic Tuple encoding
    /// ensures that callers such as database-framework can resolve namespace paths
    /// without backend-specific logic.
    public var namespaceResolver: any NamespaceResolver {
        DeterministicNamespaceResolver()
    }

    public var namespaceCatalog: (any NamespaceCatalog)? { nil }

    /// Requests shutdown and awaits authoritative backend cleanup.
    public func shutdown() async {
        requestShutdown()
        await waitUntilShutdown()
    }

    public func createOwnedTransaction() throws -> any Transaction {
        try createTransaction()
    }

    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let transaction = try createTransaction()
        try await ActiveTransactionContext.withActiveTransaction(
            transaction
        ) { _ in
            do {
                try await operation(transaction)
                try await transaction.commit()
            } catch {
                let operationError = error
                do {
                    try await transaction.cancel()
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                }
                throw operationError
            }
        }
    }

    /// Resolve or create a namespace in a one-shot transaction.
    ///
    /// Application write paths that already own a transaction must call the
    /// transaction-aware `NamespaceResolver` API directly so namespace metadata
    /// shares their commit boundary.
    public func resolveOrCreateNamespace(path: [String]) async throws -> Subspace {
        let transaction = try createOwnedTransaction()
        do {
            let subspace = try await namespaceResolver.resolveOrCreate(
                path: path,
                transaction: transaction
            )
            try await transaction.commit()
            return subspace
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    /// Resolve an existing namespace in a one-shot transaction.
    public func resolveExistingNamespace(path: [String]) async throws -> Subspace {
        let transaction = try createOwnedTransaction()
        do {
            let subspace = try await namespaceResolver.resolveExisting(
                path: path,
                transaction: transaction
            )
            try await transaction.commit()
            return subspace
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    /// List child namespaces in a one-shot transaction.
    public func listNamespaces(path: [String]) async throws -> [String] {
        guard let namespaceCatalog else {
            throw StorageError.unsupportedOperation(
                "This storage backend does not maintain a namespace catalog",
                operation: .read
            )
        }
        let transaction = try createOwnedTransaction()
        do {
            let names = try await namespaceCatalog.listNamespaces(
                path: path,
                transaction: transaction
            )
            try await transaction.commit()
            return names
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    /// Remove a namespace in a one-shot transaction.
    public func removeNamespace(path: [String]) async throws {
        guard let namespaceCatalog else {
            throw StorageError.unsupportedOperation(
                "This storage backend does not maintain a namespace catalog",
                operation: .delete
            )
        }
        let transaction = try createOwnedTransaction()
        do {
            try await namespaceCatalog.removeNamespace(
                path: path,
                transaction: transaction
            )
            try await transaction.commit()
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    /// Test namespace existence in a one-shot transaction.
    public func namespaceExists(path: [String]) async throws -> Bool {
        let transaction = try createOwnedTransaction()
        do {
            let exists = try await namespaceResolver.namespaceExists(
                path: path,
                transaction: transaction
            )
            try await transaction.commit()
            return exists
        } catch {
            let operationError = error
            do {
                try await transaction.cancel()
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
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

}
