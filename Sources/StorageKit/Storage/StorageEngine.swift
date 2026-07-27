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
public protocol StorageEngine: Sendable {
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

    /// Hierarchical namespace resolution capability.
    var namespaceResolver: any NamespaceResolver { get }

    /// Administrative namespace catalog when the backend stores independent
    /// namespace metadata.
    var namespaceCatalog: (any NamespaceCatalog)? { get }

    /// Release resources held by this engine.
    ///
    /// Called when the engine is no longer needed.
    /// Implementations should be idempotent (safe to call multiple times).
    /// Default implementation is a no-op.
    func shutdown()
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

    public func shutdown() {}

    /// Resolve or create a namespace in a one-shot transaction.
    ///
    /// Application write paths that already own a transaction must call the
    /// transaction-aware `NamespaceResolver` API directly so namespace metadata
    /// shares their commit boundary.
    public func resolveOrCreateNamespace(path: [String]) async throws -> Subspace {
        try await withTransaction { transaction in
            try await namespaceResolver.resolveOrCreate(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Resolve an existing namespace in a one-shot transaction.
    public func resolveExistingNamespace(path: [String]) async throws -> Subspace {
        try await withTransaction { transaction in
            try await namespaceResolver.resolveExisting(
                path: path,
                transaction: transaction
            )
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
        return try await withTransaction { transaction in
            return try await namespaceCatalog.listNamespaces(
                path: path,
                transaction: transaction
            )
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
        try await withTransaction { transaction in
            try await namespaceCatalog.removeNamespace(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Test namespace existence in a one-shot transaction.
    public func namespaceExists(path: [String]) async throws -> Bool {
        try await withTransaction { transaction in
            try await namespaceResolver.namespaceExists(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Execute a transaction once.
    ///
    /// Automatically commits when the closure completes successfully.
    /// Higher-level frameworks own retry policy and should create a fresh
    /// transaction for each attempt.
    public func withTransaction<T: Sendable>(
        _ operation: (any TransactionAccess) async throws -> T
    ) async throws -> T {
        let transaction = try createTransaction()
        return try await ActiveTransactionScope.withActiveTransaction(
            transaction
        ) { _ in
            do {
                let result = try await operation(transaction)
                try await transaction.commit()
                return result
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

}
