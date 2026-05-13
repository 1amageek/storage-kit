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

    /// Hierarchical namespace management service.
    ///
    /// Higher-level frameworks (e.g. database-kit) call this property to resolve
    /// model directory paths into `Subspace` instances, regardless of the backend.
    ///
    /// - FDB: `FDBDirectoryService` — dynamic prefix allocation via DirectoryLayer.
    /// - SQLite / InMemory: `StaticDirectoryService` — deterministic Tuple encoding.
    var directoryService: any DirectoryService { get }

    /// Release resources held by this engine.
    ///
    /// Called when the engine is no longer needed.
    /// Implementations should be idempotent (safe to call multiple times).
    /// Default implementation is a no-op.
    func shutdown()

    /// Execute a low-level operation in auto-commit mode (no explicit BEGIN/COMMIT).
    ///
    /// Each SQL statement commits individually via PostgreSQL's default
    /// auto-commit behavior. This eliminates 2 round-trips (BEGIN + COMMIT)
    /// compared to `withTransaction()`.
    ///
    /// Suitable for:
    /// - Single read operations that do not need a transaction runner
    /// - Backend-specific low-level probes
    ///
    /// NOT suitable for:
    /// - Multi-statement operations requiring atomicity
    /// - Operations that need read-your-writes across multiple keys
    func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T
}

extension StorageEngine {
    /// Default: `StaticDirectoryService`.
    ///
    /// Non-FDB backends use this default. The deterministic Tuple encoding
    /// ensures that callers (e.g. database-kit) can resolve directory paths
    /// without backend-specific logic.
    public var directoryService: any DirectoryService { StaticDirectoryService() }

    public func shutdown() {}

    /// Execute a transaction once.
    ///
    /// Automatically commits when the closure completes successfully.
    /// Higher-level frameworks own retry policy and should create a fresh
    /// transaction for each attempt.
    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let transaction = try createTransaction()
        return try await ActiveTransactionScope.$current.withValue(transaction) {
            do {
                let result = try await operation(transaction)
                try await transaction.commit()
                return result
            } catch {
                transaction.cancel()
                throw error
            }
        }
    }

    /// Default: runs the operation through a one-shot transaction.
    public func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        try await withTransaction(operation)
    }
}
