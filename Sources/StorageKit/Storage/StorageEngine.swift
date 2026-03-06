/// Abstract protocol for KV storage backends.
///
/// Each backend (FoundationDB, SQLite, InMemory) conforms to this protocol.
/// Provides transaction creation and execution with retry logic.
public protocol StorageEngine: Sendable {
    associatedtype TransactionType: Transaction

    /// Create a new transaction.
    func createTransaction() throws -> TransactionType

    /// Execute a transaction with retry logic.
    ///
    /// Automatically retries on transaction conflict.
    /// Automatically commits when the closure completes successfully.
    func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T

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
}

extension StorageEngine {
    /// Default: `StaticDirectoryService`.
    ///
    /// Non-FDB backends use this default. The deterministic Tuple encoding
    /// ensures that callers (e.g. database-kit) can resolve directory paths
    /// without backend-specific logic.
    public var directoryService: any DirectoryService { StaticDirectoryService() }

    public func shutdown() {}
}
