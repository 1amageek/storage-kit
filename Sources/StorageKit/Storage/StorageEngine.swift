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

    /// Monotonic clock used for deadlines, retry backoff, and throttling.
    var monotonicClock: any StorageMonotonicClock { get }

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

    public var monotonicClock: any StorageMonotonicClock {
        SystemStorageClock()
    }

    public func shutdown() {}

    /// Resolve a directory in a one-shot transaction.
    ///
    /// Application write paths that already own a transaction must call the
    /// transaction-aware `DirectoryService` API directly so namespace metadata
    /// shares their commit boundary.
    public func createOrOpenDirectory(path: [String]) async throws -> Subspace {
        try await withTransaction { transaction in
            try await directoryService.createOrOpen(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Open a directory in a one-shot transaction.
    public func openDirectory(path: [String]) async throws -> Subspace {
        try await withTransaction { transaction in
            try await directoryService.open(
                path: path,
                transaction: transaction
            )
        }
    }

    /// List directories in a one-shot transaction.
    public func listDirectories(path: [String]) async throws -> [String] {
        try await withTransaction { transaction in
            try await directoryService.list(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Remove a directory in a one-shot transaction.
    public func removeDirectory(path: [String]) async throws {
        try await withTransaction { transaction in
            try await directoryService.remove(
                path: path,
                transaction: transaction
            )
        }
    }

    /// Test directory existence in a one-shot transaction.
    public func directoryExists(path: [String]) async throws -> Bool {
        try await withTransaction { transaction in
            try await directoryService.exists(
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
