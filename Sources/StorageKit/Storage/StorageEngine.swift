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
    /// FDB: DirectoryLayer (dynamic prefix allocation).
    /// Non-FDB: StaticDirectoryService (directly converts paths via Tuple encoding).
    var directoryService: any DirectoryService { get }
}

extension StorageEngine {
    /// Default: StaticDirectoryService (directly converts paths to Subspace via Tuple encoding).
    public var directoryService: any DirectoryService { StaticDirectoryService() }
}
