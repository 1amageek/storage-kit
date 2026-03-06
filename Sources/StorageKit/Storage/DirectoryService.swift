/// Hierarchical namespace management service.
///
/// Abstracts functionality equivalent to FDB's DirectoryLayer.
/// Converts path-based namespaces to Subspace.
///
/// ## Backend-independent usage
///
/// Higher-level frameworks (e.g. database-kit) call `directoryService` on any
/// `StorageEngine` to resolve directory paths into `Subspace` instances,
/// regardless of the underlying backend.
///
/// - **FDB**: `FDBDirectoryService` uses the DirectoryLayer with HCA to
///   dynamically allocate short prefixes.
/// - **SQLite / InMemory**: `StaticDirectoryService` converts paths
///   deterministically via Tuple encoding. No dynamic allocation occurs,
///   but the same API is used so that callers remain backend-agnostic.
public protocol DirectoryService: Sendable {
    /// Create or open a Subspace corresponding to a path.
    ///
    /// - Parameter path: Hierarchical path (e.g. ["User", "email_index"]).
    /// - Returns: The Subspace corresponding to the path.
    func createOrOpen(path: [String]) async throws -> Subspace

    /// List subdirectory names under a path.
    func list(path: [String]) async throws -> [String]

    /// Remove the directory corresponding to a path.
    func remove(path: [String]) async throws

    /// Check whether a directory corresponding to a path exists.
    func exists(path: [String]) async throws -> Bool
}

extension DirectoryService {
    public func list(path: [String]) async throws -> [String] { [] }
    public func remove(path: [String]) async throws {}
    public func exists(path: [String]) async throws -> Bool { false }
}

/// Static directory service for non-FDB backends.
///
/// Converts paths directly to Subspace via Tuple encoding.
/// Does not perform dynamic prefix allocation like FDB's DirectoryLayer.
/// Since the mapping is deterministic, `exists` always returns true
/// and `remove` is a no-op.
///
/// Higher-level frameworks call `StorageEngine.directoryService` to obtain
/// a `Subspace` for each model type. This struct ensures that the same
/// code path works on SQLite and InMemory backends without modification.
public struct StaticDirectoryService: DirectoryService, Sendable {
    public init() {}

    public func createOrOpen(path: [String]) async throws -> Subspace {
        Subspace(Tuple(path.map { $0 as any TupleElement }))
    }

    public func exists(path: [String]) async throws -> Bool { true }
}
