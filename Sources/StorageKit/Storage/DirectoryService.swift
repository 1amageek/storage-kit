/// Hierarchical namespace management service.
///
/// Abstracts functionality equivalent to FDB's DirectoryLayer.
/// Converts path-based namespaces to Subspace.
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

/// Static directory service (for non-FDB backends).
///
/// Converts paths directly to Subspace via Tuple encoding.
/// Does not perform dynamic prefix allocation like FDB's DirectoryLayer.
public struct StaticDirectoryService: DirectoryService, Sendable {
    public init() {}

    public func createOrOpen(path: [String]) async throws -> Subspace {
        Subspace(Tuple(path.map { $0 as any TupleElement }))
    }
}
