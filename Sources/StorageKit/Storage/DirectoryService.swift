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
    func createOrOpen(
        path: [String],
        transaction: any Transaction
    ) async throws -> Subspace

    /// Open an existing Subspace without creating namespace metadata.
    func open(
        path: [String],
        transaction: any Transaction
    ) async throws -> Subspace

    /// List subdirectory names under a path.
    func list(
        path: [String],
        transaction: any Transaction
    ) async throws -> [String]

    /// Remove the directory corresponding to a path.
    func remove(
        path: [String],
        transaction: any Transaction
    ) async throws

    /// Check whether a directory corresponding to a path exists.
    func exists(
        path: [String],
        transaction: any Transaction
    ) async throws -> Bool
}

extension DirectoryService {
    public func list(
        path: [String],
        transaction: any Transaction
    ) async throws -> [String] {
        _ = path
        _ = transaction
        throw StorageError.unsupportedOperation(
            "This directory service cannot list namespaces",
            operation: .read
        )
    }

    public func remove(
        path: [String],
        transaction: any Transaction
    ) async throws {
        _ = path
        _ = transaction
        throw StorageError.unsupportedOperation(
            "This directory service cannot remove namespaces",
            operation: .delete
        )
    }

    public func exists(
        path: [String],
        transaction: any Transaction
    ) async throws -> Bool {
        _ = path
        _ = transaction
        throw StorageError.unsupportedOperation(
            "This directory service cannot test namespace existence",
            operation: .read
        )
    }
}

/// Static directory service for non-FDB backends.
///
/// Converts paths directly to Subspace via Tuple encoding.
/// Does not perform dynamic prefix allocation like FDB's DirectoryLayer.
/// Since the mapping is deterministic, `exists` always returns true
/// while namespace enumeration and removal are explicitly unsupported.
///
/// Higher-level frameworks call `StorageEngine.directoryService` to obtain
/// a `Subspace` for each model type. This struct ensures that the same
/// code path works on SQLite and InMemory backends without modification.
public struct StaticDirectoryService: DirectoryService, Sendable {
    public init() {}

    public func createOrOpen(
        path: [String],
        transaction: any Transaction
    ) async throws -> Subspace {
        _ = transaction
        return Subspace(Tuple(path.map { $0 as any TupleElement }))
    }

    public func open(
        path: [String],
        transaction: any Transaction
    ) async throws -> Subspace {
        _ = transaction
        return Subspace(Tuple(path.map { $0 as any TupleElement }))
    }

    public func list(
        path: [String],
        transaction: any Transaction
    ) async throws -> [String] {
        _ = path
        _ = transaction
        throw StorageError.unsupportedOperation(
            "Static directory namespaces are not enumerable",
            operation: .read
        )
    }

    public func remove(
        path: [String],
        transaction: any Transaction
    ) async throws {
        _ = path
        _ = transaction
        throw StorageError.unsupportedOperation(
            "Static directory namespaces cannot be removed independently",
            operation: .delete
        )
    }

    public func exists(
        path: [String],
        transaction: any Transaction
    ) async throws -> Bool {
        _ = path
        _ = transaction
        return true
    }
}
