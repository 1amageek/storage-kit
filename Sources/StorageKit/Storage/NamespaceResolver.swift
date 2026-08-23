/// Resolves hierarchical storage namespace paths into key subspaces.
///
/// Every storage backend provides this capability. A resolver may persist
/// namespace metadata when creating a path, or it may derive the mapping
/// deterministically when every valid path is intrinsically resolvable.
public protocol NamespaceResolver: Sendable {
    /// Resolves a namespace, creating its metadata when the backend requires it.
    func resolveOrCreate(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace

    /// Resolves a namespace without creating backend metadata.
    func resolveExisting(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace

    /// Returns whether the path can be resolved without creating metadata.
    func namespaceExists(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Bool
}
