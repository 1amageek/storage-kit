/// Resolves every valid namespace path through deterministic Tuple encoding.
///
/// No namespace metadata is stored. Consequently, every path exists by
/// definition and resolving an existing path is identical to resolving or
/// creating one.
public struct DeterministicNamespaceResolver: NamespaceResolver, Sendable {
    public init() {}

    public func resolveOrCreate(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        _ = transaction
        return subspace(for: path)
    }

    public func resolveExisting(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Subspace {
        _ = transaction
        return subspace(for: path)
    }

    public func namespaceExists(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> Bool {
        _ = path
        _ = transaction
        return true
    }

    private func subspace(for path: [String]) -> Subspace {
        Subspace(Tuple(path.map { $0 as any TupleElement }))
    }
}
