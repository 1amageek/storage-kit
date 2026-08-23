/// Administrative capability for stored hierarchical namespaces.
///
/// Backends that derive every namespace deterministically do not expose a
/// catalog because they have no independent namespace metadata to enumerate or
/// remove.
public protocol NamespaceCatalog: Sendable {
    /// Lists direct child namespace names under `path`.
    func listNamespaces(
        path: [String],
        transaction: any TransactionAccess
    ) async throws -> [String]

    /// Removes the namespace metadata and contents at `path`.
    func removeNamespace(
        path: [String],
        transaction: any TransactionAccess
    ) async throws
}
