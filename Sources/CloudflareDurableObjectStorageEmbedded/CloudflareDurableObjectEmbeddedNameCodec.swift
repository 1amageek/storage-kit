import StorageKitEmbeddedCore

/// Deterministic Durable Object name codec for Embedded scopes.
public enum CloudflareDurableObjectEmbeddedNameCodec {
    public static let version = "v1"
    private static let emptyMarker = "_"

    public static func name(for scope: CloudflareDurableObjectEmbeddedScope) -> String {
        let databasePart = component(scope.databaseID)
        let tenantPart = component(scope.tenantID)
        let workspacePart = component(scope.workspaceID)
        return "storage-kit/cfdo/v1/database/\(databasePart)/tenant/\(tenantPart)/workspace/\(workspacePart)"
    }

    private static func component(_ value: String?) -> String {
        guard let value else {
            return emptyMarker
        }
        return EmbeddedBase64URL.encode(Array(value.utf8))
    }
}
