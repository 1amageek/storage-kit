/// Versioned deterministic Durable Object name codec.
public struct CloudflareDurableObjectV1NameCodec: CloudflareDurableObjectNameCodec {
    public let version = "v1"
    public let maxNameBytes: Int

    public init(maxNameBytes: Int = CloudflareDurableObjectLimits.default.maxNameBytes) {
        self.maxNameBytes = maxNameBytes
    }

    public func name(for scope: CloudflareDurableObjectStorageScope) throws -> String {
        try routedName(for: scope).name
    }

    public func routedName(for scope: CloudflareDurableObjectStorageScope) throws -> CloudflareDurableObjectRoutedName {
        let databasePart = CloudflareDurableObjectBase64URL.encode(Array(scope.databaseID.utf8))
        let tenantPart = scope.tenantID.map { CloudflareDurableObjectBase64URL.encode(Array($0.utf8)) } ?? "_"
        let workspacePart = scope.workspaceID.map { CloudflareDurableObjectBase64URL.encode(Array($0.utf8)) } ?? "_"
        let canonicalName = "storage-kit/cfdo/v1/database/\(databasePart)/tenant/\(tenantPart)/workspace/\(workspacePart)"
        let actual = canonicalName.utf8.count
        guard actual <= maxNameBytes else {
            throw CloudflareDurableObjectNameCodecError.nameTooLong(limit: maxNameBytes, actual: actual)
        }
        return CloudflareDurableObjectRoutedName(
            scope: scope,
            name: canonicalName,
            canonicalName: canonicalName,
            codecVersion: version
        )
    }
}
