extension CloudflareDurableObjectStorageScope {
    public func durableObjectName(
        maximumBytes: Int = CloudflareDurableObjectLimits.default.maxNameBytes
    ) throws -> String {
        let databasePart = CloudflareDurableObjectBase64URL.encode(databaseID.utf8)
        let tenantPart = tenantID.map {
            CloudflareDurableObjectBase64URL.encode($0.utf8)
        } ?? "_"
        let workspacePart = workspaceID.map {
            CloudflareDurableObjectBase64URL.encode($0.utf8)
        } ?? "_"
        let canonicalName = "storage-kit/cfdo/v1/database/\(databasePart)/tenant/\(tenantPart)/workspace/\(workspacePart)"
        let actual = canonicalName.utf8.count
        guard actual <= maximumBytes else {
            throw CloudflareDurableObjectNameError.nameTooLong(
                limit: maximumBytes,
                actual: actual
            )
        }
        return canonicalName
    }
}
