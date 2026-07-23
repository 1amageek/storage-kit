/// Stable identity for one logical StorageKit database in Cloudflare Durable Objects.
public struct CloudflareDurableObjectStorageScope: Sendable, Hashable {
    public let databaseID: String
    public let tenantID: String?
    public let workspaceID: String?

    public init(
        databaseID: String,
        tenantID: String? = nil,
        workspaceID: String? = nil
    ) throws {
        let validatedDatabaseID = try Self.validated(
            databaseID,
            component: "databaseID"
        )
        let validatedTenantID = try tenantID.map {
            try Self.validated($0, component: "tenantID")
        }
        let validatedWorkspaceID = try workspaceID.map {
            try Self.validated($0, component: "workspaceID")
        }
        let canonicalNameBytes = Self.canonicalNameByteCount(
            databaseID: validatedDatabaseID,
            tenantID: validatedTenantID,
            workspaceID: validatedWorkspaceID
        )
        guard canonicalNameBytes <= 512 else {
            throw CloudflareDurableObjectScopeValidationError
                .canonicalNameTooLong(
                    actual: canonicalNameBytes,
                    maximum: 512
                )
        }
        self.databaseID = validatedDatabaseID
        self.tenantID = validatedTenantID
        self.workspaceID = validatedWorkspaceID
    }

    public var canonicalDescription: String {
        "databaseID=\(databaseID);tenantID=\(tenantID ?? "_");workspaceID=\(workspaceID ?? "_")"
    }

    private static func validated(_ value: String, component: String) throws -> String {
        let byteCount = value.utf8.count
        guard byteCount <= 512 else {
            throw CloudflareDurableObjectScopeValidationError
                .componentTooLong(
                    component: component,
                    actual: byteCount,
                    maximum: 512
                )
        }
        guard !isASCIIBlank(value) else {
            throw CloudflareDurableObjectScopeValidationError.blankComponent(component)
        }
        for scalar in value.unicodeScalars where scalar.value < 0x20 || scalar.value == 0x7F {
            throw CloudflareDurableObjectScopeValidationError.controlCharacter(component: component)
        }
        return value
    }

    private static func canonicalNameByteCount(
        databaseID: String,
        tenantID: String?,
        workspaceID: String?
    ) -> Int {
        let fixedCount =
            "storage-kit/cfdo/v1/database//tenant//workspace/".utf8.count
        return fixedCount
            + base64URLCount(databaseID.utf8.count)
            + optionalBase64URLCount(tenantID)
            + optionalBase64URLCount(workspaceID)
    }

    private static func optionalBase64URLCount(_ value: String?) -> Int {
        guard let value else {
            return 1
        }
        return base64URLCount(value.utf8.count)
    }

    private static func base64URLCount(_ byteCount: Int) -> Int {
        let completeGroups = byteCount / 3
        switch byteCount % 3 {
        case 0:
            return completeGroups * 4
        case 1:
            return completeGroups * 4 + 2
        default:
            return completeGroups * 4 + 3
        }
    }

    private static func isASCIIBlank(_ value: String) -> Bool {
        for scalar in value.unicodeScalars where !isASCIIWhitespace(scalar) {
            return false
        }
        return true
    }

    private static func isASCIIWhitespace(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.value {
        case 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20:
            return true
        default:
            return false
        }
    }
}
