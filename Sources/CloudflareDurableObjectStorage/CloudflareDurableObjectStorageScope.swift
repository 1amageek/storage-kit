/// Stable identity for one logical StorageKit database in Cloudflare Durable Objects.
public struct CloudflareDurableObjectStorageScope: Sendable, Hashable, Codable {
    public let databaseID: String
    public let tenantID: String?
    public let workspaceID: String?

    public init(
        databaseID: String,
        tenantID: String? = nil,
        workspaceID: String? = nil
    ) throws {
        self.databaseID = try Self.validated(databaseID, component: "databaseID")
        self.tenantID = try tenantID.map { try Self.validated($0, component: "tenantID") }
        self.workspaceID = try workspaceID.map { try Self.validated($0, component: "workspaceID") }
    }

    public var canonicalDescription: String {
        "databaseID=\(databaseID);tenantID=\(tenantID ?? "_");workspaceID=\(workspaceID ?? "_")"
    }

    private enum CodingKeys: String, CodingKey {
        case databaseID
        case tenantID
        case workspaceID
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let databaseID = try container.decode(String.self, forKey: .databaseID)
        let tenantID = try container.decodeIfPresent(String.self, forKey: .tenantID)
        let workspaceID = try container.decodeIfPresent(String.self, forKey: .workspaceID)
        do {
            try self.init(databaseID: databaseID, tenantID: tenantID, workspaceID: workspaceID)
        } catch {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "Invalid Cloudflare Durable Object storage scope",
                    underlyingError: error
                )
            )
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(databaseID, forKey: .databaseID)
        try container.encodeIfPresent(tenantID, forKey: .tenantID)
        try container.encodeIfPresent(workspaceID, forKey: .workspaceID)
    }

    private static func validated(_ value: String, component: String) throws -> String {
        guard !isASCIIBlank(value) else {
            throw CloudflareDurableObjectScopeValidationError.blankComponent(component)
        }
        for scalar in value.unicodeScalars where scalar.value < 0x20 || scalar.value == 0x7F {
            throw CloudflareDurableObjectScopeValidationError.controlCharacter(component: component)
        }
        return value
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
