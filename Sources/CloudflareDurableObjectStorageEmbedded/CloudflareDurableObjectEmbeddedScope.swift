import StorageKitEmbeddedCore

/// Stable logical database identity routed to one Durable Object.
public struct CloudflareDurableObjectEmbeddedScope: Sendable, Hashable {
    public let databaseID: String
    public let tenantID: String?
    public let workspaceID: String?

    public init(
        databaseID: String,
        tenantID: String? = nil,
        workspaceID: String? = nil
    ) throws(CloudflareDurableObjectEmbeddedError) {
        guard Self.isValid(databaseID), Self.isValidOptional(tenantID), Self.isValidOptional(workspaceID) else {
            throw CloudflareDurableObjectEmbeddedError.invalidScope
        }
        self.databaseID = databaseID
        self.tenantID = tenantID
        self.workspaceID = workspaceID
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try CloudflareDurableObjectEmbeddedError.writeString(databaseID, into: &writer)
        try Self.writeOptional(tenantID, into: &writer)
        try Self.writeOptional(workspaceID, into: &writer)
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        try self.init(
            databaseID: try CloudflareDurableObjectEmbeddedError.readString(from: &reader),
            tenantID: try Self.readOptional(from: &reader),
            workspaceID: try Self.readOptional(from: &reader)
        )
    }

    private static func writeOptional(
        _ value: String?,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.writeString(value, into: &writer)
        } else {
            writer.writeBool(false)
        }
    }

    private static func readOptional(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> String? {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        guard hasValue else {
            return nil
        }
        return try CloudflareDurableObjectEmbeddedError.readString(from: &reader)
    }

    private static func isValidOptional(_ value: String?) -> Bool {
        guard let value else {
            return true
        }
        return isValid(value)
    }

    private static func isValid(_ value: String) -> Bool {
        guard !isASCIIBlank(value) else {
            return false
        }
        for byte in value.utf8 where byte < 0x20 || byte == 0x7f {
            return false
        }
        return true
    }

    private static func isASCIIBlank(_ value: String) -> Bool {
        for byte in value.utf8 where !isASCIIWhitespace(byte) {
            return false
        }
        return true
    }

    private static func isASCIIWhitespace(_ byte: UInt8) -> Bool {
        switch byte {
        case 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20:
            return true
        default:
            return false
        }
    }
}
