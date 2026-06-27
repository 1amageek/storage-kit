import StorageKitEmbeddedCore

/// Minimal Embedded Swift runtime marker for Cloudflare Durable Object storage.
public enum CloudflareDurableObjectEmbeddedRuntime {
    public static let protocolVersion: UInt8 = 1

    public static func encode(
        _ request: CloudflareDurableObjectEmbeddedRequest
    ) throws(CloudflareDurableObjectEmbeddedError) -> [UInt8] {
        var writer = EmbeddedBinaryWriter()
        writer.writeUInt8(protocolVersion)
        try request.encode(into: &writer)
        return writer.bytes
    }

    public static func decodeRequest(
        _ bytes: [UInt8]
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedRequest {
        var reader = EmbeddedBinaryReader(bytes)
        let version = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard version == protocolVersion else {
            throw CloudflareDurableObjectEmbeddedError.unsupportedProtocolVersion(version)
        }
        let request = try CloudflareDurableObjectEmbeddedRequest(from: &reader)
        try CloudflareDurableObjectEmbeddedError.ensureFullyRead(reader)
        return request
    }

    public static func encode(
        _ response: CloudflareDurableObjectEmbeddedResponse
    ) throws(CloudflareDurableObjectEmbeddedError) -> [UInt8] {
        var writer = EmbeddedBinaryWriter()
        writer.writeUInt8(protocolVersion)
        try response.encode(into: &writer)
        return writer.bytes
    }

    public static func decodeResponse(
        _ bytes: [UInt8]
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedResponse {
        var reader = EmbeddedBinaryReader(bytes)
        let version = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard version == protocolVersion else {
            throw CloudflareDurableObjectEmbeddedError.unsupportedProtocolVersion(version)
        }
        let response = try CloudflareDurableObjectEmbeddedResponse(from: &reader)
        try CloudflareDurableObjectEmbeddedError.ensureFullyRead(reader)
        return response
    }

    public static func validateMutationRoundTrip(
        _ mutationType: EmbeddedMutationType
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedMutationType {
        var writer = EmbeddedBinaryWriter()
        mutationType.encode(into: &writer)
        var reader = EmbeddedBinaryReader(writer.bytes)
        return try CloudflareDurableObjectEmbeddedError.validateMutationRoundTrip(
            mutationType,
            reader: &reader
        )
    }

    public static func apply(
        committedRows: [EmbeddedKeyValue],
        writes: [EmbeddedWriteOperation],
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
        reverse: Bool,
        limit: Int
    ) throws(CloudflareDurableObjectEmbeddedError) -> [EmbeddedKeyValue] {
        try CloudflareDurableObjectEmbeddedError.overlay(
            committedRows: committedRows,
            writes: writes,
            begin: begin,
            end: end,
            reverse: reverse,
            limit: limit
        )
    }
}
