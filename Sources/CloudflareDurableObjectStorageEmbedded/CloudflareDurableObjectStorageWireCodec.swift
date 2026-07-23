import StorageKitEmbeddedCore

/// Minimal Embedded Swift runtime marker for Cloudflare Durable Object storage.
public enum CloudflareDurableObjectStorageWireCodec {
    public static let protocolVersion: UInt8 = 1

    public static func encode(
        _ request: CloudflareDurableObjectEmbeddedRequest
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedBytes {
        try CloudflareDurableObjectStorageWireValidator.validate(request)
        let bytes = try EmbeddedWireWriter.encode {
            (writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) in
            writer.writeUInt8(protocolVersion)
            try request.encode(into: &writer)
        }
        try CloudflareDurableObjectStorageWireValidator.validateFrameBytes(bytes)
        return bytes
    }

    public static func decodeRequest(
        _ bytes: EmbeddedBytes
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedRequest {
        try CloudflareDurableObjectStorageWireValidator.validateFrameBytes(bytes)
        var reader = EmbeddedWireReader(bytes)
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
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedBytes {
        try CloudflareDurableObjectStorageWireValidator.validate(response)
        let bytes = try EmbeddedWireWriter.encode {
            (writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) in
            writer.writeUInt8(protocolVersion)
            try response.encode(into: &writer)
        }
        try CloudflareDurableObjectStorageWireValidator.validateFrameBytes(bytes)
        return bytes
    }

    public static func decodeResponse(
        _ bytes: EmbeddedBytes
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedResponse {
        try CloudflareDurableObjectStorageWireValidator.validateFrameBytes(bytes)
        var reader = EmbeddedWireReader(bytes)
        let version = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard version == protocolVersion else {
            throw CloudflareDurableObjectEmbeddedError.unsupportedProtocolVersion(version)
        }
        let response = try CloudflareDurableObjectEmbeddedResponse(from: &reader)
        try CloudflareDurableObjectEmbeddedError.ensureFullyRead(reader)
        return response
    }

    public static func decodeRequest(
        _ bytes: [UInt8]
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedRequest {
        try decodeRequest(EmbeddedBytes(bytes))
    }

    public static func decodeResponse(
        _ bytes: [UInt8]
    ) throws(CloudflareDurableObjectEmbeddedError) -> CloudflareDurableObjectEmbeddedResponse {
        try decodeResponse(EmbeddedBytes(bytes))
    }

    public static func validateMutationRoundTrip(
        _ mutationType: EmbeddedMutationType
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedMutationType {
        var writer = EmbeddedWireWriter()
        mutationType.encode(into: &writer)
        var reader = EmbeddedWireReader(writer.bytes)
        return try CloudflareDurableObjectEmbeddedError.validateMutationRoundTrip(
            mutationType,
            reader: &reader
        )
    }

    public static func apply(
        committedRows: [EmbeddedKeyValue],
        writes: [EmbeddedWriteOperation],
        begin: EmbeddedRangeBoundary,
        end: EmbeddedRangeBoundary,
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

    public static func apply(
        committedRows: [EmbeddedKeyValue],
        writes: [EmbeddedWriteOperation],
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
        reverse: Bool,
        limit: Int
    ) throws(CloudflareDurableObjectEmbeddedError) -> [EmbeddedKeyValue] {
        try apply(
            committedRows: committedRows,
            writes: writes,
            begin: .selector(begin),
            end: .selector(end),
            reverse: reverse,
            limit: limit
        )
    }
}
