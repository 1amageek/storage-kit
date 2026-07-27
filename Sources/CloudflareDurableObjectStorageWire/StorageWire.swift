import DatabaseTypes

/// The canonical binary representation of the Cloudflare storage protocol.
public enum StorageWire {
    public static let protocolVersion: UInt8 = 1

    public static func encode(
        _ request: StorageWireRequest
    ) throws(StorageWireProtocolError) -> ByteString {
        try StorageWireValidator.validate(request)
        let bytes = try StorageWireWriter.encode {
            (writer: inout StorageWireWriter) throws(StorageWireProtocolError) in
            writer.writeUInt8(protocolVersion)
            try request.encode(into: &writer)
        }
        try StorageWireValidator.validateFrameBytes(bytes)
        return bytes
    }

    public static func decodeRequest(
        _ bytes: ByteString
    ) throws(StorageWireProtocolError) -> StorageWireRequest {
        try StorageWireValidator.validateFrameBytes(bytes)
        var reader = StorageWireReader(bytes)
        let version = try StorageWireProtocolError.readUInt8(from: &reader)
        guard version == protocolVersion else {
            throw StorageWireProtocolError.unsupportedProtocolVersion(version)
        }
        let request = try StorageWireRequest(from: &reader)
        try StorageWireProtocolError.ensureFullyRead(reader)
        return request
    }

    public static func encode(
        _ response: StorageWireResponse
    ) throws(StorageWireProtocolError) -> ByteString {
        try StorageWireValidator.validate(response)
        let bytes = try StorageWireWriter.encode {
            (writer: inout StorageWireWriter) throws(StorageWireProtocolError) in
            writer.writeUInt8(protocolVersion)
            try response.encode(into: &writer)
        }
        try StorageWireValidator.validateFrameBytes(bytes)
        return bytes
    }

    public static func decodeResponse(
        _ bytes: ByteString
    ) throws(StorageWireProtocolError) -> StorageWireResponse {
        try StorageWireValidator.validateFrameBytes(bytes)
        var reader = StorageWireReader(bytes)
        let version = try StorageWireProtocolError.readUInt8(from: &reader)
        guard version == protocolVersion else {
            throw StorageWireProtocolError.unsupportedProtocolVersion(version)
        }
        let response = try StorageWireResponse(from: &reader)
        try StorageWireProtocolError.ensureFullyRead(reader)
        return response
    }

    public static func decodeRequest(
        _ bytes: [UInt8]
    ) throws(StorageWireProtocolError) -> StorageWireRequest {
        try decodeRequest(ByteString(bytes))
    }

    public static func decodeResponse(
        _ bytes: [UInt8]
    ) throws(StorageWireProtocolError) -> StorageWireResponse {
        try decodeResponse(ByteString(bytes))
    }

}
