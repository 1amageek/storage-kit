import StorageKitEmbeddedCore

/// Top-level fixed binary response envelope.
public enum CloudflareDurableObjectEmbeddedResponse: Sendable, Hashable {
    case readiness(CloudflareDurableObjectEmbeddedReadinessResponse)
    case read(CloudflareDurableObjectEmbeddedReadResponse)
    case range(CloudflareDurableObjectEmbeddedRangeResponse)
    case commit(CloudflareDurableObjectEmbeddedCommitResponse)
    case failure(status: CloudflareDurableObjectEmbeddedStatusCode, message: String)

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        switch self {
        case .readiness(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.readiness.encode(into: &writer)
            response.encode(into: &writer)
        case .read(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.read.encode(into: &writer)
            try response.encode(into: &writer)
        case .range(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.range.encode(into: &writer)
            try response.encode(into: &writer)
        case .commit(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.commit.encode(into: &writer)
            response.encode(into: &writer)
        case .failure(let status, let message):
            writer.writeUInt8(status.rawValue)
            try CloudflareDurableObjectEmbeddedError.writeString(message, into: &writer)
        }
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        let statusRaw = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard let status = CloudflareDurableObjectEmbeddedStatusCode(rawValue: statusRaw) else {
            throw CloudflareDurableObjectEmbeddedError.unknownStatus(statusRaw)
        }
        guard status == .ok else {
            self = .failure(
                status: status,
                message: try CloudflareDurableObjectEmbeddedError.readString(from: &reader)
            )
            return
        }

        switch try CloudflareDurableObjectEmbeddedOperation(from: &reader) {
        case .readiness:
            self = .readiness(try CloudflareDurableObjectEmbeddedReadinessResponse(from: &reader))
        case .read:
            self = .read(try CloudflareDurableObjectEmbeddedReadResponse(from: &reader))
        case .range:
            self = .range(try CloudflareDurableObjectEmbeddedRangeResponse(from: &reader))
        case .commit:
            self = .commit(try CloudflareDurableObjectEmbeddedCommitResponse(from: &reader))
        }
    }
}
