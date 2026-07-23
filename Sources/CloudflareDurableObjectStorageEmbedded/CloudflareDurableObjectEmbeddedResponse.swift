import StorageKitEmbeddedCore

/// Top-level fixed binary response envelope.
public enum CloudflareDurableObjectEmbeddedResponse: Sendable, Hashable {
    case readiness(CloudflareDurableObjectEmbeddedReadinessResponse)
    case read(CloudflareDurableObjectEmbeddedReadResponse)
    case range(CloudflareDurableObjectEmbeddedRangeResponse)
    case commit(CloudflareDurableObjectEmbeddedCommitResponse)
    case rangeSize(CloudflareDurableObjectEmbeddedRangeSizeResponse)
    case rangeSplitPoints(
        CloudflareDurableObjectEmbeddedRangeSplitPointsResponse
    )
    case failure(
        status: CloudflareDurableObjectEmbeddedFailureStatus,
        message: String
    )

    public func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        switch self {
        case .readiness(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.readiness.encode(into: &writer)
            try response.encode(into: &writer)
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
            try response.encode(into: &writer)
        case .rangeSize(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.rangeSize.encode(into: &writer)
            try response.encode(into: &writer)
        case .rangeSplitPoints(let response):
            writer.writeUInt8(CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue)
            CloudflareDurableObjectEmbeddedOperation.rangeSplitPoints.encode(
                into: &writer
            )
            try response.encode(into: &writer)
        case .failure(let status, let message):
            writer.writeUInt8(status.rawValue)
            try CloudflareDurableObjectEmbeddedError.writeString(
                message,
                maximum: EmbeddedLimits.cloudflareDurableObject.maxErrorMessageBytes,
                into: &writer
            )
        }
    }

    public init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        let statusRaw = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard let status = CloudflareDurableObjectEmbeddedStatusCode(rawValue: statusRaw) else {
            throw CloudflareDurableObjectEmbeddedError.unknownStatus(statusRaw)
        }
        guard status == .ok else {
            guard let failureStatus = CloudflareDurableObjectEmbeddedFailureStatus(
                rawValue: statusRaw
            ) else {
                throw CloudflareDurableObjectEmbeddedError.unknownStatus(statusRaw)
            }
            self = .failure(
                status: failureStatus,
                message: try CloudflareDurableObjectEmbeddedError.readString(
                    from: &reader,
                    maximum: EmbeddedLimits.cloudflareDurableObject.maxErrorMessageBytes
                )
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
        case .rangeSize:
            self = .rangeSize(
                try CloudflareDurableObjectEmbeddedRangeSizeResponse(
                    from: &reader
                )
            )
        case .rangeSplitPoints:
            self = .rangeSplitPoints(
                try CloudflareDurableObjectEmbeddedRangeSplitPointsResponse(
                    from: &reader
                )
            )
        }
    }
}
