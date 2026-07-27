
/// Top-level fixed binary response envelope.
public enum StorageWireResponse: Sendable, Hashable {
    case readiness(StorageWireReadinessResponse)
    case read(StorageWireReadResponse)
    case range(StorageWireRangeResponse)
    case commit(StorageWireCommitResponse)
    case rangeSize(StorageWireRangeSizeResponse)
    case rangeSplitPoints(
        StorageWireRangeSplitPointsResponse
    )
    case failure(
        status: StorageWireFailureStatus,
        message: String
    )

    public func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        switch self {
        case .readiness(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.readiness.encode(into: &writer)
            try response.encode(into: &writer)
        case .read(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.read.encode(into: &writer)
            try response.encode(into: &writer)
        case .range(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.range.encode(into: &writer)
            try response.encode(into: &writer)
        case .commit(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.commit.encode(into: &writer)
            try response.encode(into: &writer)
        case .rangeSize(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.rangeSize.encode(into: &writer)
            try response.encode(into: &writer)
        case .rangeSplitPoints(let response):
            writer.writeUInt8(StorageWireStatusCode.ok.rawValue)
            StorageWireOperation.rangeSplitPoints.encode(
                into: &writer
            )
            try response.encode(into: &writer)
        case .failure(let status, let message):
            writer.writeUInt8(status.rawValue)
            try StorageWireProtocolError.writeString(
                message,
                maximum: StorageWireLimits.cloudflareDurableObject.maxErrorMessageBytes,
                into: &writer
            )
        }
    }

    public init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        let statusRaw = try StorageWireProtocolError.readUInt8(from: &reader)
        guard let status = StorageWireStatusCode(rawValue: statusRaw) else {
            throw StorageWireProtocolError.unknownStatus(statusRaw)
        }
        guard status == .ok else {
            guard let failureStatus = StorageWireFailureStatus(
                rawValue: statusRaw
            ) else {
                throw StorageWireProtocolError.unknownStatus(statusRaw)
            }
            self = .failure(
                status: failureStatus,
                message: try StorageWireProtocolError.readString(
                    from: &reader,
                    maximum: StorageWireLimits.cloudflareDurableObject.maxErrorMessageBytes
                )
            )
            return
        }

        switch try StorageWireOperation(from: &reader) {
        case .readiness:
            self = .readiness(try StorageWireReadinessResponse(from: &reader))
        case .read:
            self = .read(try StorageWireReadResponse(from: &reader))
        case .range:
            self = .range(try StorageWireRangeResponse(from: &reader))
        case .commit:
            self = .commit(try StorageWireCommitResponse(from: &reader))
        case .rangeSize:
            self = .rangeSize(
                try StorageWireRangeSizeResponse(
                    from: &reader
                )
            )
        case .rangeSplitPoints:
            self = .rangeSplitPoints(
                try StorageWireRangeSplitPointsResponse(
                    from: &reader
                )
            )
        }
    }
}
