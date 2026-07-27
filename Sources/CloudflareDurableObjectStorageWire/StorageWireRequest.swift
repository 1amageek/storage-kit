
/// Top-level fixed binary request envelope.
public enum StorageWireRequest: Sendable, Hashable {
    case readiness(StorageWireReadinessRequest)
    case read(StorageWireReadRequest)
    case range(StorageWireRangeRequest)
    case commit(StorageWireCommitRequest)
    case rangeSize(StorageWireRangeSizeRequest)
    case rangeSplitPoints(
        StorageWireRangeSplitPointsRequest
    )

    public var operation: StorageWireOperation {
        switch self {
        case .readiness:
            return .readiness
        case .read:
            return .read
        case .range:
            return .range
        case .commit:
            return .commit
        case .rangeSize:
            return .rangeSize
        case .rangeSplitPoints:
            return .rangeSplitPoints
        }
    }

    public func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        operation.encode(into: &writer)
        switch self {
        case .readiness(let request):
            try request.encode(into: &writer)
        case .read(let request):
            try request.encode(into: &writer)
        case .range(let request):
            try request.encode(into: &writer)
        case .commit(let request):
            try request.encode(into: &writer)
        case .rangeSize(let request):
            try request.encode(into: &writer)
        case .rangeSplitPoints(let request):
            try request.encode(into: &writer)
        }
    }

    public init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        switch try StorageWireOperation(from: &reader) {
        case .readiness:
            self = .readiness(try StorageWireReadinessRequest(from: &reader))
        case .read:
            self = .read(try StorageWireReadRequest(from: &reader))
        case .range:
            self = .range(try StorageWireRangeRequest(from: &reader))
        case .commit:
            self = .commit(try StorageWireCommitRequest(from: &reader))
        case .rangeSize:
            self = .rangeSize(
                try StorageWireRangeSizeRequest(
                    from: &reader
                )
            )
        case .rangeSplitPoints:
            self = .rangeSplitPoints(
                try StorageWireRangeSplitPointsRequest(
                    from: &reader
                )
            )
        }
    }
}
