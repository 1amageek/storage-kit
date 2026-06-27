import StorageKitEmbeddedCore

/// Top-level fixed binary request envelope.
public enum CloudflareDurableObjectEmbeddedRequest: Sendable, Hashable {
    case readiness(CloudflareDurableObjectEmbeddedReadinessRequest)
    case read(CloudflareDurableObjectEmbeddedReadRequest)
    case range(CloudflareDurableObjectEmbeddedRangeRequest)
    case commit(CloudflareDurableObjectEmbeddedCommitRequest)

    public var operation: CloudflareDurableObjectEmbeddedOperation {
        switch self {
        case .readiness:
            return .readiness
        case .read:
            return .read
        case .range:
            return .range
        case .commit:
            return .commit
        }
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
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
        }
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        switch try CloudflareDurableObjectEmbeddedOperation(from: &reader) {
        case .readiness:
            self = .readiness(try CloudflareDurableObjectEmbeddedReadinessRequest(from: &reader))
        case .read:
            self = .read(try CloudflareDurableObjectEmbeddedReadRequest(from: &reader))
        case .range:
            self = .range(try CloudflareDurableObjectEmbeddedRangeRequest(from: &reader))
        case .commit:
            self = .commit(try CloudflareDurableObjectEmbeddedCommitRequest(from: &reader))
        }
    }
}
