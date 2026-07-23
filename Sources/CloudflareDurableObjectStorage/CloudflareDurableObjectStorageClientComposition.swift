/// Retains a concrete Durable Object client and forwards its storage operations
/// across long-lived runtime ownership boundaries.
public final class CloudflareDurableObjectStorageClientComposition:
    CloudflareDurableObjectStorageClient,
    Sendable {
    public let callExecution: CloudflareDurableObjectCallExecution

    private let readOperation: @Sendable (
        CloudflareDurableObjectReadRequest
    ) async throws -> CloudflareDurableObjectReadResponse
    private let rangeOperation: @Sendable (
        CloudflareDurableObjectRangeRequest
    ) async throws -> CloudflareDurableObjectRangeResponse
    private let commitOperation: @Sendable (
        CloudflareDurableObjectCommitRequest
    ) async throws -> CloudflareDurableObjectCommitResponse
    private let readinessOperation: @Sendable (
        CloudflareDurableObjectReadinessRequest
    ) async throws -> CloudflareDurableObjectReadinessResponse
    private let rangeSizeOperation: @Sendable (
        CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse
    private let rangeSplitPointsOperation: @Sendable (
        CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse

    public init<Client: CloudflareDurableObjectStorageClient>(_ client: Client) {
        self.callExecution = client.callExecution
        self.readOperation = { request in
            try await client.read(request)
        }
        self.rangeOperation = { request in
            try await client.range(request)
        }
        self.commitOperation = { request in
            try await client.commit(request)
        }
        self.readinessOperation = { request in
            try await client.readiness(request)
        }
        self.rangeSizeOperation = { request in
            try await client.rangeSize(request)
        }
        self.rangeSplitPointsOperation = { request in
            try await client.rangeSplitPoints(request)
        }
    }

    public func read(
        _ request: CloudflareDurableObjectReadRequest
    ) async throws -> CloudflareDurableObjectReadResponse {
        try await readOperation(request)
    }

    public func range(
        _ request: CloudflareDurableObjectRangeRequest
    ) async throws -> CloudflareDurableObjectRangeResponse {
        try await rangeOperation(request)
    }

    public func commit(
        _ request: CloudflareDurableObjectCommitRequest
    ) async throws -> CloudflareDurableObjectCommitResponse {
        try await commitOperation(request)
    }

    public func readiness(
        _ request: CloudflareDurableObjectReadinessRequest
    ) async throws -> CloudflareDurableObjectReadinessResponse {
        try await readinessOperation(request)
    }

    public func rangeSize(
        _ request: CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse {
        try await rangeSizeOperation(request)
    }

    public func rangeSplitPoints(
        _ request: CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse {
        try await rangeSplitPointsOperation(request)
    }

}
