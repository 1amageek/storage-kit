import CloudflareDurableObjectStorageWire

/// Retains a concrete Durable Object client and forwards its storage operations
/// across long-lived runtime ownership boundaries.
public final class CloudflareDurableObjectStorageClientComposition:
    CloudflareDurableObjectStorageClient,
    Sendable
{
    public let callExecution: CloudflareDurableObjectCallExecution

    private let readOperation:
        @Sendable (
            StorageWireReadRequest
        ) async throws -> StorageWireReadResponse
    private let rangeOperation:
        @Sendable (
            StorageWireRangeRequest
        ) async throws -> StorageWireRangeResponse
    private let commitOperation:
        @Sendable (
            StorageWireCommitRequest
        ) async throws -> StorageWireCommitResponse
    private let readinessOperation:
        @Sendable (
            StorageWireReadinessRequest
        ) async throws -> StorageWireReadinessResponse
    private let rangeSizeOperation:
        @Sendable (
            StorageWireRangeSizeRequest
        ) async throws -> StorageWireRangeSizeResponse
    private let rangeSplitPointsOperation:
        @Sendable (
            StorageWireRangeSplitPointsRequest
        ) async throws -> StorageWireRangeSplitPointsResponse

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
        _ request: StorageWireReadRequest
    ) async throws -> StorageWireReadResponse {
        try await readOperation(request)
    }

    public func range(
        _ request: StorageWireRangeRequest
    ) async throws -> StorageWireRangeResponse {
        try await rangeOperation(request)
    }

    public func commit(
        _ request: StorageWireCommitRequest
    ) async throws -> StorageWireCommitResponse {
        try await commitOperation(request)
    }

    public func readiness(
        _ request: StorageWireReadinessRequest
    ) async throws -> StorageWireReadinessResponse {
        try await readinessOperation(request)
    }

    public func rangeSize(
        _ request: StorageWireRangeSizeRequest
    ) async throws -> StorageWireRangeSizeResponse {
        try await rangeSizeOperation(request)
    }

    public func rangeSplitPoints(
        _ request: StorageWireRangeSplitPointsRequest
    ) async throws -> StorageWireRangeSplitPointsResponse {
        try await rangeSplitPointsOperation(request)
    }

}
