import StorageKit

/// Typed client capability used by the StorageKit facade to call a Durable Object storage endpoint.
public protocol CloudflareDurableObjectStorageClient: Sendable {
    var callExecution: CloudflareDurableObjectCallExecution { get }

    func read(_ request: CloudflareDurableObjectReadRequest) async throws -> CloudflareDurableObjectReadResponse

    func range(_ request: CloudflareDurableObjectRangeRequest) async throws -> CloudflareDurableObjectRangeResponse

    func commit(_ request: CloudflareDurableObjectCommitRequest) async throws -> CloudflareDurableObjectCommitResponse

    func readiness(_ request: CloudflareDurableObjectReadinessRequest) async throws -> CloudflareDurableObjectReadinessResponse

    func rangeSize(
        _ request: CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse

    func rangeSplitPoints(
        _ request: CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse
}
