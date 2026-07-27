import CloudflareDurableObjectStorageWire
import StorageKit

/// Typed client capability used by the StorageKit facade to call a Durable Object storage endpoint.
public protocol CloudflareDurableObjectStorageClient: Sendable {
    var callExecution: CloudflareDurableObjectCallExecution { get }

    func read(_ request: StorageWireReadRequest) async throws -> StorageWireReadResponse

    func range(_ request: StorageWireRangeRequest) async throws -> StorageWireRangeResponse

    func commit(_ request: StorageWireCommitRequest) async throws -> StorageWireCommitResponse

    func readiness(_ request: StorageWireReadinessRequest) async throws
        -> StorageWireReadinessResponse

    func rangeSize(
        _ request: StorageWireRangeSizeRequest
    ) async throws -> StorageWireRangeSizeResponse

    func rangeSplitPoints(
        _ request: StorageWireRangeSplitPointsRequest
    ) async throws -> StorageWireRangeSplitPointsResponse
}
