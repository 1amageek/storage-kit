/// Typed client capability used by the regular Swift facade to call a Durable Object storage endpoint.
public protocol CloudflareDurableObjectStorageClient: Sendable {
    func read(_ request: CloudflareDurableObjectReadRequest) async throws -> CloudflareDurableObjectReadResponse

    func range(_ request: CloudflareDurableObjectRangeRequest) async throws -> CloudflareDurableObjectRangeResponse

    func commit(_ request: CloudflareDurableObjectCommitRequest) async throws -> CloudflareDurableObjectCommitResponse

    func readiness(_ request: CloudflareDurableObjectReadinessRequest) async throws -> CloudflareDurableObjectReadinessResponse
}
