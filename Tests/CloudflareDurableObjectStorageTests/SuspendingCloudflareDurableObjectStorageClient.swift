import CloudflareDurableObjectStorage

actor SuspendingCloudflareDurableObjectStorageClient: CloudflareDurableObjectStorageClient {
    nonisolated var callExecution: CloudflareDurableObjectCallExecution {
        .suspending
    }

    enum Operation: Sendable, Hashable {
        case read
        case range
        case commit
        case readiness
        case rangeSize
        case rangeSplitPoints
    }

    private let suspendedOperation: Operation
    private var invocationCounts: [Operation: Int] = [:]
    private var startedOperations: Set<Operation> = []
    private var startWaiters: [Operation: [CheckedContinuation<Void, Never>]] = [:]

    init(suspending operation: Operation) {
        self.suspendedOperation = operation
    }

    func read(
        _ request: CloudflareDurableObjectReadRequest
    ) async throws -> CloudflareDurableObjectReadResponse {
        try await recordAndSuspendIfNeeded(.read)
        return CloudflareDurableObjectReadResponse(
            value: nil,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func range(
        _ request: CloudflareDurableObjectRangeRequest
    ) async throws -> CloudflareDurableObjectRangeResponse {
        try await recordAndSuspendIfNeeded(.range)
        return CloudflareDurableObjectRangeResponse(
            rows: [],
            hasMore: false,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func commit(
        _ request: CloudflareDurableObjectCommitRequest
    ) async throws -> CloudflareDurableObjectCommitResponse {
        try await recordAndSuspendIfNeeded(.commit)
        return CloudflareDurableObjectCommitResponse(committedVersion: 1)
    }

    func readiness(
        _ request: CloudflareDurableObjectReadinessRequest
    ) async throws -> CloudflareDurableObjectReadinessResponse {
        try await recordAndSuspendIfNeeded(.readiness)
        return CloudflareDurableObjectReadinessResponse(
            schemaVersion: 1,
            commitVersion: 0,
            metadataInitialized: true
        )
    }

    func rangeSize(
        _ request: CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse {
        try await recordAndSuspendIfNeeded(.rangeSize)
        return CloudflareDurableObjectRangeSizeResponse(
            byteCount: 0,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func rangeSplitPoints(
        _ request: CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse {
        try await recordAndSuspendIfNeeded(.rangeSplitPoints)
        return CloudflareDurableObjectRangeSplitPointsResponse(
            splitPoints: [request.begin, request.end],
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func waitUntilStarted(_ operation: Operation) async {
        guard !startedOperations.contains(operation) else { return }
        await withCheckedContinuation { continuation in
            startWaiters[operation, default: []].append(continuation)
        }
    }

    func invocationCount(for operation: Operation) -> Int {
        invocationCounts[operation, default: 0]
    }

    private func recordAndSuspendIfNeeded(
        _ operation: Operation
    ) async throws {
        invocationCounts[operation, default: 0] += 1
        startedOperations.insert(operation)
        let waiters = startWaiters.removeValue(forKey: operation) ?? []
        for waiter in waiters {
            waiter.resume()
        }
        guard operation == suspendedOperation else { return }
        try await Task.sleep(for: .seconds(60))
    }
}
