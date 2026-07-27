import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

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
        _ request: StorageWireReadRequest
    ) async throws -> StorageWireReadResponse {
        try await recordAndSuspendIfNeeded(.read)
        return StorageWireReadResponse(
            value: nil,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func range(
        _ request: StorageWireRangeRequest
    ) async throws -> StorageWireRangeResponse {
        try await recordAndSuspendIfNeeded(.range)
        return StorageWireRangeResponse(
            rows: [],
            hasMore: false,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func commit(
        _ request: StorageWireCommitRequest
    ) async throws -> StorageWireCommitResponse {
        try await recordAndSuspendIfNeeded(.commit)
        return StorageWireCommitResponse(committedVersion: 1)
    }

    func readiness(
        _ request: StorageWireReadinessRequest
    ) async throws -> StorageWireReadinessResponse {
        try await recordAndSuspendIfNeeded(.readiness)
        return StorageWireReadinessResponse(
            schemaVersion: 1,
            commitVersion: 0,
            metadataInitialized: true
        )
    }

    func rangeSize(
        _ request: StorageWireRangeSizeRequest
    ) async throws -> StorageWireRangeSizeResponse {
        try await recordAndSuspendIfNeeded(.rangeSize)
        return StorageWireRangeSizeResponse(
            byteCount: 0,
            currentCommitVersion: request.expectedReadVersion ?? 0
        )
    }

    func rangeSplitPoints(
        _ request: StorageWireRangeSplitPointsRequest
    ) async throws -> StorageWireRangeSplitPointsResponse {
        try await recordAndSuspendIfNeeded(.rangeSplitPoints)
        return StorageWireRangeSplitPointsResponse(
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
