import DatabaseTypes
import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

actor SuspendingCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport {
    nonisolated var callExecution: CloudflareDurableObjectCallExecution {
        .suspending
    }

    private var commitStarted = false
    private var commitStartWaiters: [CheckedContinuation<Void, Never>] = []

    func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        let request = try decodeStorageTransportRequest(requestBytes)
        switch request {
        case .readiness:
            return try encodeStorageTransportResponse(
                .readiness(
                    StorageWireReadinessResponse(
                        schemaVersion: 1,
                        commitVersion: 0,
                        metadataInitialized: true
                    )
                )
            )
        case .commit:
            signalCommitStarted()
            do {
                try await Task.sleep(for: .seconds(60))
            } catch {
                throw .cancelled
            }
            return try encodeStorageTransportResponse(
                .commit(StorageWireCommitResponse(committedVersion: 1))
            )
        default:
            return try encodeStorageTransportResponse(
                .failure(status: .invalidOperation, message: "Only commit is supported")
            )
        }
    }

    func waitUntilCommitStarts() async {
        guard !commitStarted else {
            return
        }
        await withCheckedContinuation { continuation in
            commitStartWaiters.append(continuation)
        }
    }

    private func signalCommitStarted() {
        commitStarted = true
        let waiters = commitStartWaiters
        commitStartWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume()
        }
    }
}
