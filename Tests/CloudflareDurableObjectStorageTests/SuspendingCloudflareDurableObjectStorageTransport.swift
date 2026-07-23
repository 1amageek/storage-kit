import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore

actor SuspendingCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport {
    nonisolated var callExecution: CloudflareDurableObjectCallExecution {
        .suspending
    }

    private var commitStarted = false
    private var commitStartWaiters: [CheckedContinuation<Void, Never>] = []

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes {
        let request = try CloudflareDurableObjectStorageWireCodec.decodeRequest(requestBytes)
        switch request {
        case .readiness:
            return try CloudflareDurableObjectStorageWireCodec.encode(
                .readiness(
                    CloudflareDurableObjectEmbeddedReadinessResponse(
                        schemaVersion: 1,
                        commitVersion: 0,
                        metadataInitialized: true
                    )
                )
            )
        case .commit:
            signalCommitStarted()
            try await Task.sleep(for: .seconds(60))
            return try CloudflareDurableObjectStorageWireCodec.encode(
                .commit(CloudflareDurableObjectEmbeddedCommitResponse(committedVersion: 1))
            )
        default:
            return try CloudflareDurableObjectStorageWireCodec.encode(
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
