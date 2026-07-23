import Synchronization

/// Keeps one suspending FoundationDB operation admitted until explicit cleanup
/// or iterator release.
final class FDBStorageOperationLease: Sendable {
    private let released = Mutex(false)
    private let releaseOperation: @Sendable () -> Void

    init(releaseOperation: @escaping @Sendable () -> Void) {
        self.releaseOperation = releaseOperation
    }

    func release() {
        let shouldRelease = released.withLock { released in
            guard !released else { return false }
            released = true
            return true
        }
        if shouldRelease {
            releaseOperation()
        }
    }

    deinit {
        release()
    }
}
