import Synchronization
import StorageKit

/// Keeps one suspending transaction operation admitted until explicit cleanup
/// or iterator release.
final class TransactionActivityLease: Sendable {
    private let released = Mutex(false)
    private let resumeOperation: @Sendable () throws -> Void
    private let pauseOperation: @Sendable () -> Void
    private let releaseOperation: @Sendable () -> Void

    init(
        resumeOperation: @escaping @Sendable () throws -> Void,
        pauseOperation: @escaping @Sendable () -> Void,
        releaseOperation: @escaping @Sendable () -> Void
    ) {
        self.resumeOperation = resumeOperation
        self.pauseOperation = pauseOperation
        self.releaseOperation = releaseOperation
    }

    func resume() throws {
        let isReleased = released.withLock { $0 }
        guard !isReleased else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .foundationDB,
                message: "FoundationDB range lease is closed"
            )
        }
        try resumeOperation()
    }

    func pause() {
        let isReleased = released.withLock { $0 }
        guard !isReleased else { return }
        pauseOperation()
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
