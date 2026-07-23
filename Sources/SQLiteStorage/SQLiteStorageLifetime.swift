import Synchronization

/// Shares the open/closed lifecycle of one SQLite storage engine with its
/// transactions and connection lease scheduler.
final class SQLiteStorageLifetime: Sendable {
    private let closed = Mutex(false)

    var isClosed: Bool {
        closed.withLock { $0 }
    }

    func close() -> Bool {
        closed.withLock { value in
            guard !value else { return false }
            value = true
            return true
        }
    }
}
