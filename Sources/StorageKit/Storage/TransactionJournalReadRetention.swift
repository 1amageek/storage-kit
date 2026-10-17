import DatabaseTypes
import Synchronization

/// Payload charges of a single overlay cursor, independent of journal entries.
final class TransactionJournalReadRetention: Sendable {
    private let activity: TransactionJournalActivity
    private let bytes = Mutex(0)

    init(activity: TransactionJournalActivity) { self.activity = activity }

    func retain(_ value: ByteString) throws -> ByteString {
        let (retained, count) = try activity.retaining(value)
        bytes.withLock { $0 += count }
        return retained
    }

    func release() {
        let count = bytes.withLock { value in
            let count = value
            value = 0
            return count
        }
        activity.release(count)
    }

    deinit { release() }
}
