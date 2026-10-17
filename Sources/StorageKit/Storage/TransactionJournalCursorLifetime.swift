import Synchronization

/// Completion identity used to prune cursor registrations without a back-reference.
final class TransactionJournalCursorLifetime: Sendable {
    private let finished = Mutex(false)
    var isFinished: Bool { finished.withLock { $0 } }
    func finish() { finished.withLock { $0 = true } }
}
