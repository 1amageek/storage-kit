import Synchronization

/// Dynamic authority for nested operations already admitted by a journal.
final class TransactionJournalOperation: Sendable {
    let journal: TransactionJournal
    private let active = Mutex(true)

    init(journal: TransactionJournal) { self.journal = journal }
    var isActive: Bool { active.withLock { $0 } }
    func finish() { active.withLock { $0 = false } }
}

enum TransactionJournalContext: Sendable {
    @TaskLocal static var operation: TransactionJournalOperation?
}
