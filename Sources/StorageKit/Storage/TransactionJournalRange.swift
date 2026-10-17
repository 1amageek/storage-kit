/// Physical edges used by checkpoint snapshot readers.
public struct TransactionJournalRange: Sendable, Hashable {
    public let lower: TransactionJournalBoundary
    public let upper: TransactionJournalBoundary

    public init(lower: TransactionJournalBoundary, upper: TransactionJournalBoundary) {
        self.lower = lower
        self.upper = upper
    }
}
