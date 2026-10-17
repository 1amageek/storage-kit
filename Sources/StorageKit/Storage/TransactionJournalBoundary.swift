import DatabaseTypes

/// A physical bound that never resolves a selector against an incomplete view.
public enum TransactionJournalBoundary: Sendable, Hashable {
    case unbounded
    case inclusive(ByteString)
    case exclusive(ByteString)
}
