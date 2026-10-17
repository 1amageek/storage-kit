import DatabaseTypes

/// One ordered mutation surviving until authoritative native replay.
public enum TransactionJournalMutation: Sendable {
    case set(key: ByteString, value: ByteString, suppressWriteConflict: Bool)
    case clear(key: ByteString, suppressWriteConflict: Bool)
    case clearRange(begin: ByteString, end: ByteString, suppressWriteConflict: Bool)
    case atomic(key: ByteString, parameter: ByteString, type: MutationType,
                suppressWriteConflict: Bool)
    case writeConflict(begin: ByteString, end: ByteString)
}
