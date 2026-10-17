import DatabaseTypes

/// Raw snapshot access without journal recursion or mutation-buffer flushing.
public protocol TransactionJournalBackendAccess: Sendable {
    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString?

    func rangeCursor(
        in range: TransactionJournalRange,
        reverse: Bool,
        snapshot: Bool,
        pageSize: Int,
        maximumByteCount: Int
    ) -> KeyValueCursor
}
