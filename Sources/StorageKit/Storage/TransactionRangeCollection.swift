import DatabaseTypes

/// Materializes a storage range at an explicit ownership boundary.
///
/// Streaming execution should retain `KeyValueCursor`. This operation exists
/// for consumers that require an owned collection and deliberately performs
/// the payload copies implied by that result type.
public enum TransactionRangeCollection {
    public static func collect(
        using transaction: any TransactionReadAccess,
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) async throws -> [(ByteString, ByteString)] {
        var rows: [(ByteString, ByteString)] = []
        var cursor = transaction.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
        try await cursor.consume { key, value in
            rows.append((key, value))
        }
        return rows
    }
}

public extension TransactionReadAccess {
    /// Materializes a selector-bounded range at an explicit ownership boundary.
    func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) async throws -> [(ByteString, ByteString)] {
        try await TransactionRangeCollection.collect(
            using: self,
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }

    /// Materializes a half-open key range at an explicit ownership boundary.
    func collectRange(
        begin: ByteString,
        end: ByteString,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) async throws -> [(ByteString, ByteString)] {
        try await collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}
