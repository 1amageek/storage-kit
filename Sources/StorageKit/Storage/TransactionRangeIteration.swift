import DatabaseTypes

/// Streams a storage range through a non-generic transaction boundary.
///
/// Key and value owners are forwarded directly from the backend cursor for the
/// duration of each call. No intermediate range collection is materialized.
public enum TransactionRangeIteration {
    public static func forEach(
        in transaction: any TransactionReadAccess,
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll,
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        var cursor = transaction.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
        try await cursor.consume(body)
    }
}

public extension TransactionReadAccess {
    /// Streams a range without materializing an owned collection.
    func forEachInRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll,
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        try await TransactionRangeIteration.forEach(
            in: self,
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode,
            body
        )
    }
}
