import DatabaseTypes
/// Backend-native range result with explicit asynchronous cleanup.
public protocol TransactionRangeResult: Sendable {
    associatedtype Cursor: TransactionRangeCursor

    func makeCursor() -> Cursor
}

extension TransactionRangeResult {
    /// Consumes the range and always awaits backend cleanup.
    ///
    /// Transaction range consumers use this scoped operation whenever
    /// iteration can throw or stop before exhaustion.
    public func consumeRows(
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        var cursor = makeCursor()
        do {
            while let (key, value) = try await cursor.next() {
                try await body(key, value)
            }
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await cursor.finish()
    }
}
