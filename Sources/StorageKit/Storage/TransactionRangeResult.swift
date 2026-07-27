import DatabaseTypes
/// Backend-native range sequence with explicit asynchronous cleanup.
public protocol TransactionRangeResult: AsyncSequence, Sendable
where Element == (ByteString, ByteString), AsyncIterator: TransactionRangeIterator {}

extension TransactionRangeResult {
    /// Consumes the range and always awaits backend cleanup.
    ///
    /// `AsyncSequence` has no asynchronous early-exit hook. Transaction range
    /// consumers must therefore use this scoped operation instead of a bare
    /// `for await` loop whenever iteration can throw or stop before exhaustion.
    public func consumeRows(
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        var iterator = makeAsyncIterator()
        do {
            while let (key, value) = try await iterator.next() {
                try await body(key, value)
            }
        } catch {
            let iterationError = error
            do {
                try await iterator.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await iterator.finish()
    }
}
