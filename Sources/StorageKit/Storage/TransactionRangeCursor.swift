import DatabaseTypes
/// Cursor contract for a transaction range read.
///
/// `finish()` is mandatory because a consumer can stop before `next()` returns
/// `nil`. Persistent backends use it to close cursors and await in-flight I/O
/// before the owning transaction commits or cancels.
public protocol TransactionRangeCursor: Sendable {
    mutating func next() async throws -> (ByteString, ByteString)?

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws
}

extension TransactionRangeCursor {
    public mutating func finish() async throws {
        try await finish(isolation: #isolation)
    }
}
