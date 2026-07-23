/// Iterator contract for a transaction range read.
///
/// `finish()` is mandatory because a consumer can stop before `next()` returns
/// `nil`. Persistent backends use it to close cursors and await in-flight I/O
/// before the owning transaction commits or cancels.
public protocol TransactionRangeIterator: AsyncIteratorProtocol, Sendable
where Element == (Bytes, Bytes) {
    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws
}

extension TransactionRangeIterator {
    public mutating func finish() async throws {
        try await finish(isolation: #isolation)
    }
}
