import DatabaseTypes
/// Cursor contract for a transaction range read.
///
/// `finish()` is mandatory because a consumer can stop before `next()` returns
/// `nil`. Persistent backends use it to close cursors and await in-flight I/O
/// before the owning transaction commits or cancels.
///
/// A return or throw from `finish()` is terminal. Implementations must release
/// their native cursor authority before returning success or failure, and
/// repeated calls must report the same terminal outcome.
///
/// If the final cursor value is released without `finish()`, any native handle
/// still owned by that value must be abandoned synchronously before its
/// transaction-lifetime owner is released. Consumers that require asynchronous
/// cleanup must call and await `finish()`.
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
