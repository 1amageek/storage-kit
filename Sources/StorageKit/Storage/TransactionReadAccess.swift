import DatabaseTypes

/// Read-only access to one storage transaction.
///
/// This capability intentionally excludes mutation, transaction lifecycle,
/// read-version mutation, transaction options, conflict ranges, range metrics,
/// and versionstamp requests. Components that only inspect the admitted
/// transaction view should depend on this protocol.
public protocol TransactionReadAccess: Sendable {
    /// The storage engine instance whose physical state this transaction reads.
    ///
    /// Read-side caches include this identity in their keys so equal logical
    /// keys from independent engines can never share process-local state.
    var transactionDomain: StorageTransactionDomain { get }

    /// Gets the value for a key, or `nil` when the key does not exist.
    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString?

    /// Gets a value while enforcing an exact upper bound before the value is
    /// returned to the caller.
    ///
    /// A maximum of zero permits only an empty stored value. A negative
    /// maximum is invalid. Backend implementations should inspect their
    /// native result length before constructing a `ByteString` whenever that
    /// boundary is available; the default implementation preserves the
    /// contract for custom transactions by validating before returning. A
    /// value-limit violation is caller-owned and does not invalidate the
    /// transaction.
    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString?

    /// Performs a serializable point read.
    func getValue(for key: ByteString) async throws -> ByteString?

    /// Gets the key selected by `selector`.
    func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString?

    /// Opens a type-erased cursor without copying backend-owned key/value bytes.
    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor
}

public extension TransactionReadAccess {
    /// Bounded point-read fallback for custom transaction implementations.
    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        guard maximumByteCount >= 0 else {
            throw StorageError.invalidPointReadMaximum(maximumByteCount)
        }
        guard let value = try await getValue(for: key, snapshot: snapshot)
        else {
            return nil
        }
        guard value.count <= maximumByteCount else {
            throw StorageError.pointReadValueTooLarge(
                observedByteCount: value.count,
                maximumByteCount: maximumByteCount
            )
        }
        return value
    }

    /// Serializable key selection convenience.
    func getKey(selector: KeySelector) async throws -> ByteString? {
        try await getKey(selector: selector, snapshot: false)
    }
}
