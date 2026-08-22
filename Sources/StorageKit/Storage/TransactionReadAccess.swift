import DatabaseTypes

/// Physical read access to one storage transaction without mutation or
/// transaction-control authority.
public protocol TransactionReadAccess: Sendable {
    var capabilities: TransactionCapabilities { get }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString?

    func getValue(for key: ByteString) async throws -> ByteString?

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString?

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor

    func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int

    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString]
}

extension TransactionReadAccess {
    /// Backends expose no optional read semantics unless they declare them.
    public var capabilities: TransactionCapabilities { .none }

    public func getKey(
        selector: KeySelector
    ) async throws -> ByteString? {
        try await getKey(selector: selector, snapshot: false)
    }

    /// Default: computes the exact stored key and value byte count by scanning
    /// the transaction view.
    public func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        guard compareBytes(beginKey, endKey) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Range size boundaries are not ordered"
            )
        }
        return try await StorageRangeMetrics.exactSize(
            rangeCursor(
                from: .firstGreaterOrEqual(beginKey),
                to: .firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    /// Default: returns ordered chunk boundaries, including `beginKey` and
    /// `endKey`, whose exact stored byte count is approximately `chunkSize`.
    public func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        guard compareBytes(beginKey, endKey) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Split point boundaries are not ordered"
            )
        }
        guard chunkSize > 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Split point chunk size must be positive"
            )
        }

        return try await StorageRangeMetrics.splitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize,
            maximumPointCount: StorageRangeMetrics
                .defaultMaximumSplitPointCount,
            source: rangeCursor(
                from: .firstGreaterOrEqual(beginKey),
                to: .firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }
}
