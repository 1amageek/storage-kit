import DatabaseTypes
/// Exact range metrics computed from an ordered transaction view.
public enum StorageRangeMetrics {
    public static let defaultMaximumSplitPointCount = 10_000

    public static func exactSize<Rows: TransactionRangeResult>(
        _ rows: Rows
    ) async throws -> Int {
        var total = 0
        try await rows.consumeRows { key, value in
            let (rowSize, rowOverflow) = key.count.addingReportingOverflow(
                value.count
            )
            let (nextTotal, totalOverflow) = total.addingReportingOverflow(
                rowSize
            )
            guard !rowOverflow, !totalOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range size exceeds the platform integer limit"
                )
            }
            total = nextTotal
        }
        return total
    }

    public static func exactSize(
        _ source: consuming KeyValueCursor
    ) async throws -> Int {
        var rows = source
        var total = 0
        try await rows.consume { key, value in
            let (rowSize, rowOverflow) = key.count.addingReportingOverflow(
                value.count
            )
            let (nextTotal, totalOverflow) = total.addingReportingOverflow(
                rowSize
            )
            guard !rowOverflow, !totalOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range size exceeds the platform integer limit"
                )
            }
            total = nextTotal
        }
        return total
    }

    public static func splitPoints<Rows: TransactionRangeResult>(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int,
        maximumPointCount: Int,
        rows: Rows
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
        let minimumPointCount = beginKey == endKey ? 1 : 2
        guard maximumPointCount >= minimumPointCount else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .rangeRead,
                message: "Split point limit is smaller than the required boundaries"
            )
        }

        // The result outlives each source page. Detach only the selected keys at
        // that lifetime boundary so a tiny split point cannot pin a full page.
        var splitPoints: [ByteString] = [beginKey.detached()]
        var chunkBytes = 0
        try await rows.consumeRows { key, value in
            let (rowSize, rowOverflow) = key.count.addingReportingOverflow(
                value.count
            )
            guard !rowOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range row size exceeds the platform integer limit"
                )
            }
            if chunkBytes > 0,
               rowSize > chunkSize - min(chunkBytes, chunkSize) {
                let reservedEndPointCount = beginKey == endKey ? 0 : 1
                guard splitPoints.count
                        < maximumPointCount - reservedEndPointCount else {
                    throw StorageError(
                        code: .resourceUnavailable,
                        operation: .rangeRead,
                        message: "Split point result exceeds the configured limit"
                    )
                }
                splitPoints.append(key.detached())
                chunkBytes = 0
            }
            let (nextChunkBytes, chunkOverflow) = chunkBytes
                .addingReportingOverflow(rowSize)
            guard !chunkOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range chunk size exceeds the platform integer limit"
                )
            }
            chunkBytes = nextChunkBytes
        }
        if beginKey != endKey {
            guard splitPoints.count < maximumPointCount else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Split point result exceeds the configured limit"
                )
            }
            splitPoints.append(endKey.detached())
        }
        return splitPoints
    }

    public static func splitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int,
        maximumPointCount: Int,
        source: consuming KeyValueCursor
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
        let minimumPointCount = beginKey == endKey ? 1 : 2
        guard maximumPointCount >= minimumPointCount else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .rangeRead,
                message: "Split point limit is smaller than the required boundaries"
            )
        }

        var splitPoints: [ByteString] = [beginKey.detached()]
        var chunkBytes = 0
        var rows = source
        try await rows.consume { key, value in
            let (rowSize, rowOverflow) = key.count.addingReportingOverflow(
                value.count
            )
            guard !rowOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range row size exceeds the platform integer limit"
                )
            }
            if chunkBytes > 0,
               rowSize > chunkSize - min(chunkBytes, chunkSize) {
                let reservedEndPointCount = beginKey == endKey ? 0 : 1
                guard splitPoints.count
                        < maximumPointCount - reservedEndPointCount else {
                    throw StorageError(
                        code: .resourceUnavailable,
                        operation: .rangeRead,
                        message: "Split point result exceeds the configured limit"
                    )
                }
                splitPoints.append(key.detached())
                chunkBytes = 0
            }
            let (nextChunkBytes, chunkOverflow) = chunkBytes
                .addingReportingOverflow(rowSize)
            guard !chunkOverflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Range chunk size exceeds the platform integer limit"
                )
            }
            chunkBytes = nextChunkBytes
        }
        if beginKey != endKey {
            guard splitPoints.count < maximumPointCount else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    message: "Split point result exceeds the configured limit"
                )
            }
            splitPoints.append(endKey.detached())
        }
        return splitPoints
    }
}
