
public struct StorageWireRangeResponse: Sendable, Hashable {
    public let rows: [StorageWireKeyValue]
    public let hasMore: Bool
    public let currentCommitVersion: Int64
    public let readConflictRanges: [StorageWireKeyRange]

    public init(
        rows: [StorageWireKeyValue],
        hasMore: Bool,
        currentCommitVersion: Int64,
        readConflictRanges: [StorageWireKeyRange] = []
    ) {
        self.rows = rows
        self.hasMore = hasMore
        self.currentCommitVersion = currentCommitVersion
        self.readConflictRanges = readConflictRanges
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        let limits = StorageWireLimits.cloudflareDurableObject
        try StorageWireProtocolError.writeCount(
            rows.count,
            maximum: limits.maxRangeLimit,
            into: &writer
        )
        for row in rows {
            try StorageWireProtocolError.encode(row, into: &writer)
        }
        guard !hasMore || !rows.isEmpty else {
            throw .wire(.invalidRangeContinuation)
        }
        writer.writeBool(hasMore)
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(currentCommitVersion)
        try StorageWireProtocolError.writeCount(
            readConflictRanges.count,
            maximum: limits.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in readConflictRanges {
            try StorageWireProtocolError.encode(range, into: &writer)
        }
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        let count = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxRangeLimit
        )
        var rows: [StorageWireKeyValue] = []
        rows.reserveCapacity(count)
        for _ in 0..<count {
            rows.append(try StorageWireProtocolError.readKeyValue(from: &reader))
        }
        self.rows = rows
        let hasMore = try StorageWireProtocolError.readBool(
            from: &reader
        )
        guard !hasMore || !rows.isEmpty else {
            throw .wire(.invalidRangeContinuation)
        }
        self.hasMore = hasMore
        let version = try StorageWireProtocolError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.currentCommitVersion = version
        let rangeCount = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxConflictRangesPerCommit
        )
        var ranges: [StorageWireKeyRange] = []
        ranges.reserveCapacity(rangeCount)
        for _ in 0..<rangeCount {
            ranges.append(try StorageWireProtocolError.readKeyRange(from: &reader))
        }
        self.readConflictRanges = ranges
    }
}
