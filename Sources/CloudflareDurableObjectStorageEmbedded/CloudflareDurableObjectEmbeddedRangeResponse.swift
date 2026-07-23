import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedRangeResponse: Sendable, Hashable {
    public let rows: [EmbeddedKeyValue]
    public let hasMore: Bool
    public let currentCommitVersion: Int64
    public let readConflictRanges: [EmbeddedKeyRange]

    public init(
        rows: [EmbeddedKeyValue],
        hasMore: Bool,
        currentCommitVersion: Int64,
        readConflictRanges: [EmbeddedKeyRange] = []
    ) {
        self.rows = rows
        self.hasMore = hasMore
        self.currentCommitVersion = currentCommitVersion
        self.readConflictRanges = readConflictRanges
    }

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        let limits = EmbeddedLimits.cloudflareDurableObject
        try CloudflareDurableObjectEmbeddedError.writeCount(
            rows.count,
            maximum: limits.maxRangeLimit,
            into: &writer
        )
        for row in rows {
            try CloudflareDurableObjectEmbeddedError.encode(row, into: &writer)
        }
        guard !hasMore || !rows.isEmpty else {
            throw .wire(.invalidRangeContinuation)
        }
        writer.writeBool(hasMore)
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(currentCommitVersion)
        try CloudflareDurableObjectEmbeddedError.writeCount(
            readConflictRanges.count,
            maximum: limits.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in readConflictRanges {
            try CloudflareDurableObjectEmbeddedError.encode(range, into: &writer)
        }
    }

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        let count = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxRangeLimit
        )
        var rows: [EmbeddedKeyValue] = []
        rows.reserveCapacity(count)
        for _ in 0..<count {
            rows.append(try CloudflareDurableObjectEmbeddedError.readKeyValue(from: &reader))
        }
        self.rows = rows
        let hasMore = try CloudflareDurableObjectEmbeddedError.readBool(
            from: &reader
        )
        guard !hasMore || !rows.isEmpty else {
            throw .wire(.invalidRangeContinuation)
        }
        self.hasMore = hasMore
        let version = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.currentCommitVersion = version
        let rangeCount = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxConflictRangesPerCommit
        )
        var ranges: [EmbeddedKeyRange] = []
        ranges.reserveCapacity(rangeCount)
        for _ in 0..<rangeCount {
            ranges.append(try CloudflareDurableObjectEmbeddedError.readKeyRange(from: &reader))
        }
        self.readConflictRanges = ranges
    }
}
