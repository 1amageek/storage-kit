import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedRangeResponse: Sendable, Hashable {
    public let rows: [EmbeddedKeyValue]
    public let nextCursor: String?
    public let currentCommitVersion: Int64
    public let conflictRange: EmbeddedKeyRange?

    public init(
        rows: [EmbeddedKeyValue],
        nextCursor: String?,
        currentCommitVersion: Int64,
        conflictRange: EmbeddedKeyRange? = nil
    ) {
        self.rows = rows
        self.nextCursor = nextCursor
        self.currentCommitVersion = currentCommitVersion
        self.conflictRange = conflictRange
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try CloudflareDurableObjectEmbeddedError.writeCount(rows.count, into: &writer)
        for row in rows {
            try CloudflareDurableObjectEmbeddedError.encode(row, into: &writer)
        }
        try CloudflareDurableObjectEmbeddedRangeRequest.writeOptionalString(nextCursor, into: &writer)
        writer.writeInt64(currentCommitVersion)
        if let conflictRange {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.encode(conflictRange, into: &writer)
        } else {
            writer.writeBool(false)
        }
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        let count = try CloudflareDurableObjectEmbeddedError.readCount(from: &reader)
        var rows: [EmbeddedKeyValue] = []
        rows.reserveCapacity(count)
        for _ in 0..<count {
            rows.append(try CloudflareDurableObjectEmbeddedError.readKeyValue(from: &reader))
        }
        self.rows = rows
        self.nextCursor = try CloudflareDurableObjectEmbeddedRangeRequest.readOptionalString(from: &reader)
        self.currentCommitVersion = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
        if try CloudflareDurableObjectEmbeddedError.readBool(from: &reader) {
            self.conflictRange = try CloudflareDurableObjectEmbeddedError.readKeyRange(from: &reader)
        } else {
            self.conflictRange = nil
        }
    }
}
