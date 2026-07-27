import DatabaseTypes

/// Ordered chunk boundaries, including the requested begin and end keys.
public struct StorageWireRangeSplitPointsResponse: Sendable, Hashable {
    public let splitPoints: [ByteString]
    public let currentCommitVersion: Int64

    public init(
        splitPoints: [ByteString],
        currentCommitVersion: Int64
    ) {
        self.splitPoints = splitPoints
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try Self.validate(splitPoints)
        try StorageWireProtocolError.writeCount(
            splitPoints.count,
            maximum: StorageWireLimits.cloudflareDurableObject.maxSplitPoints,
            into: &writer
        )
        for point in splitPoints {
            try StorageWireProtocolError.writeBytes(
                point,
                maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes,
                into: &writer
            )
        }
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(currentCommitVersion)
    }

    init(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) {
        let count = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxSplitPoints
        )
        var points: [ByteString] = []
        points.reserveCapacity(count)
        for _ in 0..<count {
            points.append(
                try StorageWireProtocolError.readBytes(
                    from: &reader,
                    maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
                )
            )
        }
        try Self.validate(points)
        let version = try StorageWireProtocolError.readInt64(
            from: &reader
        )
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.splitPoints = points
        self.currentCommitVersion = version
    }

    static func validate(
        _ points: [ByteString]
    ) throws(StorageWireProtocolError) {
        guard !points.isEmpty else {
            throw .wire(.invalidSplitPoints)
        }
        for index in 1..<points.count {
            guard StorageWireByteOrdering.compare(
                points[index - 1],
                points[index]
            ) < 0 else {
                throw .wire(.invalidSplitPoints)
            }
        }
    }
}
