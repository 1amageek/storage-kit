import StorageKitEmbeddedCore

/// Ordered chunk boundaries, including the requested begin and end keys.
public struct CloudflareDurableObjectEmbeddedRangeSplitPointsResponse: Sendable, Hashable {
    public let splitPoints: [EmbeddedBytes]
    public let currentCommitVersion: Int64

    public init(
        splitPoints: [EmbeddedBytes],
        currentCommitVersion: Int64
    ) {
        self.splitPoints = splitPoints
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        try Self.validate(splitPoints)
        try CloudflareDurableObjectEmbeddedError.writeCount(
            splitPoints.count,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxSplitPoints,
            into: &writer
        )
        for point in splitPoints {
            try CloudflareDurableObjectEmbeddedError.writeBytes(
                point,
                maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes,
                into: &writer
            )
        }
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(currentCommitVersion)
    }

    init(
        from reader: inout EmbeddedWireReader
    ) throws(CloudflareDurableObjectEmbeddedError) {
        let count = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxSplitPoints
        )
        var points: [EmbeddedBytes] = []
        points.reserveCapacity(count)
        for _ in 0..<count {
            points.append(
                try CloudflareDurableObjectEmbeddedError.readBytes(
                    from: &reader,
                    maximum: EmbeddedLimits.cloudflareDurableObject.maxBoundaryBytes
                )
            )
        }
        try Self.validate(points)
        let version = try CloudflareDurableObjectEmbeddedError.readInt64(
            from: &reader
        )
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.splitPoints = points
        self.currentCommitVersion = version
    }

    static func validate(
        _ points: [EmbeddedBytes]
    ) throws(CloudflareDurableObjectEmbeddedError) {
        guard !points.isEmpty else {
            throw .wire(.invalidSplitPoints)
        }
        for index in 1..<points.count {
            guard EmbeddedByteOrdering.compare(
                points[index - 1],
                points[index]
            ) < 0 else {
                throw .wire(.invalidSplitPoints)
            }
        }
    }
}
