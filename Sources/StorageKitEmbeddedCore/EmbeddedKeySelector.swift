/// Embedded representation of StorageKit key selectors.
public struct EmbeddedKeySelector: Sendable, Hashable {
    public enum Kind: UInt8, Sendable, Hashable {
        case firstGreaterOrEqual = 1
        case firstGreaterThan = 2
        case lastLessOrEqual = 3
        case lastLessThan = 4
    }

    public let key: EmbeddedBytes
    public let orEqual: Bool
    public let offset: Int

    public init(key: EmbeddedBytes, kind: Kind) {
        self.key = key
        switch kind {
        case .firstGreaterOrEqual:
            self.orEqual = false
            self.offset = 1
        case .firstGreaterThan:
            self.orEqual = true
            self.offset = 1
        case .lastLessOrEqual:
            self.orEqual = true
            self.offset = 0
        case .lastLessThan:
            self.orEqual = false
            self.offset = 0
        }
    }

    public init(key: EmbeddedBytes, orEqual: Bool, offset: Int) {
        self.key = key
        self.orEqual = orEqual
        self.offset = offset
    }

    public func encode(into writer: inout EmbeddedWireWriter) throws(EmbeddedWireError) {
        try writer.writeBytes(key)
        writer.writeBool(orEqual)
        guard let encodedOffset = Int64(exactly: offset) else {
            throw EmbeddedWireError.keySelectorOffsetOverflow
        }
        writer.writeInt64(encodedOffset)
    }

    public init(from reader: inout EmbeddedWireReader) throws(EmbeddedWireError) {
        self.key = try reader.readByteRegion()
        self.orEqual = try reader.readBool()
        let encodedOffset = try reader.readInt64()
        guard let offset = Int(exactly: encodedOffset) else {
            throw EmbeddedWireError.keySelectorOffsetOverflow
        }
        self.offset = offset
    }

    public func resolve(in sortedKeys: [EmbeddedBytes]) -> Int {
        let base = orEqual
            ? upperBound(key, in: sortedKeys) - 1
            : lowerBound(key, in: sortedKeys) - 1
        let (resolved, overflow) = base.addingReportingOverflow(offset)
        if overflow { return offset >= 0 ? sortedKeys.count : 0 }
        return max(0, min(resolved, sortedKeys.count))
    }

    private func lowerBound(
        _ key: EmbeddedBytes,
        in sortedKeys: [EmbeddedBytes]
    ) -> Int {
        var low = 0
        var high = sortedKeys.count
        while low < high {
            let mid = (low + high) / 2
            if EmbeddedByteOrdering.compare(sortedKeys[mid], key) < 0 {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }

    private func upperBound(
        _ key: EmbeddedBytes,
        in sortedKeys: [EmbeddedBytes]
    ) -> Int {
        var low = 0
        var high = sortedKeys.count
        while low < high {
            let mid = (low + high) / 2
            if EmbeddedByteOrdering.compare(sortedKeys[mid], key) <= 0 {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }
}
