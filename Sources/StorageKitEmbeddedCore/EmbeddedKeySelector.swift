/// Embedded representation of StorageKit key selectors.
public struct EmbeddedKeySelector: Sendable, Hashable {
    public enum Kind: UInt8, Sendable, Hashable {
        case firstGreaterOrEqual = 1
        case firstGreaterThan = 2
        case lastLessOrEqual = 3
        case lastLessThan = 4
    }

    public let key: [UInt8]
    public let kind: Kind

    public init(key: [UInt8], kind: Kind) {
        self.key = key
        self.kind = kind
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(EmbeddedWireError) {
        writer.writeUInt8(kind.rawValue)
        try writer.writeBytes(key)
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        let rawKind = try reader.readUInt8()
        guard let kind = Kind(rawValue: rawKind) else {
            throw EmbeddedWireError.unknownKeySelector(rawKind)
        }
        self.kind = kind
        self.key = try reader.readBytes()
    }

    public func resolve(in sortedKeys: [[UInt8]]) -> Int {
        switch kind {
        case .firstGreaterOrEqual:
            return lowerBound(key, in: sortedKeys)
        case .firstGreaterThan:
            return upperBound(key, in: sortedKeys)
        case .lastLessOrEqual:
            return upperBound(key, in: sortedKeys) - 1
        case .lastLessThan:
            return lowerBound(key, in: sortedKeys) - 1
        }
    }

    private func lowerBound(_ key: [UInt8], in sortedKeys: [[UInt8]]) -> Int {
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

    private func upperBound(_ key: [UInt8], in sortedKeys: [[UInt8]]) -> Int {
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
