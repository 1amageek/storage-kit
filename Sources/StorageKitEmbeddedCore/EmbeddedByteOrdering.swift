/// Lexicographic byte ordering used by StorageKit keys.
public enum EmbeddedByteOrdering {
    public static func compare(_ lhs: [UInt8], _ rhs: [UInt8]) -> Int {
        let count = lhs.count < rhs.count ? lhs.count : rhs.count
        var index = 0
        while index < count {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
            index += 1
        }
        if lhs.count == rhs.count {
            return 0
        }
        return lhs.count < rhs.count ? -1 : 1
    }

    public static func lessThan(_ lhs: [UInt8], _ rhs: [UInt8]) -> Bool {
        compare(lhs, rhs) < 0
    }
}
