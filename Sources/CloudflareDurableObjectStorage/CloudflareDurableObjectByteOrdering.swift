import StorageKit

enum CloudflareDurableObjectByteOrdering {
    static func compare(_ lhs: Bytes, _ rhs: Bytes) -> Int {
        let minCount = min(lhs.count, rhs.count)
        var index = 0
        while index < minCount {
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

    static func sortedUnique(_ keys: [Bytes]) -> [Bytes] {
        let sorted = keys.sorted { compare($0, $1) < 0 }
        var result: [Bytes] = []
        result.reserveCapacity(sorted.count)
        for key in sorted where result.last != key {
            result.append(key)
        }
        return result
    }
}
