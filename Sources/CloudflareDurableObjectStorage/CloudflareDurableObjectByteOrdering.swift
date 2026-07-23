import StorageKit
import StorageKitEmbeddedCore

enum CloudflareDurableObjectByteOrdering {
    static func compare(_ lhs: Bytes, _ rhs: Bytes) -> Int {
        EmbeddedByteOrdering.compare(lhs, rhs)
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
