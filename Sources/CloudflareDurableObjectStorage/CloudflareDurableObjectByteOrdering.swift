import DatabaseTypes
import StorageKit
import CloudflareDurableObjectStorageWire

enum CloudflareDurableObjectByteOrdering {
    static func compare(_ lhs: ByteString, _ rhs: ByteString) -> Int {
        StorageWireByteOrdering.compare(lhs, rhs)
    }

    static func sortedUnique(_ keys: [ByteString]) -> [ByteString] {
        let sorted = keys.sorted { compare($0, $1) < 0 }
        var result: [ByteString] = []
        result.reserveCapacity(sorted.count)
        for key in sorted where result.last != key {
            result.append(key)
        }
        return result
    }
}
