import DatabaseTypes
import StorageKit

/// Adapts the shared journal cursor without retaining its transaction facade.
struct CloudflareDurableObjectJournalRangeScan: CloudflareDurableObjectRangeScanning {
    var cursor: KeyValueCursor

    mutating func next() async throws -> (ByteString, ByteString)? {
        try await cursor.next()
    }

    mutating func finish(isolation actor: isolated (any Actor)?) async throws {
        try await cursor.finish()
    }
}
