import StorageKit
import Testing
@testable import SQLiteStorage

@Suite("SQLite connection byte ownership")
struct SQLiteConnectionOwnershipTests {
    @Test("Empty key and value round-trip as blobs instead of NULL")
    func emptyKeyAndValueRoundTrip() throws {
        let connection = try SQLiteConnection(path: ":memory:")
        defer { connection.close() }
        try connection.initialize()

        try connection.insertOrReplace(key: [], value: [])
        let stored = try connection.get(key: [])

        #expect(stored != nil)
        #expect(stored == [])
    }

    @Test("Static blob bindings remain borrowed through SQLite step")
    func staticBlobBindingsRemainBorrowedThroughStep() throws {
        let connection = try SQLiteConnection(path: ":memory:")
        defer { connection.close() }
        try connection.initialize()

        let expectedKey: Bytes = [0x10, 0x20, 0x30]
        let expectedValue: Bytes = [0x40, 0x50, 0x60]
        let keyOwner = SQLiteBorrowInvalidatingBytesOwner(
            expectedKey.copyBytes()
        )
        let valueOwner = SQLiteBorrowInvalidatingBytesOwner(
            expectedValue.copyBytes()
        )

        try connection.insertOrReplace(
            key: Bytes(retaining: keyOwner),
            value: Bytes(retaining: valueOwner)
        )

        #expect(keyOwner.borrowCount == 1)
        #expect(valueOwner.borrowCount == 1)
        #expect(try connection.get(key: expectedKey) == expectedValue)
    }
}
