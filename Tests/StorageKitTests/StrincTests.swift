import Testing
@testable import StorageKit

@Suite("strinc Tests")
struct StrincTests {

    @Test func normalBytes_incrementsLastByte() throws {
        let result = try strinc([0x01, 0x02])
        #expect(result == [0x01, 0x03])
    }

    @Test func trailingFF_stripsAndIncrementsNext() throws {
        let result = try strinc([0x01, 0xFF])
        #expect(result == [0x02])
    }

    @Test func multipleTrailingFF_stripsAll() throws {
        let result = try strinc([0x01, 0xFF, 0xFF])
        #expect(result == [0x02])
    }

    @Test func singleByte_increments() throws {
        let result = try strinc([0x00])
        #expect(result == [0x01])
    }

    @Test func allFF_throwsCannotIncrementKey() throws {
        do {
            _ = try strinc([0xFF, 0xFF, 0xFF])
            Issue.record("Expected cannotIncrementKey error")
        } catch let error as TupleError {
            guard case .cannotIncrementKey = error else {
                Issue.record("Expected cannotIncrementKey, got \(error)")
                return
            }
        }
    }

    @Test func singleFF_throwsCannotIncrementKey() throws {
        do {
            _ = try strinc([0xFF])
            Issue.record("Expected cannotIncrementKey error")
        } catch let error as TupleError {
            guard case .cannotIncrementKey = error else {
                Issue.record("Expected cannotIncrementKey, got \(error)")
                return
            }
        }
    }

    @Test func emptyBytes_throwsCannotIncrementKey() throws {
        do {
            _ = try strinc([])
            Issue.record("Expected cannotIncrementKey error")
        } catch let error as TupleError {
            guard case .cannotIncrementKey = error else {
                Issue.record("Expected cannotIncrementKey, got \(error)")
                return
            }
        }
    }

    @Test func maxNonFF_wrapsCorrectly() throws {
        let result = try strinc([0xFE])
        #expect(result == [0xFF])
    }

    @Test func mixedBytes_incrementsLastNonFF() throws {
        let result = try strinc([0x01, 0x02, 0xFF, 0xFF])
        #expect(result == [0x01, 0x03])
    }
}
