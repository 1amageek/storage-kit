import Testing
@testable import StorageKit

@Suite("KeySelector Tests")
struct KeySelectorTests {

    @Test func firstGreaterOrEqual() throws {
        let key: Bytes = [0x01, 0x02, 0x03]
        let selector = KeySelector.firstGreaterOrEqual(key)
        #expect(selector.key == key)
        #expect(selector.orEqual == true)
        #expect(selector.offset == 0)
    }

    @Test func firstGreaterThan() throws {
        let key: Bytes = [0x01, 0x02, 0x03]
        let selector = KeySelector.firstGreaterThan(key)
        #expect(selector.key == key)
        #expect(selector.orEqual == true)
        #expect(selector.offset == 1)
    }

    @Test func lastLessOrEqual() throws {
        let key: Bytes = [0x01, 0x02, 0x03]
        let selector = KeySelector.lastLessOrEqual(key)
        #expect(selector.key == key)
        #expect(selector.orEqual == true)
        #expect(selector.offset == 0)
    }

    @Test func lastLessThan() throws {
        let key: Bytes = [0x01, 0x02, 0x03]
        let selector = KeySelector.lastLessThan(key)
        #expect(selector.key == key)
        #expect(selector.orEqual == false)
        #expect(selector.offset == 0)
    }

    @Test func equality() throws {
        let a = KeySelector.firstGreaterOrEqual([0x01])
        let b = KeySelector.firstGreaterOrEqual([0x01])
        let c = KeySelector.firstGreaterThan([0x01])
        #expect(a == b)
        #expect(a != c)
    }
}
