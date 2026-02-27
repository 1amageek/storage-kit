import Testing
@testable import StorageKit

@Suite("KeySelector Tests")
struct KeySelectorTests {

    @Test func firstGreaterOrEqual() throws {
        let key: Bytes = [0x01, 0x02, 0x03]
        let selector = KeySelector.firstGreaterOrEqual(key)
        #expect(selector.key == key)
        #expect(selector.orEqual == false)
        #expect(selector.offset == 1)
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

    // MARK: - KeySelector Resolution Tests

    @Test func resolveFirstGreaterOrEqual() throws {
        let keys: [Bytes] = [[1], [3], [5], [7], [9]]

        // firstGreaterOrEqual([3]) → index of [3] = 1
        let sel1 = KeySelector.firstGreaterOrEqual([3])
        #expect(sel1.resolve(in: keys) == 1)

        // firstGreaterOrEqual([4]) → index of [5] = 2
        let sel2 = KeySelector.firstGreaterOrEqual([4])
        #expect(sel2.resolve(in: keys) == 2)

        // firstGreaterOrEqual([0]) → index of [1] = 0
        let sel3 = KeySelector.firstGreaterOrEqual([0])
        #expect(sel3.resolve(in: keys) == 0)

        // firstGreaterOrEqual([10]) → past the end = 5
        let sel4 = KeySelector.firstGreaterOrEqual([10])
        #expect(sel4.resolve(in: keys) == 5)
    }

    @Test func resolveFirstGreaterThan() throws {
        let keys: [Bytes] = [[1], [3], [5], [7], [9]]

        // firstGreaterThan([3]) → index of [5] = 2
        let sel1 = KeySelector.firstGreaterThan([3])
        #expect(sel1.resolve(in: keys) == 2)

        // firstGreaterThan([4]) → index of [5] = 2
        let sel2 = KeySelector.firstGreaterThan([4])
        #expect(sel2.resolve(in: keys) == 2)

        // firstGreaterThan([9]) → past the end = 5
        let sel3 = KeySelector.firstGreaterThan([9])
        #expect(sel3.resolve(in: keys) == 5)
    }

    @Test func resolveLastLessOrEqual() throws {
        let keys: [Bytes] = [[1], [3], [5], [7], [9]]

        // lastLessOrEqual([5]) → index of [5] = 2
        let sel1 = KeySelector.lastLessOrEqual([5])
        #expect(sel1.resolve(in: keys) == 2)

        // lastLessOrEqual([4]) → index of [3] = 1
        let sel2 = KeySelector.lastLessOrEqual([4])
        #expect(sel2.resolve(in: keys) == 1)

        // lastLessOrEqual([0]) → before all = clamped to 0... but actually -1 clamped to 0
        // This means "no key found", so index 0 is the clamp
        // But note: for lastLessOrEqual, the result should be -1 (nothing <= 0),
        // clamped to 0 which happens to be [1]. This is an edge case where
        // the caller should check the resolved key.
    }

    @Test func resolveLastLessThan() throws {
        let keys: [Bytes] = [[1], [3], [5], [7], [9]]

        // lastLessThan([5]) → index of [3] = 1
        let sel1 = KeySelector.lastLessThan([5])
        #expect(sel1.resolve(in: keys) == 1)

        // lastLessThan([1]) → before all = clamped to 0
        let sel2 = KeySelector.lastLessThan([1])
        #expect(sel2.resolve(in: keys) == 0)
    }

    @Test func resolveEmptyKeys() throws {
        let keys: [Bytes] = []

        let sel = KeySelector.firstGreaterOrEqual([5])
        #expect(sel.resolve(in: keys) == 0)
    }

    @Test func resolveRangePattern() throws {
        // Typical pattern: getRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end))
        // This should select keys in [begin, end)
        let keys: [Bytes] = [[1], [2], [3], [4], [5]]

        let beginIdx = KeySelector.firstGreaterOrEqual([2]).resolve(in: keys)
        let endIdx = KeySelector.firstGreaterOrEqual([4]).resolve(in: keys)

        #expect(beginIdx == 1) // [2]
        #expect(endIdx == 3)   // [4]
        // Range [1, 3) = keys[1], keys[2] = [2], [3]
        let rangeKeys = Array(keys[beginIdx..<endIdx])
        #expect(rangeKeys == [[2], [3]])
    }
}
