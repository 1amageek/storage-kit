import Testing
@testable import StorageKit

/// Tests for SortedKeyValueStore — the core sorted-array abstraction.
///
/// Verifies that binary search, insertion, deletion, and range deletion
/// maintain sort invariants and have correct boundary semantics.
@Suite("SortedKeyValueStore Tests")
struct SortedKeyValueStoreTests {

    // MARK: - Get

    @Test func get_existingKey() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        #expect(store.get([0x01]) == [10])
    }

    @Test func get_missingKey() {
        let store = SortedKeyValueStore()
        #expect(store.get([0x01]) == nil)
    }

    // MARK: - Set

    @Test func set_insertsInSortedOrder() {
        var store = SortedKeyValueStore()
        store.set([0x03], [30])
        store.set([0x01], [10])
        store.set([0x02], [20])

        let keys = store.entries.map(\.key)
        #expect(keys == [[0x01], [0x02], [0x03]])
    }

    @Test func set_updatesExistingKey() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        store.set([0x01], [99])
        #expect(store.count == 1)
        #expect(store.get([0x01]) == [99])
    }

    // MARK: - Delete

    @Test func delete_existingKey() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        store.set([0x02], [20])
        store.delete([0x01])
        #expect(store.count == 1)
        #expect(store.get([0x01]) == nil)
        #expect(store.get([0x02]) == [20])
    }

    @Test func delete_missingKey_noOp() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        store.delete([0x99])
        #expect(store.count == 1)
    }

    // MARK: - DeleteRange

    @Test func deleteRange_beginInclusive_endExclusive() {
        var store = SortedKeyValueStore()
        for i: UInt8 in 1...5 {
            store.set([i], [i * 10])
        }

        // Delete [0x02, 0x05) → removes 0x02, 0x03, 0x04
        store.deleteRange(begin: [0x02], end: [0x05])

        #expect(store.count == 2)
        #expect(store.get([0x01]) == [10])  // before range
        #expect(store.get([0x02]) == nil)   // begin inclusive
        #expect(store.get([0x03]) == nil)   // inside
        #expect(store.get([0x04]) == nil)   // inside
        #expect(store.get([0x05]) == [50])  // end exclusive
    }

    @Test func deleteRange_emptyRange_noOp() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        store.set([0x05], [50])

        // No keys in [0x02, 0x04)
        store.deleteRange(begin: [0x02], end: [0x04])
        #expect(store.count == 2)
    }

    @Test func deleteRange_entireStore() {
        var store = SortedKeyValueStore()
        for i: UInt8 in 1...5 {
            store.set([i], [i * 10])
        }

        store.deleteRange(begin: [0x00], end: [0xFF])
        #expect(store.isEmpty)
    }

    @Test func deleteRange_multiByteKeys() {
        var store = SortedKeyValueStore()
        store.set([0x01, 0xFF], [1])
        store.set([0x02, 0x00], [2])
        store.set([0x02, 0x50], [3])
        store.set([0x03, 0x00], [4])

        // Delete [0x02, 0x00] ..< [0x03, 0x00]
        store.deleteRange(begin: [0x02, 0x00], end: [0x03, 0x00])

        #expect(store.count == 2)
        #expect(store.get([0x01, 0xFF]) == [1])
        #expect(store.get([0x02, 0x00]) == nil)
        #expect(store.get([0x02, 0x50]) == nil)
        #expect(store.get([0x03, 0x00]) == [4])
    }

    @Test func deleteRange_beginEqualsEnd_noOp() {
        var store = SortedKeyValueStore()
        store.set([0x01], [10])
        store.deleteRange(begin: [0x01], end: [0x01])
        #expect(store.count == 1)
    }

    // MARK: - Slice

    @Test func slice_returnsSubrange() {
        var store = SortedKeyValueStore()
        for i: UInt8 in 1...5 {
            store.set([i], [i * 10])
        }

        let sliced = store.slice(1..<4)
        let keys = sliced.map(\.key)
        #expect(keys == [[0x02], [0x03], [0x04]])
    }

    // MARK: - Sort Invariant Under Mixed Operations

    @Test func mixedOperations_maintainSortOrder() {
        var store = SortedKeyValueStore()

        // Insert out of order
        store.set([0x05], [50])
        store.set([0x01], [10])
        store.set([0x03], [30])

        // Delete middle
        store.delete([0x03])

        // Insert new keys
        store.set([0x02], [20])
        store.set([0x04], [40])

        // Update existing
        store.set([0x01], [99])

        let keys = store.entries.map(\.key)
        #expect(keys == [[0x01], [0x02], [0x04], [0x05]])
        #expect(store.get([0x01]) == [99])
    }

    // MARK: - Init from Existing Entries

    @Test func initFromEntries_preservesOrder() {
        let entries: [(key: Bytes, value: Bytes)] = [
            (key: [0x01], value: [10]),
            (key: [0x02], value: [20]),
            (key: [0x03], value: [30]),
        ]
        let store = SortedKeyValueStore(entries)
        #expect(store.count == 3)
        #expect(store.get([0x02]) == [20])
    }

    // MARK: - Keys

    @Test func keys_returnsAllKeysInOrder() {
        var store = SortedKeyValueStore()
        store.set([0x03], [30])
        store.set([0x01], [10])
        store.set([0x02], [20])

        #expect(store.keys == [[0x01], [0x02], [0x03]])
    }
}
