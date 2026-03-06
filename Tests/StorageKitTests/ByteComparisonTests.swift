import Testing
@testable import StorageKit

/// Regression tests for the shared `compareBytes` implementation.
///
/// `compareBytes` was previously duplicated across InMemory and SQLite modules.
/// These tests verify the single shared implementation in ByteComparison.swift
/// handles all edge cases correctly.
@Suite("ByteComparison Tests")
struct ByteComparisonTests {

    // MARK: - Equality

    @Test func equalArrays() {
        #expect(compareBytes([1, 2, 3], [1, 2, 3]) == 0)
    }

    @Test func emptyArraysAreEqual() {
        let a: Bytes = []
        let b: Bytes = []
        #expect(compareBytes(a, b) == 0)
    }

    @Test func singleByteEqual() {
        #expect(compareBytes([0x42], [0x42]) == 0)
    }

    // MARK: - Ordering

    @Test func lessThan_firstByteDiffers() {
        #expect(compareBytes([1, 2, 3], [2, 2, 3]) < 0)
    }

    @Test func greaterThan_firstByteDiffers() {
        #expect(compareBytes([2, 2, 3], [1, 2, 3]) > 0)
    }

    @Test func lessThan_laterByteDiffers() {
        #expect(compareBytes([1, 2, 3], [1, 2, 4]) < 0)
    }

    @Test func greaterThan_laterByteDiffers() {
        #expect(compareBytes([1, 2, 4], [1, 2, 3]) > 0)
    }

    // MARK: - Prefix / Length Comparison

    @Test func shorterPrefix_isLessThan() {
        // [1, 2] < [1, 2, 3] — shorter prefix comes first
        #expect(compareBytes([1, 2], [1, 2, 3]) < 0)
    }

    @Test func longerPrefix_isGreaterThan() {
        #expect(compareBytes([1, 2, 3], [1, 2]) > 0)
    }

    @Test func emptyVsNonEmpty() {
        #expect(compareBytes([], [1]) < 0)
        #expect(compareBytes([1], []) > 0)
    }

    // MARK: - Byte Value Boundaries

    @Test func zeroBytes() {
        #expect(compareBytes([0x00], [0x00]) == 0)
        #expect(compareBytes([0x00], [0x01]) < 0)
    }

    @Test func maxBytes() {
        #expect(compareBytes([0xFF], [0xFF]) == 0)
        #expect(compareBytes([0xFE], [0xFF]) < 0)
        #expect(compareBytes([0xFF], [0xFE]) > 0)
    }

    @Test func zeroVsMax() {
        #expect(compareBytes([0x00], [0xFF]) < 0)
        #expect(compareBytes([0xFF], [0x00]) > 0)
    }

    // MARK: - Cross-Module Consistency

    /// Verify that InMemory engine (which uses the shared compareBytes)
    /// maintains correct lexicographic ordering in range scans.
    @Test func inMemoryEngine_usesSharedCompareBytes() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { tx in
            // Insert keys that exercise prefix comparison
            tx.setValue([1], for: [0x01, 0xFF])
            tx.setValue([2], for: [0x01])
            tx.setValue([3], for: [0x01, 0x00])
            tx.setValue([4], for: [0x02])
            tx.setValue([5], for: [0x00])
        }

        try await engine.withTransaction { tx in
            let results = try await tx.collectRange(
                begin: [0x00], end: [0xFF]
            )
            let keys = results.map { $0.0 }
            // Expected order: [0x00] < [0x01] < [0x01, 0x00] < [0x01, 0xFF] < [0x02]
            #expect(keys.count == 5)
            #expect(keys[0] == [0x00])
            #expect(keys[1] == [0x01])
            #expect(keys[2] == [0x01, 0x00])
            #expect(keys[3] == [0x01, 0xFF])
            #expect(keys[4] == [0x02])
        }
    }
}
