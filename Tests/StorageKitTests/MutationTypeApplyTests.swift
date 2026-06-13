import Testing
@testable import StorageKit

/// Direct unit tests for `MutationType.apply(to:param:)`.
///
/// This suite is the single source of truth for atomic-mutation semantics.
/// Backend engines (InMemory, SQLite, PostgreSQL) all delegate to this function,
/// so verifying it here once covers their shared arithmetic; the per-engine
/// suites only need to confirm the wiring (read-your-writes, commit staging).
///
/// Semantics follow FoundationDB's atomic operations exactly:
/// https://apple.github.io/foundationdb/api-c.html#c.FDBMutationType
@Suite("MutationType.apply Semantics")
struct MutationTypeApplyTests {

    // MARK: - add (little-endian integer addition)

    @Test func add_singleByte() throws {
        #expect(try MutationType.add.apply(to: [10], param: [5]) == .set([15]))
    }

    @Test func add_missingValueTreatedAsZero() throws {
        #expect(try MutationType.add.apply(to: nil, param: [5]) == .set([5]))
    }

    @Test func add_multiByteCarryPropagates() throws {
        // [0xFF, 0x00] = 255, + [0x01, 0x00] = 1 -> [0x00, 0x01] = 256 (LE).
        #expect(try MutationType.add.apply(to: [0xFF, 0x00], param: [0x01, 0x00]) == .set([0x00, 0x01]))
    }

    @Test func add_zeroExtendsShorterExisting() throws {
        // [0xFF] is zero-extended to [0xFF, 0x00] before adding [0x01, 0x00].
        #expect(try MutationType.add.apply(to: [0xFF], param: [0x01, 0x00]) == .set([0x00, 0x01]))
    }

    @Test func add_truncatesLongerExisting() throws {
        // [0x05, 0x06] is truncated to [0x05] before adding [0x01].
        #expect(try MutationType.add.apply(to: [0x05, 0x06], param: [0x01]) == .set([0x06]))
    }

    @Test func add_overflowWrapsWithinParamWidth() throws {
        // 255 + 1 overflows a single byte and wraps to 0; the carry is dropped.
        #expect(try MutationType.add.apply(to: [0xFF], param: [0x01]) == .set([0x00]))
    }

    // MARK: - bitAnd

    @Test func bitAnd_combinesBits() throws {
        #expect(try MutationType.bitAnd.apply(to: [0b1100], param: [0b1010]) == .set([0b1000]))
    }

    @Test func bitAnd_missingValueStoresParam() throws {
        // Unlike bitOr/bitXor, a missing value is set to param directly,
        // matching FDB (AND against an absent key yields the operand).
        #expect(try MutationType.bitAnd.apply(to: nil, param: [0xAB]) == .set([0xAB]))
    }

    // MARK: - bitOr

    @Test func bitOr_combinesBits() throws {
        #expect(try MutationType.bitOr.apply(to: [0b1100], param: [0b1010]) == .set([0b1110]))
    }

    @Test func bitOr_missingValueTreatedAsZero() throws {
        #expect(try MutationType.bitOr.apply(to: nil, param: [0xAB]) == .set([0xAB]))
    }

    // MARK: - bitXor

    @Test func bitXor_combinesBits() throws {
        #expect(try MutationType.bitXor.apply(to: [0b1100], param: [0b1010]) == .set([0b0110]))
    }

    @Test func bitXor_missingValueTreatedAsZero() throws {
        #expect(try MutationType.bitXor.apply(to: nil, param: [0xAB]) == .set([0xAB]))
    }

    // MARK: - max (little-endian unsigned; larger wins)

    @Test func max_existingLargerWins() throws {
        #expect(try MutationType.max.apply(to: [10], param: [5]) == .set([10]))
    }

    @Test func max_paramLargerWins() throws {
        #expect(try MutationType.max.apply(to: [5], param: [10]) == .set([10]))
    }

    @Test func max_equalKeepsExisting() throws {
        #expect(try MutationType.max.apply(to: [7], param: [7]) == .set([7]))
    }

    @Test func max_missingValueTreatedAsZero() throws {
        #expect(try MutationType.max.apply(to: nil, param: [5]) == .set([5]))
    }

    @Test func max_comparesAsLittleEndianIntegerNotBytewise() throws {
        // [0x00, 0x02] = 512, [0xFF, 0x01] = 511 (LE). Integer max is 512.
        // A byte-wise comparison would instead pick [0xFF, 0x01] on the first
        // byte, so this asserts the little-endian integer semantics.
        #expect(try MutationType.max.apply(to: [0x00, 0x02], param: [0xFF, 0x01]) == .set([0x00, 0x02]))
    }

    // MARK: - min (little-endian unsigned; smaller wins)

    @Test func min_existingSmallerWins() throws {
        #expect(try MutationType.min.apply(to: [5], param: [10]) == .set([5]))
    }

    @Test func min_paramSmallerWins() throws {
        #expect(try MutationType.min.apply(to: [10], param: [5]) == .set([5]))
    }

    @Test func min_missingValueStoresParam() throws {
        // FDB MIN against an absent key stores the operand directly.
        #expect(try MutationType.min.apply(to: nil, param: [5]) == .set([5]))
    }

    @Test func min_comparesAsLittleEndianIntegerNotBytewise() throws {
        // [0x00, 0x02] = 512, [0xFF, 0x01] = 511 (LE). Integer min is 511.
        #expect(try MutationType.min.apply(to: [0x00, 0x02], param: [0xFF, 0x01]) == .set([0xFF, 0x01]))
    }

    // MARK: - compareAndClear

    @Test func compareAndClear_equalClears() throws {
        #expect(try MutationType.compareAndClear.apply(to: [42], param: [42]) == .clear)
    }

    @Test func compareAndClear_unequalLeavesUnchanged() throws {
        #expect(try MutationType.compareAndClear.apply(to: [42], param: [99]) == .unchanged)
    }

    @Test func compareAndClear_missingLeavesUnchanged() throws {
        #expect(try MutationType.compareAndClear.apply(to: nil, param: [42]) == .unchanged)
    }

    // MARK: - versionstamp mutations (unsupported outside FDB)

    @Test func setVersionstampedKey_throwsInvalidOperation() throws {
        do {
            _ = try MutationType.setVersionstampedKey.apply(to: [0x01], param: [0x01])
            Issue.record("Expected setVersionstampedKey to throw")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
    }

    @Test func setVersionstampedValue_throwsInvalidOperation() throws {
        do {
            _ = try MutationType.setVersionstampedValue.apply(to: [0x01], param: [0x01])
            Issue.record("Expected setVersionstampedValue to throw")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }
    }
}
