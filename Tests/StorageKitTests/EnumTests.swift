import Testing
@testable import StorageKit

@Suite("Enum Type Tests")
struct EnumTests {

    // =========================================================================
    // MARK: - MutationType
    // =========================================================================

    @Test func mutationType_allCases() {
        let cases: [MutationType] = [
            .add, .setVersionstampedKey, .setVersionstampedValue,
            .bitOr, .bitAnd, .bitXor,
            .max, .min, .compareAndClear,
        ]
        #expect(cases.count == 9)
    }

    @Test func mutationType_isSendable() {
        let value: any Sendable = MutationType.add
        #expect(value is MutationType)
    }

    // =========================================================================
    // MARK: - StreamingMode
    // =========================================================================

    @Test func streamingMode_rawValues() {
        #expect(StreamingMode.wantAll.rawValue == -2)
        #expect(StreamingMode.iterator.rawValue == -1)
        #expect(StreamingMode.exact.rawValue == 0)
        #expect(StreamingMode.small.rawValue == 1)
        #expect(StreamingMode.medium.rawValue == 2)
        #expect(StreamingMode.large.rawValue == 3)
        #expect(StreamingMode.serial.rawValue == 4)
    }

    @Test func streamingMode_initFromRawValue() {
        #expect(StreamingMode(rawValue: -2) == .wantAll)
        #expect(StreamingMode(rawValue: -1) == .iterator)
        #expect(StreamingMode(rawValue: 0) == .exact)
        #expect(StreamingMode(rawValue: 1) == .small)
        #expect(StreamingMode(rawValue: 2) == .medium)
        #expect(StreamingMode(rawValue: 3) == .large)
        #expect(StreamingMode(rawValue: 4) == .serial)
        #expect(StreamingMode(rawValue: 99) == nil)
    }

    // =========================================================================
    // MARK: - TransactionOption
    // =========================================================================

    @Test func transactionOption_timeout_carriesValue() {
        let option = TransactionOption.timeout(milliseconds: 5000)
        if case .timeout(let ms) = option {
            #expect(ms == 5000)
        } else {
            Issue.record("Expected timeout")
        }
    }

    @Test func transactionOption_allSimpleCases() {
        // Verify all cases compile and can be constructed
        let cases: [TransactionOption] = [
            .timeout(milliseconds: 1000),
            .priorityBatch,
            .prioritySystemImmediate,
            .readPriorityLow,
            .readPriorityHigh,
            .accessSystemKeys,
            .readServerSideCacheDisable,
        ]
        #expect(cases.count == 7)
    }

    @Test func transactionOption_isSendable() {
        let value: any Sendable = TransactionOption.priorityBatch
        #expect(value is TransactionOption)
    }

    // =========================================================================
    // MARK: - ConflictRangeType
    // =========================================================================

    @Test func conflictRangeType_cases() {
        let read = ConflictRangeType.read
        let write = ConflictRangeType.write
        // Verify distinct pattern matching
        switch read {
        case .read: break
        case .write: Issue.record("Expected read")
        }
        switch write {
        case .write: break
        case .read: Issue.record("Expected write")
        }
    }

    @Test func conflictRangeType_isSendable() {
        let value: any Sendable = ConflictRangeType.read
        #expect(value is ConflictRangeType)
    }
}
