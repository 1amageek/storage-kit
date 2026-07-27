import DatabaseTypes
import StorageKit
import Synchronization
import Testing

@Suite("Transaction mutation byte meter")
struct TransactionMutationByteMeterTests {
    @Test func setCountsLogicalPayloadWithoutMaterializingBytes() throws {
        let meter = try TransactionMutationByteMeter(maximumBytes: 22)
        let key = ByteString([0x01, 0x02])
        let value = ByteString([0x03, 0x04, 0x05])

        try meter.recordSet(key: key, value: value)

        #expect(meter.consumedBytes == 22)
    }

    @Test func rejectedMutationDoesNotAdvanceConsumedBytes() throws {
        let meter = try TransactionMutationByteMeter(maximumBytes: 31)
        let key = ByteString([0x01])

        try meter.recordClear(key: key)
        #expect(throws: TransactionMutationByteLimitError.self) {
            try meter.recordSet(
                key: key,
                value: ByteString([UInt8](repeating: 0, count: 4))
            )
        }

        #expect(meter.consumedBytes == 10)
    }

    @Test func arithmeticOverflowIsRejectedDeterministically() throws {
        let meter = try TransactionMutationByteMeter(maximumBytes: Int.max)
        try meter.consume(Int.max)

        #expect(throws: TransactionMutationByteLimitError.self) {
            try meter.consume(1)
        }
        #expect(meter.consumedBytes == Int.max)
    }

    @Test func concurrentScopesKeepAttemptMetersIndependent() async throws {
        let first = try TransactionMutationByteMeter(maximumBytes: 64)
        let second = try TransactionMutationByteMeter(maximumBytes: 64)
        let key = ByteString([0x01])

        async let firstWrite: Void = first.recordClear(key: key)
        async let secondWrite: Void = second.recordClear(key: key)
        _ = try await (firstWrite, secondWrite)

        #expect(first.consumedBytes == 10)
        #expect(second.consumedBytes == 10)
    }

    @Test func everyMutationFamilyHonorsItsExactBoundary() throws {
        let key = ByteString([0x01, 0x02])
        let value = ByteString([0x03, 0x04, 0x05])

        try expectBoundary(byteCount: 22) { meter in
            try meter.recordSet(key: key, value: value)
        }
        try expectBoundary(byteCount: 11) { meter in
            try meter.recordClear(key: key)
        }
        try expectBoundary(byteCount: 22) { meter in
            try meter.recordClearRange(beginKey: key, endKey: value)
        }
        try expectBoundary(byteCount: 26) { meter in
            try meter.recordAtomic(key: key, parameter: value)
        }
    }

    @Test func accountingDoesNotBorrowOrMaterializeOwnerBackedBytes() throws {
        let keyOwner = BorrowCountBytesOwner(bytes: [0x01, 0x02])
        let valueOwner = BorrowCountBytesOwner(bytes: [0x03, 0x04, 0x05])
        let meter = try TransactionMutationByteMeter(maximumBytes: 22)

        try meter.recordSet(
            key: ByteString(retaining: keyOwner),
            value: ByteString(retaining: valueOwner)
        )

        #expect(meter.consumedBytes == 22)
        #expect(keyOwner.borrowCount == 0)
        #expect(valueOwner.borrowCount == 0)
    }

    @Test func configurationIsSingleAssignmentAndPrecedesAdmission() throws {
        let configured = TransactionMutationByteMeter()
        try configured.configure(maximumBytes: 64)
        #expect(configured.maximumBytes == 64)
        #expect(throws: TransactionMutationByteLimitError.alreadyConfigured) {
            try configured.configure(maximumBytes: 64)
        }

        let admitted = TransactionMutationByteMeter()
        try admitted.recordClear(key: [0x01])
        #expect(
            throws: TransactionMutationByteLimitError.configurationAfterAdmission
        ) {
            try admitted.configure(maximumBytes: 64)
        }
    }

    private func expectBoundary(
        byteCount: Int,
        operation: (TransactionMutationByteMeter) throws -> Void
    ) throws {
        let accepted = try TransactionMutationByteMeter(
            maximumBytes: byteCount
        )
        try operation(accepted)
        #expect(accepted.consumedBytes == byteCount)

        let rejected = try TransactionMutationByteMeter(
            maximumBytes: byteCount - 1
        )
        #expect(throws: TransactionMutationByteLimitError.self) {
            try operation(rejected)
        }
        #expect(rejected.consumedBytes == 0)
    }
}

private final class BorrowCountBytesOwner: ByteStringOwner, Sendable {
    private let bytes: [UInt8]
    private let countState = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    var borrowCount: Int {
        countState.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        countState.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}
