import DatabaseTypes
import Testing
@testable import StorageKit

@Suite("Transaction read access")
struct TransactionReadAccessTests {
    @Test("read-only capability preserves the admitted transaction view")
    func readOnlyCapabilityPreservesTransactionView() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])

            let access: any TransactionReadAccess = transaction

            #expect(access.transactionDomain === transaction.transactionDomain)
            #expect(try await access.getValue(for: [0x01]) == [0xA1])
            #expect(
                try await access.getKey(
                    selector: .firstGreaterOrEqual([0x02])
                ) == [0x02]
            )
            let rows = try await access.collectRange(
                begin: [0x01],
                end: [0x03]
            )
            #expect(rows.map(\.0) == [[0x01], [0x02]])
            #expect(rows.map(\.1) == [[0xA1], [0xA2]])
        }
    }

    @Test("In-memory bounded point reads preserve read-your-writes and exact bounds")
    func inMemoryBoundedPointReadPreservesReadYourWrites() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let key: ByteString = [0x41]
        let value: ByteString = [0x10, 0x20, 0x30, 0x40]
        let transaction = try engine.createTransaction()

        try transaction.setValue(value, for: key)
        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count
            ) == value
        )

        var failure: StorageError?
        do {
            _ = try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count - 1
            )
        } catch let error as StorageError {
            failure = error
        }
        #expect(failure?.code == .valueTooLarge)
        #expect(failure?.backend == .inMemory)
        #expect(failure?.operation == .read)
        #expect(
            failure?.byteLimitViolation == StorageByteLimitViolation(
                resource: .value,
                observedByteCount: UInt64(value.count),
                maximumByteCount: UInt64(value.count - 1),
                measurement: .exact
            )
        )
        #expect(transaction.storageFailure == nil)

        #expect(
            try await transaction.getValue(
                for: key,
                snapshot: false,
                maximumByteCount: value.count
            ) == value
        )
        try await transaction.commit()

        let committed = try engine.createTransaction()
        let emptyKey: ByteString = [0x42]
        try committed.setValue([], for: emptyKey)
        #expect(
            try await committed.getValue(
                for: emptyKey,
                snapshot: false,
                maximumByteCount: 0
            ) == []
        )
        #expect(
            try await committed.getValue(
                for: [0x99],
                snapshot: true,
                maximumByteCount: 0
        ) == nil
        )
        try await committed.cancel()
    }

    @Test("Bounded point reads reject a negative maximum before reading")
    func boundedPointReadRejectsNegativeMaximum() async throws {
        let readCounter = ReadCounter()
        let access = DefaultTransactionReadAccess(
            value: [0x01],
            readCounter: readCounter
        )

        do {
            _ = try await access.getValue(
                for: [0x01],
                snapshot: true,
                maximumByteCount: -1
            )
            Issue.record("Expected an invalid point-read maximum")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .read)
            #expect(error.backend == .unknown)
            #expect(error.byteLimitViolation == nil)
        }
        #expect(await readCounter.value == 0)
    }

    @Test("Default bounded point-read implementation rejects oversized values")
    func defaultBoundedPointReadRejectsOversizedValue() async throws {
        let readCounter = ReadCounter()
        let access = DefaultTransactionReadAccess(
            value: ByteString([0x01, 0x02, 0x03]),
            readCounter: readCounter
        )

        do {
            _ = try await access.getValue(
                for: [0x01],
                snapshot: true,
                maximumByteCount: 2
            )
            Issue.record("Expected an oversized point-read value")
        } catch let error as StorageError {
            #expect(error.code == .valueTooLarge)
            #expect(error.backend == .unknown)
            #expect(
                error.byteLimitViolation?.observedByteCount == 3
            )
            #expect(
                error.byteLimitViolation?.maximumByteCount == 2
            )
        }
        #expect(await readCounter.value == 1)
    }
}

private struct DefaultTransactionReadAccess: TransactionReadAccess {
    let value: ByteString?
    let readCounter: ReadCounter?
    let transactionDomain = StorageTransactionDomain()

    init(value: ByteString?, readCounter: ReadCounter? = nil) {
        self.value = value
        self.readCounter = readCounter
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        await readCounter?.recordRead()
        return value
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        value
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        nil
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: KeyValueRangeResult([])
        )
    }
}

private actor ReadCounter {
    private(set) var value = 0

    func recordRead() {
        value += 1
    }
}
