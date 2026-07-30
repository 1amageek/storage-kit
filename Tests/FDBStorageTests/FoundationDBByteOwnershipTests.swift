import DatabaseTypes
import FoundationDB
import StorageKit
import Synchronization
import Testing
@testable import FDBStorage

@Suite("FoundationDB byte ownership")
struct FoundationDBByteOwnershipTests {
    @Test("Write inputs preserve StorageKit buffer addresses")
    func writeInputsPreserveStorageKitBufferAddresses() throws {
        let keyOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xA0, 0x10, 0x20, 0x30, 0xA1]
        )
        let valueOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xB0, 0x40, 0x50, 0x60, 0x70, 0xB1]
        )
        let key = ByteString(retaining: keyOwner)[1..<4]
        let value = ByteString(retaining: valueOwner)[1..<5]
        let expectedKeyAddress = try byteAddress(of: key)
        let expectedValueAddress = try byteAddress(of: value)
        let backend = RecordingTransaction()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        #expect(keyOwner.borrowCount == 1)
        #expect(valueOwner.borrowCount == 1)

        try transaction.setValue(value, for: key)

        let setInvocation = try #require(backend.setInvocation)
        #expect(setInvocation.keyAddress == expectedKeyAddress)
        #expect(setInvocation.keyCount == key.count)
        #expect(setInvocation.valueAddress == expectedValueAddress)
        #expect(setInvocation.valueCount == value.count)
        #expect(keyOwner.borrowCount == 2)
        #expect(valueOwner.borrowCount == 2)
    }

    @Test("Point reads retain FoundationDB output without copying")
    func pointReadsRetainFoundationDBOutputWithoutCopying() async throws {
        let releaseRecorder = ByteReleaseRecorder()
        var outputOwner: ReleaseTrackedByteStringOwner? =
            ReleaseTrackedByteStringOwner(
                bytes: [0x11, 0x22, 0x33, 0x44],
                releaseRecorder: releaseRecorder
            )
        var outputBytes: ByteString? = ByteString(
            retaining: try #require(outputOwner)
        )
        let expectedOutputAddress = try byteAddress(
            of: try #require(outputBytes)
        )
        let inputOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xA0, 0x01, 0x02, 0xA1]
        )
        let inputKey = ByteString(retaining: inputOwner)[1..<3]
        let expectedInputAddress = try byteAddress(of: inputKey)
        let backend = RecordingTransaction(pointValue: outputBytes)
        outputOwner = nil
        outputBytes = nil
        #expect(!releaseRecorder.wasReleased)
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        var result = try await transaction.getValue(
            for: inputKey,
            snapshot: true
        )

        let actualOutputAddress = try #require(result).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let readInvocation = try #require(backend.readInvocation)
        #expect(readInvocation.address == expectedInputAddress)
        #expect(readInvocation.count == inputKey.count)
        #expect(inputOwner.borrowCount == 2)
        #expect(actualOutputAddress == expectedOutputAddress)
        #expect(!releaseRecorder.wasReleased)

        result = nil
        #expect(releaseRecorder.wasReleased)
    }

    @Test("Range selectors and rows preserve their source addresses")
    func rangeSelectorsAndRowsPreserveSourceAddresses() async throws {
        let beginOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xA0, 0x10, 0x20, 0xA1]
        )
        let endOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xB0, 0x30, 0x40, 0xB1]
        )
        let begin = ByteString(retaining: beginOwner)[1..<3]
        let end = ByteString(retaining: endOwner)[1..<3]
        let expectedBeginAddress = try byteAddress(of: begin)
        let expectedEndAddress = try byteAddress(of: end)

        let keyBytes = ByteString([0x31, 0x32])
        let valueBytes = ByteString([0x41, 0x42, 0x43])
        let expectedKeyAddress = try keyBytes.withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let expectedValueAddress = try valueBytes.withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let backend: RecordingTransaction
        do {
            let page = RangeBatch(
                records: [FDB.KeyValue(key: keyBytes, value: valueBytes)],
                hasMore: false
            )
            backend = RecordingTransaction(rangePages: [page])
        }
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let range = transaction.getRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )

        #expect(beginOwner.borrowCount == 1)
        #expect(endOwner.borrowCount == 1)

        var cursor: FDBStorageRangeResult.Cursor? = range.makeCursor()
        var row = try await cursor?.next()

        let rangeInvocation = try #require(backend.rangeInvocation)
        #expect(rangeInvocation.beginAddress == expectedBeginAddress)
        #expect(rangeInvocation.beginCount == begin.count)
        #expect(rangeInvocation.endAddress == expectedEndAddress)
        #expect(rangeInvocation.endCount == end.count)
        #expect(beginOwner.borrowCount == 2)
        #expect(endOwner.borrowCount == 2)
        var returnedRow = try #require(row)
        let actualKeyAddress = returnedRow.0.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        let actualValueAddress = returnedRow.1.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        #expect(actualKeyAddress == expectedKeyAddress)
        #expect(actualValueAddress == expectedValueAddress)

        try await cursor?.finish()
        cursor = nil
        row = nil
        returnedRow = (ByteString(), ByteString())
    }

    @Test("Oversized StorageKit inputs fail before borrowing or dispatch")
    func oversizedInputsFailBeforeBorrowingOrDispatch() throws {
        let owner = OversizedBorrowCountingStorageBytesOwner(
            count: Int(Int32.max) + 1
        )
        let backend = RecordingTransaction()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        do {
            try transaction.clear(key: ByteString(retaining: owner))
            Issue.record("Oversized key unexpectedly reached FoundationDB")
        } catch let error as StorageError {
            #expect(error.code == .backendContractViolation)
            #expect(error.operation == .delete)
            #expect(error.backend == .foundationDB)
        }

        #expect(owner.borrowCount == 0)
        #expect(backend.clearCount == 0)
    }
}

private struct SetInvocationRecord: Sendable {
    let keyAddress: UInt
    let keyCount: Int
    let valueAddress: UInt
    let valueCount: Int
}

private struct ReadInvocationRecord: Sendable {
    let address: UInt
    let count: Int
}

private struct RangeInvocationRecord: Sendable {
    let beginAddress: UInt
    let beginCount: Int
    let endAddress: UInt
    let endCount: Int
}

private final class RecordingTransaction: TransactionProtocol, Sendable {
    private struct State: Sendable {
        var pointValue: ByteString?
        var rangePages: [RangeBatch]
        var nextRangePageIndex = 0
        var setInvocation: SetInvocationRecord?
        var readInvocation: ReadInvocationRecord?
        var rangeInvocation: RangeInvocationRecord?
        var clearCount = 0
    }

    private let state: Mutex<State>

    init(
        pointValue: ByteString? = nil,
        rangePages: [RangeBatch] = []
    ) {
        self.state = Mutex(State(
            pointValue: pointValue,
            rangePages: rangePages
        ))
    }

    var setInvocation: SetInvocationRecord? { state.withLock { $0.setInvocation } }
    var readInvocation: ReadInvocationRecord? { state.withLock { $0.readInvocation } }
    var rangeInvocation: RangeInvocationRecord? { state.withLock { $0.rangeInvocation } }
    var clearCount: Int { state.withLock { $0.clearCount } }

    func getValue<Key: FDB.ByteInput>(
        for key: Key,
        snapshot: Bool
    ) async throws -> ByteString? {
        _ = snapshot
        let keyRegion = borrowedRegion(of: key)
        return state.withLock { state in
            state.readInvocation = ReadInvocationRecord(
                address: keyRegion.address,
                count: keyRegion.count
            )
            let value = state.pointValue
            state.pointValue = nil
            return value
        }
    }

    func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(
        _ value: Value,
        for key: Key
    ) throws {
        let keyRegion = borrowedRegion(of: key)
        let valueRegion = borrowedRegion(of: value)
        state.withLock { state in
            state.setInvocation = SetInvocationRecord(
                keyAddress: keyRegion.address,
                keyCount: keyRegion.count,
                valueAddress: valueRegion.address,
                valueCount: valueRegion.count
            )
        }
    }

    func clear<Key: FDB.ByteInput>(key: Key) throws {
        _ = key
        state.withLock { $0.clearCount += 1 }
    }

    func clearRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(
        beginKey: Begin,
        endKey: End
    ) throws {
        _ = beginKey
        _ = endKey
    }

    func getKey(
        selector: FDB.KeySelector,
        snapshot: Bool
    ) async throws -> ByteString {
        _ = selector
        _ = snapshot
        return []
    }

    func readRangeBatch(
        from begin: FDB.KeySelector,
        to end: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> RangeBatch {
        _ = limit
        _ = targetBytes
        _ = streamingMode
        _ = iteration
        _ = reverse
        _ = snapshot
        let beginRegion = borrowedRegion(of: begin.key)
        let endRegion = borrowedRegion(of: end.key)
        return state.withLock { state in
            state.rangeInvocation = RangeInvocationRecord(
                beginAddress: beginRegion.address,
                beginCount: beginRegion.count,
                endAddress: endRegion.address,
                endCount: endRegion.count
            )
            guard state.nextRangePageIndex < state.rangePages.count else {
                return RangeBatch(records: [], hasMore: false)
            }
            let page = state.rangePages[state.nextRangePageIndex]
            state.rangePages[state.nextRangePageIndex] = RangeBatch(
                records: [],
                hasMore: false
            )
            state.nextRangePageIndex += 1
            return page
        }
    }

    func commit() async throws {}
    func cancel() {}
    func requestVersionstamp() -> any FDB.PendingTransactionVersionstamp {
        FixedTransactionVersionstamp()
    }
    func setReadVersion(_ version: FDB.Version) { _ = version }
    func getReadVersion() async throws -> FDB.Version { 0 }
    func onError(_ error: FDBError) async throws { _ = error }

    func getEstimatedRangeSizeBytes<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(beginKey: Begin, endKey: End) async throws -> Int64 {
        _ = beginKey
        _ = endKey
        return 0
    }

    func getRangeSplitPoints<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        chunkSize: Int64
    ) async throws -> [ByteString] {
        _ = beginKey
        _ = endKey
        _ = chunkSize
        return []
    }

    func getCommittedVersion() throws -> FDB.Version { 0 }
    func approximateSize() async throws -> Int64 { 0 }

    func atomicOp<Key: FDB.ByteInput, Parameter: FDB.ByteInput>(
        key: Key,
        param: Parameter,
        mutationType: FDB.MutationType
    ) throws {
        _ = key
        _ = param
        _ = mutationType
    }

    func addConflictRange<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        type: FDB.ConflictRangeType
    ) throws {
        _ = beginKey
        _ = endKey
        _ = type
    }

    func setOption<Value: FDB.ByteInput>(
        to value: Value,
        forOption option: FDB.TransactionOption
    ) throws {
        _ = value
        _ = option
    }

    func setOption(forOption option: FDB.TransactionOption) throws {
        _ = option
    }

    func setOption(
        to value: String,
        forOption option: FDB.TransactionOption
    ) throws {
        _ = value
        _ = option
    }

    func setOption(
        to value: Int,
        forOption option: FDB.TransactionOption
    ) throws {
        _ = value
        _ = option
    }
}

private final class BorrowCountingStorageBytesOwner: ByteStringOwner, Sendable {
    private let bytes: [UInt8]
    private let state = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }
    var borrowCount: Int { state.withLock { $0 } }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}

private final class ReleaseTrackedByteStringOwner:
        ByteStringOwner,
        Sendable {
    let count: Int

    private let bytes: [UInt8]
    private let releaseRecorder: ByteReleaseRecorder

    init(bytes: [UInt8], releaseRecorder: ByteReleaseRecorder) {
        self.bytes = bytes
        self.count = bytes.count
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        releaseRecorder.recordRelease()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private final class ByteReleaseRecorder: Sendable {
    private let state = Mutex(false)

    var wasReleased: Bool { state.withLock { $0 } }

    func recordRelease() {
        state.withLock { $0 = true }
    }
}

private final class OversizedBorrowCountingStorageBytesOwner: ByteStringOwner, Sendable {
    let count: Int
    private let state = Mutex(0)

    init(count: Int) {
        self.count = count
    }

    var borrowCount: Int { state.withLock { $0 } }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try body(UnsafeRawBufferPointer(start: nil, count: 0))
    }
}

private func byteAddress(of bytes: ByteString) throws -> UInt {
    try bytes.withUnsafeBytes {
        try #require($0.baseAddress.map(UInt.init(bitPattern:)))
    }
}

private func borrowedRegion<Source: FDB.ByteInput>(
    of source: Source
) -> (address: UInt, count: Int) {
    source.withUnsafeBytes { bytes in
        guard let baseAddress = bytes.baseAddress else {
            preconditionFailure("Byte ownership test requires nonempty bytes")
        }
        return (UInt(bitPattern: baseAddress), bytes.count)
    }
}
