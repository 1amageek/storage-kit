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
        let key = Bytes(retaining: keyOwner)[1..<4]
        let value = Bytes(retaining: valueOwner)[1..<5]
        let expectedKeyAddress = try byteAddress(of: key)
        let expectedValueAddress = try byteAddress(of: value)
        let backend = RecordingFoundationDBTransaction()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: FoundationDBTransactionDomain()
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
        let releaseRecorder = ObjectReleaseRecorder()
        var outputSource: ReleaseTrackedFoundationDBByteSource? = ReleaseTrackedFoundationDBByteSource(
            bytes: [0x11, 0x22, 0x33, 0x44],
            releaseRecorder: releaseRecorder
        )
        let inputOwner = BorrowCountingStorageBytesOwner(
            bytes: [0xA0, 0x01, 0x02, 0xA1]
        )
        let inputKey = Bytes(retaining: inputOwner)[1..<3]
        let expectedInputAddress = try byteAddress(of: inputKey)
        let backend: RecordingFoundationDBTransaction
        do {
            backend = RecordingFoundationDBTransaction(
                pointValue: FDB.ByteBuffer(try #require(outputSource))
            )
        }
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: FoundationDBTransactionDomain()
        )

        var result = try await transaction.getValue(
            for: inputKey,
            snapshot: true
        )

        #expect(outputSource?.borrowCount == 0)
        let expectedOutputAddress = try #require(outputSource).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let actualOutputAddress = try #require(result).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let readInvocation = try #require(backend.readInvocation)
        #expect(readInvocation.address == expectedInputAddress)
        #expect(readInvocation.count == inputKey.count)
        #expect(inputOwner.borrowCount == 2)
        #expect(actualOutputAddress == expectedOutputAddress)
        #expect(outputSource?.borrowCount == 2)

        outputSource = nil
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
        let begin = Bytes(retaining: beginOwner)[1..<3]
        let end = Bytes(retaining: endOwner)[1..<3]
        let expectedBeginAddress = try byteAddress(of: begin)
        let expectedEndAddress = try byteAddress(of: end)

        let keyReleaseRecorder = ObjectReleaseRecorder()
        let valueReleaseRecorder = ObjectReleaseRecorder()
        var keySource: ReleaseTrackedFoundationDBByteSource? = ReleaseTrackedFoundationDBByteSource(
            bytes: [0x31, 0x32],
            releaseRecorder: keyReleaseRecorder
        )
        var valueSource: ReleaseTrackedFoundationDBByteSource? = ReleaseTrackedFoundationDBByteSource(
            bytes: [0x41, 0x42, 0x43],
            releaseRecorder: valueReleaseRecorder
        )
        let backend: RecordingFoundationDBTransaction
        do {
            let page = RangeBatch(
                records: [(
                    FDB.ByteBuffer(try #require(keySource)),
                    FDB.ByteBuffer(try #require(valueSource))
                )],
                hasMore: false
            )
            backend = RecordingFoundationDBTransaction(rangePages: [page])
        }
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: FoundationDBTransactionDomain()
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
        #expect(keySource?.borrowCount == 0)
        #expect(valueSource?.borrowCount == 0)

        var iterator: FDBStorageRangeResult.AsyncIterator? = range.makeAsyncIterator()
        var row = try await iterator?.next()

        let rangeInvocation = try #require(backend.rangeInvocation)
        #expect(rangeInvocation.beginAddress == expectedBeginAddress)
        #expect(rangeInvocation.beginCount == begin.count)
        #expect(rangeInvocation.endAddress == expectedEndAddress)
        #expect(rangeInvocation.endCount == end.count)
        #expect(beginOwner.borrowCount == 2)
        #expect(endOwner.borrowCount == 2)
        #expect(keySource?.borrowCount == 0)
        #expect(valueSource?.borrowCount == 0)

        let expectedKeyAddress = try #require(keySource).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        let expectedValueAddress = try #require(valueSource).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        var returnedRow = try #require(row)
        let actualKeyAddress = returnedRow.0.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        let actualValueAddress = returnedRow.1.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        #expect(actualKeyAddress == expectedKeyAddress)
        #expect(actualValueAddress == expectedValueAddress)
        #expect(keySource?.borrowCount == 2)
        #expect(valueSource?.borrowCount == 2)

        try await iterator?.finish()
        iterator = nil
        keySource = nil
        valueSource = nil
        #expect(!keyReleaseRecorder.wasReleased)
        #expect(!valueReleaseRecorder.wasReleased)
        row = nil
        returnedRow = (Bytes(), Bytes())
        #expect(keyReleaseRecorder.wasReleased)
        #expect(valueReleaseRecorder.wasReleased)
    }

    @Test("Oversized StorageKit inputs fail before borrowing or dispatch")
    func oversizedInputsFailBeforeBorrowingOrDispatch() throws {
        let owner = OversizedBorrowCountingStorageBytesOwner(
            count: Int(Int32.max) + 1
        )
        let backend = RecordingFoundationDBTransaction()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: FoundationDBTransactionDomain()
        )

        do {
            try transaction.clear(key: Bytes(retaining: owner))
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

private final class RecordingFoundationDBTransaction: TransactionProtocol, Sendable {
    private struct State: Sendable {
        var pointValue: FDB.ByteBuffer?
        var rangePages: [RangeBatch]
        var nextRangePageIndex = 0
        var setInvocation: SetInvocationRecord?
        var readInvocation: ReadInvocationRecord?
        var rangeInvocation: RangeInvocationRecord?
        var clearCount = 0
    }

    private let state: Mutex<State>

    init(
        pointValue: FDB.ByteBuffer? = nil,
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

    func getValue<Key: FDB.ByteSource>(
        for key: Key,
        snapshot: Bool
    ) async throws -> FDB.ByteBuffer? {
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

    func setValue<Value: FDB.ByteSource, Key: FDB.ByteSource>(
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

    func clear<Key: FDB.ByteSource>(key: Key) throws {
        _ = key
        state.withLock { $0.clearCount += 1 }
    }

    func clearRange<Begin: FDB.ByteSource, End: FDB.ByteSource>(
        beginKey: Begin,
        endKey: End
    ) throws {
        _ = beginKey
        _ = endKey
    }

    func getKey(
        selector: FDB.KeySelector,
        snapshot: Bool
    ) async throws -> FDB.ByteBuffer? {
        _ = selector
        _ = snapshot
        return nil
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

    func commit() async throws -> Bool { true }
    func cancel() {}
    func getVersionstamp() async throws -> FDB.ByteBuffer? { nil }
    func setReadVersion(_ version: FDB.Version) { _ = version }
    func getReadVersion() async throws -> FDB.Version { 0 }
    func onError(_ error: FDBError) async throws { _ = error }

    func getEstimatedRangeSizeBytes<
        Begin: FDB.ByteSource,
        End: FDB.ByteSource
    >(beginKey: Begin, endKey: End) async throws -> Int {
        _ = beginKey
        _ = endKey
        return 0
    }

    func getRangeSplitPoints<
        Begin: FDB.ByteSource,
        End: FDB.ByteSource
    >(
        beginKey: Begin,
        endKey: End,
        chunkSize: Int
    ) async throws -> [FDB.ByteBuffer] {
        _ = beginKey
        _ = endKey
        _ = chunkSize
        return []
    }

    func getCommittedVersion() throws -> FDB.Version { 0 }
    func approximateSize() async throws -> Int64 { 0 }

    func atomicOp<Key: FDB.ByteSource, Parameter: FDB.ByteSource>(
        key: Key,
        param: Parameter,
        mutationType: FDB.MutationType
    ) throws {
        _ = key
        _ = param
        _ = mutationType
    }

    func addConflictRange<
        Begin: FDB.ByteSource,
        End: FDB.ByteSource
    >(
        beginKey: Begin,
        endKey: End,
        type: FDB.ConflictRangeType
    ) throws {
        _ = beginKey
        _ = endKey
        _ = type
    }

    func setOption<Value: FDB.ByteSource>(
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

private final class BorrowCountingStorageBytesOwner: BytesOwner, Sendable {
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

private final class ReleaseTrackedFoundationDBByteSource: FDB.ByteSource, Sendable {
    private let bytes: [UInt8]
    private let state = Mutex(0)
    private let releaseRecorder: ObjectReleaseRecorder

    init(bytes: [UInt8], releaseRecorder: ObjectReleaseRecorder) {
        self.bytes = bytes
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        releaseRecorder.recordRelease()
    }

    var count: Int { bytes.count }
    var borrowCount: Int { state.withLock { $0 } }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        state.withLock { $0 += 1 }
        return try bytes.withUnsafeBytes(body)
    }
}

private final class OversizedBorrowCountingStorageBytesOwner: BytesOwner, Sendable {
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

private final class ObjectReleaseRecorder: Sendable {
    private let state = Mutex(false)

    var wasReleased: Bool { state.withLock { $0 } }

    func recordRelease() {
        state.withLock { $0 = true }
    }
}

private func byteAddress(of bytes: Bytes) throws -> UInt {
    try bytes.withUnsafeBytes {
        try #require($0.baseAddress.map(UInt.init(bitPattern:)))
    }
}

private func borrowedRegion<Source: FDB.ByteSource>(
    of source: Source
) -> (address: UInt, count: Int) {
    source.withUnsafeBytes { bytes in
        precondition(bytes.count == source.count)
        guard let baseAddress = bytes.baseAddress else {
            preconditionFailure("Byte ownership test requires nonempty bytes")
        }
        return (UInt(bitPattern: baseAddress), bytes.count)
    }
}
