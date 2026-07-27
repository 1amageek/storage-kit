import DatabaseTypes
import Testing
import Foundation
import Synchronization
@testable import StorageKit

@Suite("KeyValueSequence Tests")
struct KeyValueSequenceTests {

    // =========================================================================
    // MARK: - Array Initializer
    // =========================================================================

    @Test func arrayInit_emptyResults() async throws {
        let seq = KeyValueSequence([])
        var count = 0
        for try await _ in seq {
            count += 1
        }
        #expect(count == 0)
    }

    @Test func arrayInit_iteratesAllElements() async throws {
        let input: [(key: ByteString, value: ByteString)] = [
            (key: [0x01], value: [10]),
            (key: [0x02], value: [20]),
            (key: [0x03], value: [30]),
        ]
        let seq = KeyValueSequence(input)

        var results: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq {
            results.append((key: key, value: value))
        }

        #expect(results.count == 3)
        #expect(results[0].key == [0x01])
        #expect(results[1].key == [0x02])
        #expect(results[2].key == [0x03])
    }

    @Test func arrayInit_singleElement() async throws {
        let seq = KeyValueSequence([(key: [0x01] as ByteString, value: [42] as ByteString)])
        var results: [(ByteString, ByteString)] = []
        for try await pair in seq {
            results.append(pair)
        }
        #expect(results.count == 1)
        #expect(results[0].0 == [0x01])
        #expect(results[0].1 == [42])
    }

    // =========================================================================
    // MARK: - AsyncStream Initializer
    // =========================================================================

    @Test func streamInit_iteratesAllElements() async throws {
        let stream = AsyncStream<(key: ByteString, value: ByteString)> { continuation in
            continuation.yield((key: [0x01], value: [10]))
            continuation.yield((key: [0x02], value: [20]))
            continuation.finish()
        }
        let seq = KeyValueSequence(stream)

        var results: [(key: ByteString, value: ByteString)] = []
        for try await (key, value) in seq {
            results.append((key: key, value: value))
        }

        #expect(results.count == 2)
        #expect(results[0].key == [0x01])
        #expect(results[1].key == [0x02])
    }

    @Test func streamInit_emptyStream() async throws {
        let stream = AsyncStream<(key: ByteString, value: ByteString)> { continuation in
            continuation.finish()
        }
        let seq = KeyValueSequence(stream)

        var count = 0
        for try await _ in seq {
            count += 1
        }
        #expect(count == 0)
    }

    // =========================================================================
    // MARK: - KeyValueRangeResult Error Path
    // =========================================================================

    @Test func rangeResult_errorThrowsOnIteration() async throws {
        let result = KeyValueRangeResult(error: StorageError.invalidOperation("test error"))

        do {
            for try await _ in result {
                Issue.record("Should not yield any elements")
            }
            Issue.record("Expected error to be thrown")
        } catch let error as StorageError {
            guard error.code == .invalidOperation else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
            #expect(error.message == "test error")
        }
    }

    @Test func rangeResult_errorIsConsumedOnce() async throws {
        let result = KeyValueRangeResult(
            error: StorageError.invalidOperation("test error")
        )
        var iterator = result.makeAsyncIterator()

        do {
            _ = try await iterator.next()
            Issue.record("Expected the first iteration to throw")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
        }

        let terminal = try await iterator.next()
        #expect(terminal == nil)
    }

    @Test func rangeResult_normalIteration() async throws {
        let result = KeyValueRangeResult([
            (key: [0x01] as ByteString, value: [10] as ByteString),
            (key: [0x02] as ByteString, value: [20] as ByteString),
        ])

        var keys: [ByteString] = []
        for try await (key, _) in result {
            keys.append(key)
        }
        #expect(keys == [[0x01], [0x02]])
    }

    @Test func rangeResult_emptyResults() async throws {
        let result = KeyValueRangeResult([])

        var count = 0
        for try await _ in result {
            count += 1
        }
        #expect(count == 0)
    }

    @Test func rangeResultIteratorReleasesBackingAfterFinalElement() async throws {
        let firstReleaseRecorder = RangeResultReleaseRecorder()
        let secondReleaseRecorder = RangeResultReleaseRecorder()
        var result: KeyValueRangeResult? = KeyValueRangeResult([
            (
                key: ByteString(retaining: RangeResultBytesOwner(
                    bytes: [0x01],
                    releaseRecorder: firstReleaseRecorder
                )),
                value: [0x11] as ByteString
            ),
            (
                key: ByteString(retaining: RangeResultBytesOwner(
                    bytes: [0x02],
                    releaseRecorder: secondReleaseRecorder
                )),
                value: [0x22] as ByteString
            ),
        ])
        var iterator = try #require(result?.makeAsyncIterator())
        result = nil

        var first = try await iterator.next()
        #expect(first?.0 == [0x01])
        #expect(firstReleaseRecorder.releaseCount == 0)
        #expect(secondReleaseRecorder.releaseCount == 0)
        first = nil
        #expect(firstReleaseRecorder.releaseCount == 0)

        var second = try await iterator.next()
        #expect(second?.0 == [0x02])
        #expect(firstReleaseRecorder.releaseCount == 1)
        #expect(secondReleaseRecorder.releaseCount == 0)
        second = nil
        #expect(secondReleaseRecorder.releaseCount == 1)

        let terminal = try await iterator.next()
        #expect(terminal == nil)
    }
}

private final class RangeResultBytesOwner: ByteStringOwner {
    let bytes: [UInt8]
    let releaseRecorder: RangeResultReleaseRecorder

    init(bytes: [UInt8], releaseRecorder: RangeResultReleaseRecorder) {
        self.bytes = bytes
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        releaseRecorder.recordRelease()
    }

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private final class RangeResultReleaseRecorder: Sendable {
    private let state = Mutex(0)

    var releaseCount: Int {
        state.withLock { $0 }
    }

    func recordRelease() {
        state.withLock { $0 += 1 }
    }
}
