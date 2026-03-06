import Testing
import Foundation
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
        let input: [(key: Bytes, value: Bytes)] = [
            (key: [0x01], value: [10]),
            (key: [0x02], value: [20]),
            (key: [0x03], value: [30]),
        ]
        let seq = KeyValueSequence(input)

        var results: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq {
            results.append((key: key, value: value))
        }

        #expect(results.count == 3)
        #expect(results[0].key == [0x01])
        #expect(results[1].key == [0x02])
        #expect(results[2].key == [0x03])
    }

    @Test func arrayInit_singleElement() async throws {
        let seq = KeyValueSequence([(key: [0x01] as Bytes, value: [42] as Bytes)])
        var results: [(Bytes, Bytes)] = []
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
        let stream = AsyncStream<(key: Bytes, value: Bytes)> { continuation in
            continuation.yield((key: [0x01], value: [10]))
            continuation.yield((key: [0x02], value: [20]))
            continuation.finish()
        }
        let seq = KeyValueSequence(stream)

        var results: [(key: Bytes, value: Bytes)] = []
        for try await (key, value) in seq {
            results.append((key: key, value: value))
        }

        #expect(results.count == 2)
        #expect(results[0].key == [0x01])
        #expect(results[1].key == [0x02])
    }

    @Test func streamInit_emptyStream() async throws {
        let stream = AsyncStream<(key: Bytes, value: Bytes)> { continuation in
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
            guard case .invalidOperation(let msg) = error else {
                Issue.record("Expected invalidOperation, got \(error)")
                return
            }
            #expect(msg == "test error")
        }
    }

    @Test func rangeResult_normalIteration() async throws {
        let result = KeyValueRangeResult([
            (key: [0x01] as Bytes, value: [10] as Bytes),
            (key: [0x02] as Bytes, value: [20] as Bytes),
        ])

        var keys: [Bytes] = []
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
}
