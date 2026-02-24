import Testing
import Foundation
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLiteStorageEngine Tests")
struct SQLiteStorageEngineTests {

    @Test func getSetBasic() async throws {
        let engine = try SQLiteStorageEngine()
        try await engine.withTransaction { tx in
            tx.setValue([1, 2, 3], for: [0, 1])
            let value = try await tx.getValue(for: [0, 1])
            #expect(value == [1, 2, 3])
        }
    }

    @Test func getNonExistentKey() async throws {
        let engine = try SQLiteStorageEngine()
        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0xFF])
            #expect(value == nil)
        }
    }

    @Test func overwriteValue() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1], for: [0x01])
        tx.setValue([2], for: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [2])
        try await tx.commit()
    }

    @Test func clearKey() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([1, 2, 3], for: [0x01])
        }

        try await engine.withTransaction { tx in
            tx.clear(key: [0x01])
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0x01])
            #expect(value == nil)
        }
    }

    @Test func clearWithinTransaction() async throws {
        let engine = try SQLiteStorageEngine()
        let tx = try engine.createTransaction()
        tx.setValue([1, 2, 3], for: [0x01])
        tx.clear(key: [0x01])
        let value = try await tx.getValue(for: [0x01])
        #expect(value == nil)
        try await tx.commit()
    }

    @Test func clearRange() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
            tx.setValue([3], for: [0x03])
            tx.setValue([4], for: [0x04])
        }

        // [0x02, 0x04) を削除
        try await engine.withTransaction { tx in
            tx.clearRange(begin: [0x02], end: [0x04])
        }

        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: [0x01])
            let v2 = try await tx.getValue(for: [0x02])
            let v3 = try await tx.getValue(for: [0x03])
            let v4 = try await tx.getValue(for: [0x04])
            #expect(v1 == [1])
            #expect(v2 == nil)
            #expect(v3 == nil)
            #expect(v4 == [4])
        }
    }

    @Test func getRangeForward() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
            tx.setValue([40], for: [0x04])
            tx.setValue([50], for: [0x05])
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x02], end: [0x05], limit: 0, reverse: false
            )
            var collected: [(key: Bytes, value: Bytes)] = []
            for try await item in results {
                collected.append(item)
            }
            #expect(collected.count == 3)
            #expect(collected[0].key == [0x02])
            #expect(collected[1].key == [0x03])
            #expect(collected[2].key == [0x04])
        }
    }

    @Test func getRangeReverse() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: [0x01])
            tx.setValue([20], for: [0x02])
            tx.setValue([30], for: [0x03])
            tx.setValue([40], for: [0x04])
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x01], end: [0x05], limit: 0, reverse: true
            )
            var collected: [(key: Bytes, value: Bytes)] = []
            for try await item in results {
                collected.append(item)
            }
            #expect(collected.count == 4)
            #expect(collected[0].key == [0x04])
            #expect(collected[3].key == [0x01])
        }
    }

    @Test func getRangeWithLimit() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            for i: UInt8 in 0..<10 {
                tx.setValue([i], for: [i])
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x00], end: [0xFF], limit: 3, reverse: false
            )
            var collected: [(key: Bytes, value: Bytes)] = []
            for try await item in results {
                collected.append(item)
            }
            #expect(collected.count == 3)
        }
    }

    @Test func commitPersists() async throws {
        let engine = try SQLiteStorageEngine()

        let tx1 = try engine.createTransaction()
        tx1.setValue([42], for: [0x01])
        try await tx1.commit()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: [0x01])
        #expect(value == [42])
        try await tx2.commit()
    }

    @Test func cancelDoesNotPersist() async throws {
        let engine = try SQLiteStorageEngine()

        let tx1 = try engine.createTransaction()
        tx1.setValue([42], for: [0x01])
        tx1.cancel()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: [0x01])
        #expect(value == nil)
        try await tx2.commit()
    }

    @Test func readYourWrites() async throws {
        let engine = try SQLiteStorageEngine()

        let tx = try engine.createTransaction()
        tx.setValue([100], for: [0x01])

        // 同一トランザクション内で書き込みが読める（バッファ経由）
        let value = try await tx.getValue(for: [0x01])
        #expect(value == [100])
        try await tx.commit()
    }

    @Test func readYourWritesClearRange() async throws {
        let engine = try SQLiteStorageEngine()

        // まず書き込み
        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x01])
            tx.setValue([2], for: [0x02])
        }

        // clearRange 後に getValue
        let tx = try engine.createTransaction()
        tx.clearRange(begin: [0x01], end: [0x03])
        let v1 = try await tx.getValue(for: [0x01])
        let v2 = try await tx.getValue(for: [0x02])
        #expect(v1 == nil)
        #expect(v2 == nil)
        try await tx.commit()
    }

    @Test func lexicographicOrder() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([5], for: [0x05])
            tx.setValue([1], for: [0x01])
            tx.setValue([3], for: [0x03])
            tx.setValue([2], for: [0x02])
            tx.setValue([4], for: [0x04])
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x00], end: [0xFF], limit: 0, reverse: false
            )
            var keys: [Bytes] = []
            for try await item in results {
                keys.append(item.key)
            }
            #expect(keys == [[0x01], [0x02], [0x03], [0x04], [0x05]])
        }
    }

    @Test func largeDataset() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            for i: UInt16 in 0..<1000 {
                let key = withUnsafeBytes(of: i.bigEndian) { Array($0) }
                let value = withUnsafeBytes(of: i) { Array($0) }
                tx.setValue(value, for: key)
            }
        }

        try await engine.withTransaction { tx in
            let allResults = try await tx.getRange(
                begin: [0x00, 0x00], end: [0xFF, 0xFF], limit: 0, reverse: false
            )
            var count = 0
            for try await _ in allResults {
                count += 1
            }
            #expect(count == 1000)
        }
    }

    @Test func emptyRangeReturnsNothing() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([1], for: [0x05])
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: [0x01], end: [0x03], limit: 0, reverse: false
            )
            var count = 0
            for try await _ in results {
                count += 1
            }
            #expect(count == 0)
        }
    }

    @Test func filePersistence() async throws {
        let tmpDir = FileManager.default.temporaryDirectory
        let dbPath = tmpDir.appendingPathComponent("test-\(UUID().uuidString).sqlite").path

        defer { try? FileManager.default.removeItem(atPath: dbPath) }

        // 書き込み
        do {
            let engine = try SQLiteStorageEngine(path: dbPath)
            try await engine.withTransaction { tx in
                tx.setValue([1, 2, 3], for: [0x01])
            }
            engine.close()
        }

        // 再オープンして読み取り
        do {
            let engine = try SQLiteStorageEngine(path: dbPath)
            try await engine.withTransaction { tx in
                let value = try await tx.getValue(for: [0x01])
                #expect(value == [1, 2, 3])
            }
            engine.close()
        }
    }

    @Test func tupleKeyIntegration() async throws {
        let engine = try SQLiteStorageEngine()

        let space = Subspace("users")

        try await engine.withTransaction { tx in
            let key1 = space.pack(Tuple(Int64(1)))
            let key2 = space.pack(Tuple(Int64(2)))
            let key3 = space.pack(Tuple(Int64(3)))
            tx.setValue([10], for: key1)
            tx.setValue([20], for: key2)
            tx.setValue([30], for: key3)
        }

        try await engine.withTransaction { tx in
            let (begin, end) = space.range()
            let results = try await tx.getRange(
                begin: begin, end: end, limit: 0, reverse: false
            )
            var count = 0
            for try await item in results {
                let tuple = try space.unpack(item.key)
                let elements = try Tuple.unpack(from: tuple.pack())
                #expect(elements[0] is Int64)
                count += 1
            }
            #expect(count == 3)
        }
    }

    @Test func withTransactionAutoCommit() async throws {
        let engine = try SQLiteStorageEngine()

        try await engine.withTransaction { tx in
            tx.setValue([99], for: [0xAA])
        }

        // withTransaction 後にデータが永続化されている
        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: [0xAA])
            #expect(value == [99])
        }
    }
}
