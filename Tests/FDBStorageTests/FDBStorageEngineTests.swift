import Testing
import Foundation
@testable import StorageKit
@testable import FDBStorage
import FoundationDB

/// FDB が起動していない環境ではスキップされる
///
/// テスト実行前に FoundationDB が起動している必要がある:
/// ```
/// sudo launchctl load /Library/LaunchDaemons/com.apple.foundationdb.fdbmonitor.plist
/// ```
@Suite("FDBStorageEngine Tests")
struct FDBStorageEngineTests {

    init() async throws {
        if !FDBClient.isInitialized {
            try await FDBClient.initialize()
        }
    }

    private func makeEngine() throws -> FDBStorageEngine {
        let database = try FDBClient.openDatabase()
        return FDBStorageEngine(database: database)
    }

    /// テスト用のユニークなキープレフィックス
    private func testPrefix() -> Bytes {
        let uuid = UUID().uuidString.prefix(8)
        return Array("_test_\(uuid)_".utf8)
    }

    private func prefixedKey(_ prefix: Bytes, _ suffix: [UInt8]) -> Bytes {
        prefix + suffix
    }

    @Test func getSetValue() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            let key = prefixedKey(prefix, [0x01])
            tx.setValue([1, 2, 3], for: key)
            let value = try await tx.getValue(for: key)
            #expect(value == [1, 2, 3])
        }

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF])
        }
    }

    @Test func clearKey() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        try await engine.withTransaction { tx in
            tx.setValue([42], for: key)
        }

        try await engine.withTransaction { tx in
            tx.clear(key: key)
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: key)
            #expect(value == nil)
        }
    }

    @Test func clearRange() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            tx.setValue([1], for: prefixedKey(prefix, [0x01]))
            tx.setValue([2], for: prefixedKey(prefix, [0x02]))
            tx.setValue([3], for: prefixedKey(prefix, [0x03]))
            tx.setValue([4], for: prefixedKey(prefix, [0x04]))
        }

        try await engine.withTransaction { tx in
            tx.clearRange(
                begin: prefixedKey(prefix, [0x02]),
                end: prefixedKey(prefix, [0x04])
            )
        }

        try await engine.withTransaction { tx in
            let v1 = try await tx.getValue(for: prefixedKey(prefix, [0x01]))
            let v2 = try await tx.getValue(for: prefixedKey(prefix, [0x02]))
            let v3 = try await tx.getValue(for: prefixedKey(prefix, [0x03]))
            let v4 = try await tx.getValue(for: prefixedKey(prefix, [0x04]))
            #expect(v1 == [1])
            #expect(v2 == nil)
            #expect(v3 == nil)
            #expect(v4 == [4])
        }

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF])
        }
    }

    @Test func getRange() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            tx.setValue([10], for: prefixedKey(prefix, [0x01]))
            tx.setValue([20], for: prefixedKey(prefix, [0x02]))
            tx.setValue([30], for: prefixedKey(prefix, [0x03]))
            tx.setValue([40], for: prefixedKey(prefix, [0x04]))
            tx.setValue([50], for: prefixedKey(prefix, [0x05]))
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: prefixedKey(prefix, [0x02]),
                end: prefixedKey(prefix, [0x05]),
                limit: 0,
                reverse: false
            )
            var collected: [(key: Bytes, value: Bytes)] = []
            for try await item in results {
                collected.append(item)
            }
            #expect(collected.count == 3)
            #expect(collected[0].value == [20])
            #expect(collected[2].value == [40])
        }

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF])
        }
    }

    @Test func commitPersists() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        let tx1 = try engine.createTransaction()
        tx1.setValue([42], for: key)
        try await tx1.commit()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: key)
        #expect(value == [42])
        tx2.cancel()

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF])
        }
    }

    @Test func cancelDiscards() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        let tx1 = try engine.createTransaction()
        tx1.setValue([42], for: key)
        tx1.cancel()

        let tx2 = try engine.createTransaction()
        let value = try await tx2.getValue(for: key)
        #expect(value == nil)
        tx2.cancel()
    }

    @Test func withTransactionAutoCommit() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()
        let key = prefixedKey(prefix, [0x01])

        try await engine.withTransaction { tx in
            tx.setValue([99], for: key)
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: key)
            #expect(value == [99])
        }

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF])
        }
    }

    @Test func fdbTransactionAccess() async throws {
        let engine = try makeEngine()
        let tx = try engine.createTransaction()
        _ = tx.fdbTransaction
        tx.cancel()
    }

    @Test func largeRangeScan() async throws {
        let engine = try makeEngine()
        let prefix = testPrefix()

        try await engine.withTransaction { tx in
            for i: UInt16 in 0..<500 {
                let key = prefix + withUnsafeBytes(of: i.bigEndian) { Array($0) }
                let value = withUnsafeBytes(of: i) { Array($0) }
                tx.setValue(value, for: key)
            }
        }

        try await engine.withTransaction { tx in
            let results = try await tx.getRange(
                begin: prefix,
                end: prefix + [0xFF, 0xFF],
                limit: 0,
                reverse: false
            )
            var count = 0
            for try await _ in results {
                count += 1
            }
            #expect(count == 500)
        }

        // cleanup
        try await engine.withTransaction { tx in
            tx.clearRange(begin: prefix, end: prefix + [0xFF, 0xFF])
        }
    }
}
