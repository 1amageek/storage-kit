import DatabaseTypes
import Testing
@testable import StorageKit
@testable import SQLiteStorage

@Suite("SQLite key selector behavior")
struct SQLiteKeySelectorTests {
    @Test("Selector reads materialize one key and no value payload")
    func selectorReadCopiesOnlyTheSelectedKey() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString([UInt8](repeating: 0xA5, count: 1_048_576)),
                for: [0x42]
            )
        }

        let transaction = try engine.createTransaction()
        let baseline = engine.rangeInstrumentation
        let key = try await transaction.getKey(
            selector: .firstGreaterOrEqual([0x00]),
            snapshot: true
        )
        let measured = engine.rangeInstrumentation

        #expect(key == [0x42])
        #expect(
            measured.payloadCopyCount == baseline.payloadCopyCount + 1
        )
        try await transaction.cancel()
    }

    @Test("Selector matrix matches the canonical in-memory implementation")
    func selectorMatrixMatchesCanonicalSemantics() async throws {
        let sqliteEngine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { sqliteEngine.shutdown() }
        let memoryEngine = InMemoryEngine()
        let sqliteTransaction = try sqliteEngine.createTransaction()
        let memoryTransaction = try memoryEngine.createTransaction()
        let keys: [ByteString] = [
            [],
            [0x00],
            [0x01],
            [0x01, 0x00],
            [0x7F],
            [0xFF],
            [0xFF, 0x00],
        ]
        for (index, key) in keys.enumerated() {
            let value: ByteString = [UInt8(index)]
            try sqliteTransaction.setValue(value, for: key)
            try memoryTransaction.setValue(value, for: key)
        }

        let probes: [ByteString] = [
            [],
            [0x00],
            [0x00, 0xFF],
            [0x01],
            [0x02],
            [0xFF],
            [0xFF, 0x01],
        ]
        for probe in probes {
            for orEqual in [false, true] {
                for offset in -5 ... 5 {
                    let selector = KeySelector(
                        key: probe,
                        orEqual: orEqual,
                        offset: offset
                    )
                    let expected = try await memoryTransaction.getKey(
                        selector: selector,
                        snapshot: true
                    )
                    let actual = try await sqliteTransaction.getKey(
                        selector: selector,
                        snapshot: true
                    )
                    #expect(
                        actual == expected,
                        "probe=\(probe), orEqual=\(orEqual), offset=\(offset)"
                    )
                }
            }
        }

        try await sqliteTransaction.commit()
        try await memoryTransaction.commit()
    }

    @Test("Standard selectors resolve against buffered writes")
    func standardSelectorsResolveAgainstBufferedWrites() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()
        for key: UInt8 in 1 ... 4 {
            try transaction.setValue([key], for: [key])
        }

        #expect(try await transaction.getKey(
            selector: .firstGreaterOrEqual([2]),
            snapshot: true
        ) == [2])
        #expect(try await transaction.getKey(
            selector: .firstGreaterThan([2]),
            snapshot: true
        ) == [3])
        #expect(try await transaction.getKey(
            selector: .lastLessOrEqual([2]),
            snapshot: true
        ) == [2])
        #expect(try await transaction.getKey(
            selector: .lastLessThan([2]),
            snapshot: true
        ) == [1])

        try await transaction.commit()
    }

    @Test("Arbitrary positive and negative offsets preserve FDB semantics")
    func arbitraryOffsetsPreserveSemantics() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()
        for key: UInt8 in 1 ... 4 {
            try transaction.setValue([key], for: [key])
        }

        #expect(try await transaction.getKey(
            selector: KeySelector(key: [2], orEqual: false, offset: 2),
            snapshot: false
        ) == [3])
        #expect(try await transaction.getKey(
            selector: KeySelector(key: [2], orEqual: true, offset: 2),
            snapshot: false
        ) == [4])
        #expect(try await transaction.getKey(
            selector: KeySelector(key: [2], orEqual: true, offset: -1),
            snapshot: false
        ) == [1])
        #expect(try await transaction.getKey(
            selector: KeySelector(key: [3], orEqual: false, offset: -1),
            snapshot: false
        ) == [1])

        try await transaction.commit()
    }

    @Test("Selectors return nil beyond either keyspace boundary")
    func selectorsReturnNilBeyondBoundaries() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()
        try transaction.setValue([1], for: [1])
        try transaction.setValue([2], for: [2])

        #expect(try await transaction.getKey(
            selector: KeySelector(key: [2], orEqual: true, offset: 2),
            snapshot: true
        ) == nil)
        #expect(try await transaction.getKey(
            selector: KeySelector(key: [1], orEqual: false, offset: 0),
            snapshot: true
        ) == nil)

        try await transaction.commit()
    }

    @Test("Selector resolution does not assume FF is the final key")
    func selectorSupportsKeysAfterFF() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()
        try transaction.setValue([1], for: [])
        try transaction.setValue([2], for: [0xFF])
        try transaction.setValue([3], for: [0xFF, 0x00])

        #expect(try await transaction.getKey(
            selector: .firstGreaterOrEqual([]),
            snapshot: true
        ) == [])
        #expect(try await transaction.getKey(
            selector: .firstGreaterThan([0xFF]),
            snapshot: true
        ) == [0xFF, 0x00])

        try await transaction.commit()
    }

    @Test("Unrepresentable offsets fail without poisoning the transaction")
    func unrepresentableOffsetIsTypedAndNonfatal() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()

        do {
            _ = try await transaction.getKey(
                selector: KeySelector(
                    key: [1],
                    orEqual: false,
                    offset: .min
                ),
                snapshot: true
            )
            Issue.record("Expected an unrepresentable selector failure")
        } catch let error as StorageError {
            #expect(error.code == .resourceUnavailable)
            #expect(error.operation == .rangeRead)
            #expect(error.backend == .sqlite)
        }

        try transaction.setValue([9], for: [9])
        #expect(try await transaction.getValue(for: [9]) == [9])
        try await transaction.commit()
    }

    @Test("Terminal transactions reject selector reads with a typed error")
    func terminalTransactionRejectsSelectorRead() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { engine.shutdown() }
        let transaction = try engine.createTransaction()
        try await transaction.cancel()

        do {
            _ = try await transaction.getKey(
                selector: .firstGreaterOrEqual([0]),
                snapshot: true
            )
            Issue.record("Expected a terminal transaction failure")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .rangeRead)
            #expect(error.backend == .sqlite)
        }
    }
}
