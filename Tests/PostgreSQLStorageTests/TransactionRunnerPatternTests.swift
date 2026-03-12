import Testing
import Foundation
@testable import PostgreSQLStorage
@testable import StorageKit

/// Integration tests that mirror database-framework's TransactionRunner pattern.
///
/// These tests exercise the **exact code paths** that database-framework uses:
/// - `engine.createTransaction()` (not withTransaction)
/// - `ActiveTransactionScope.$current.withValue(transaction)` wrapping
/// - Explicit `transaction.commit()` and `transaction.cancel()`
/// - Subspace-based key organization (items / indexes / blobs)
/// - ItemEnvelope-style data storage
/// - Retry on `StorageError.isRetryable`
/// - `collectRange()` for index scans
///
/// Requires a running PostgreSQL instance. See PostgreSQLStorageTests for setup.
extension AllPostgreSQLTests {
@Suite("TransactionRunner Pattern Tests", .serialized)
struct TransactionRunnerPatternTests {

    // MARK: - Simulated Subspace Layout

    /// Simulates database-framework's subspace key organization.
    /// Uses 0xA0+ prefix range to avoid collision with PostgreSQLStorageTests (which uses 0x00-0x0F).
    private enum SubspacePrefix {
        /// Item records: [0xA0]/[typeName]/[id]
        static let items: UInt8 = 0xA0
        /// Index entries: [0xB0]/[indexName]/[values]/[id]
        static let indexes: UInt8 = 0xB0
        /// Blob chunks: [0xC0]/[itemKey]/[chunkIndex]
        static let blobs: UInt8 = 0xC0
        /// Schema metadata: [0xD0]/[typeName]
        static let schema: UInt8 = 0xD0
    }

    /// Build a key with subspace prefix, type name, and id.
    private func itemKey(type: String, id: String) -> Bytes {
        var key: Bytes = [SubspacePrefix.items]
        key.append(contentsOf: Array(type.utf8))
        key.append(0x00) // separator
        key.append(contentsOf: Array(id.utf8))
        return key
    }

    /// Build an index key: [prefix][indexName][0x00][value][0x00][id]
    private func indexKey(name: String, value: String, id: String) -> Bytes {
        var key: Bytes = [SubspacePrefix.indexes]
        key.append(contentsOf: Array(name.utf8))
        key.append(0x00)
        key.append(contentsOf: Array(value.utf8))
        key.append(0x00)
        key.append(contentsOf: Array(id.utf8))
        return key
    }

    /// Build subspace range: [prefix][name][0x00] to [prefix][name][0x01]
    private func subspaceRange(prefix: UInt8, name: String) -> (begin: Bytes, end: Bytes) {
        var begin: Bytes = [prefix]
        begin.append(contentsOf: Array(name.utf8))
        begin.append(0x00)
        var end: Bytes = [prefix]
        end.append(contentsOf: Array(name.utf8))
        end.append(0x01)
        return (begin, end)
    }

    /// Simulates ItemEnvelope serialization (magic header + protobuf payload).
    private func serializeItem(name: String, email: String) -> Bytes {
        // ItemEnvelope magic: ITEM (0x49 0x54 0x45 0x4D)
        var data: Bytes = [0x49, 0x54, 0x45, 0x4D]
        // Simplified payload: length-prefixed strings
        let nameBytes = Array(name.utf8)
        let emailBytes = Array(email.utf8)
        data.append(UInt8(nameBytes.count))
        data.append(contentsOf: nameBytes)
        data.append(UInt8(emailBytes.count))
        data.append(contentsOf: emailBytes)
        return data
    }

    /// Deserialize simulated ItemEnvelope.
    private func deserializeItem(_ data: Bytes) -> (name: String, email: String)? {
        guard data.count >= 4,
              data[0] == 0x49, data[1] == 0x54, data[2] == 0x45, data[3] == 0x4D else {
            return nil
        }
        var offset = 4
        guard offset < data.count else { return nil }
        let nameLen = Int(data[offset])
        offset += 1
        guard offset + nameLen <= data.count else { return nil }
        let name = String(bytes: data[offset..<offset+nameLen], encoding: .utf8) ?? ""
        offset += nameLen
        guard offset < data.count else { return nil }
        let emailLen = Int(data[offset])
        offset += 1
        guard offset + emailLen <= data.count else { return nil }
        let email = String(bytes: data[offset..<offset+emailLen], encoding: .utf8) ?? ""
        return (name, email)
    }

    private func makeEngine() async throws -> PostgreSQLStorageEngine {
        let engine = try await PostgreSQLTestHelper.makeEngine()

        // Clean all data — suites are serialized so no concurrent conflict
        try await engine.withTransaction { tx in
            tx.clearRange(beginKey: [0x00], endKey: [0xFF, 0xFF])
        }

        return engine
    }

    // =========================================================================
    // MARK: - TransactionRunner: createTransaction → commit pattern
    // =========================================================================

    /// Mirrors TransactionRunner.run() lines 77-99:
    /// `let transaction = try database.createTransaction()`
    /// `ActiveTransactionScope.$current.withValue(transaction) { ... }`
    /// `try await transaction.commit()`
    @Test func transactionRunner_createCommitPattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "user-001")
        let value = serializeItem(name: "Alice", email: "alice@example.com")

        // TransactionRunner pattern: create → scope → operation → commit
        let transaction = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(transaction) {
            transaction.setValue(value, for: key)
            try await transaction.commit()
        }

        // Verify with a separate read transaction
        let readTx = try engine.createTransaction()
        let result = try await ActiveTransactionScope.$current.withValue(readTx) {
            try await readTx.getValue(for: key)
        }
        try await readTx.commit()

        let item = try #require(result.flatMap { deserializeItem($0) })
        #expect(item.name == "Alice")
        #expect(item.email == "alice@example.com")
    }

    /// Mirrors TransactionRunner error path (lines 111-126):
    /// On error → `transaction.cancel()` → retry
    @Test func transactionRunner_cancelOnError() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "user-002")
        let value = serializeItem(name: "Bob", email: "bob@example.com")

        // Simulate failure: cancel discards writes
        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            tx1.setValue(value, for: key)
            // Simulate error → cancel
            tx1.cancel()
        }

        // Verify data was NOT persisted
        let tx2 = try engine.createTransaction()
        let result = try await ActiveTransactionScope.$current.withValue(tx2) {
            try await tx2.getValue(for: key)
        }
        try await tx2.commit()

        #expect(result == nil)
    }

    // =========================================================================
    // MARK: - ItemStorage: write + read pattern
    // =========================================================================

    /// Mirrors ItemStorage.write() + read() pattern.
    /// write: transaction.setValue(envelope.serialize(), for: key)
    /// read:  transaction.getValue(for: key, snapshot: snapshot)
    @Test func itemStorage_writeReadPattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let users: [(id: String, name: String, email: String)] = [
            ("u1", "Alice", "alice@test.com"),
            ("u2", "Bob", "bob@test.com"),
            ("u3", "Carol", "carol@test.com"),
        ]

        // Write phase (single transaction, batch)
        let writeTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(writeTx) {
            for user in users {
                let key = itemKey(type: "User", id: user.id)
                let data = serializeItem(name: user.name, email: user.email)
                writeTx.setValue(data, for: key)
            }
            try await writeTx.commit()
        }

        // Read phase (separate transaction)
        let readTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(readTx) {
            for user in users {
                let key = itemKey(type: "User", id: user.id)
                let data = try await readTx.getValue(for: key, snapshot: true)
                let item = try #require(data.flatMap { deserializeItem($0) })
                #expect(item.name == user.name)
                #expect(item.email == user.email)
            }
        }
        try await readTx.commit()
    }

    /// Mirrors ItemStorage.write() overwrite (upsert) behavior.
    /// Second setValue for the same key overwrites the value.
    @Test func itemStorage_overwritePattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "u-overwrite")

        // Initial write
        let tx1 = try engine.createTransaction()
        tx1.setValue(serializeItem(name: "Original", email: "old@test.com"), for: key)
        try await tx1.commit()

        // Overwrite (update) - same key, new value
        let tx2 = try engine.createTransaction()
        tx2.setValue(serializeItem(name: "Updated", email: "new@test.com"), for: key)
        try await tx2.commit()

        // Verify latest value
        let tx3 = try engine.createTransaction()
        let data = try await tx3.getValue(for: key)
        try await tx3.commit()

        let item = try #require(data.flatMap { deserializeItem($0) })
        #expect(item.name == "Updated")
        #expect(item.email == "new@test.com")
    }

    // =========================================================================
    // MARK: - Index Maintenance: diff-based update pattern
    // =========================================================================

    /// Mirrors TransactionContext.updateScalarIndexes() pattern:
    /// - Index entries use empty value `[]`
    /// - Set-based diff: clear removed keys, set new keys
    @Test func indexMaintenance_diffBasedUpdate() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let userId = "user-idx-001"
        let itemKeyBytes = itemKey(type: "User", id: userId)

        // Step 1: Insert with initial index values
        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            // Write item record
            tx1.setValue(serializeItem(name: "Alice", email: "alice@test.com"), for: itemKeyBytes)
            // Write index entry (email → id) with empty value
            let idxKey = indexKey(name: "user_email", value: "alice@test.com", id: userId)
            tx1.setValue([], for: idxKey) // Empty value = index marker
            try await tx1.commit()
        }

        // Step 2: Update — diff-based index maintenance
        // Old index key: email=alice@test.com, New: email=alice2@test.com
        let tx2 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx2) {
            // Read old value (for diff computation)
            let oldData = try await tx2.getValue(for: itemKeyBytes)
            #expect(oldData != nil) // Must exist

            // Write new item record
            tx2.setValue(serializeItem(name: "Alice", email: "alice2@test.com"), for: itemKeyBytes)

            // Compute diff: remove old index, add new index
            let oldIdxKey = indexKey(name: "user_email", value: "alice@test.com", id: userId)
            let newIdxKey = indexKey(name: "user_email", value: "alice2@test.com", id: userId)
            tx2.clear(key: oldIdxKey) // Remove old
            tx2.setValue([], for: newIdxKey) // Add new

            try await tx2.commit()
        }

        // Step 3: Verify — old index gone, new index present
        let tx3 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx3) {
            let oldIdxVal = try await tx3.getValue(for: indexKey(name: "user_email", value: "alice@test.com", id: userId))
            #expect(oldIdxVal == nil) // Removed

            let newIdxVal = try await tx3.getValue(for: indexKey(name: "user_email", value: "alice2@test.com", id: userId))
            #expect(newIdxVal != nil) // Present (empty value)
            #expect(newIdxVal == []) // Verify it's empty
        }
        try await tx3.commit()
    }

    // =========================================================================
    // MARK: - collectRange: index scan pattern
    // =========================================================================

    /// Mirrors FDBDataStore.scanIndex() pattern:
    /// collectRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end))
    @Test func collectRange_indexScanPattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Create multiple items with index entries
        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            for i in 0..<10 {
                let id = String(format: "user-%03d", i)
                let age = String(format: "%03d", 20 + i)

                let key = itemKey(type: "User", id: id)
                tx1.setValue(serializeItem(name: "User\(i)", email: "\(id)@test.com"), for: key)

                // Age index: ordered by age value
                let idxKey = indexKey(name: "user_age", value: age, id: id)
                tx1.setValue([], for: idxKey)
            }
            try await tx1.commit()
        }

        // Scan index range: age 023..027 (users aged 23-26)
        let tx2 = try engine.createTransaction()
        let results = try await ActiveTransactionScope.$current.withValue(tx2) {
            try await tx2.collectRange(
                from: .firstGreaterOrEqual(indexKey(name: "user_age", value: "023", id: "")),
                to: .firstGreaterOrEqual(indexKey(name: "user_age", value: "027", id: "")),
                snapshot: true
            )
        }
        try await tx2.commit()

        // Should find 4 entries (age 23, 24, 25, 26)
        #expect(results.count == 4)
    }

    /// Mirrors FDBDataStore count operation with collectRange.
    @Test func collectRange_countPattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            for i in 0..<25 {
                let key = itemKey(type: "Product", id: String(format: "p-%03d", i))
                tx1.setValue(serializeItem(name: "Product\(i)", email: ""), for: key)
            }
            try await tx1.commit()
        }

        // Count all products via collectRange
        let tx2 = try engine.createTransaction()
        let (begin, end) = subspaceRange(prefix: SubspacePrefix.items, name: "Product")
        let allItems = try await ActiveTransactionScope.$current.withValue(tx2) {
            try await tx2.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true,
                streamingMode: .wantAll
            )
        }
        try await tx2.commit()

        #expect(allItems.count == 25)
    }

    /// Mirrors FDBDataStore reverse scan with limit.
    @Test func collectRange_reverseScanWithLimit() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            for i in 0..<20 {
                let key = itemKey(type: "Event", id: String(format: "evt-%03d", i))
                tx1.setValue(Array("event-\(i)".utf8), for: key)
            }
            try await tx1.commit()
        }

        // Reverse scan with limit (latest 5 events)
        let tx2 = try engine.createTransaction()
        let (begin, end) = subspaceRange(prefix: SubspacePrefix.items, name: "Event")
        let latest = try await ActiveTransactionScope.$current.withValue(tx2) {
            try await tx2.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 5,
                reverse: true,
                snapshot: true
            )
        }
        try await tx2.commit()

        #expect(latest.count == 5)
        // Reverse order: evt-019, evt-018, evt-017, evt-016, evt-015
        // First result should contain "event-19"
        let firstValue = String(bytes: latest[0].1, encoding: .utf8)
        #expect(firstValue == "event-19")
    }

    // =========================================================================
    // MARK: - Bulk deletion: clearRange pattern
    // =========================================================================

    /// Mirrors FDBDataStore.clearAll() pattern:
    /// Clear all items and indexes of a type using subspace range.
    @Test func bulkDeletion_clearAllTypeData() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Write items + indexes for two types
        let tx1 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx1) {
            // Type A: 5 items
            for i in 0..<5 {
                let key = itemKey(type: "TypeA", id: "a-\(i)")
                tx1.setValue(Array("value-a-\(i)".utf8), for: key)
                tx1.setValue([], for: indexKey(name: "typeA_idx", value: "v\(i)", id: "a-\(i)"))
            }
            // Type B: 3 items
            for i in 0..<3 {
                let key = itemKey(type: "TypeB", id: "b-\(i)")
                tx1.setValue(Array("value-b-\(i)".utf8), for: key)
            }
            try await tx1.commit()
        }

        // Clear all TypeA items and indexes (like clearAll())
        let tx2 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx2) {
            let (itemBegin, itemEnd) = subspaceRange(prefix: SubspacePrefix.items, name: "TypeA")
            tx2.clearRange(beginKey: itemBegin, endKey: itemEnd)

            let (idxBegin, idxEnd) = subspaceRange(prefix: SubspacePrefix.indexes, name: "typeA_idx")
            tx2.clearRange(beginKey: idxBegin, endKey: idxEnd)

            try await tx2.commit()
        }

        // Verify: TypeA gone, TypeB intact
        let tx3 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx3) {
            let (aBegin, aEnd) = subspaceRange(prefix: SubspacePrefix.items, name: "TypeA")
            let typeAItems = try await tx3.collectRange(
                from: .firstGreaterOrEqual(aBegin),
                to: .firstGreaterOrEqual(aEnd)
            )
            #expect(typeAItems.isEmpty)

            let (bBegin, bEnd) = subspaceRange(prefix: SubspacePrefix.items, name: "TypeB")
            let typeBItems = try await tx3.collectRange(
                from: .firstGreaterOrEqual(bBegin),
                to: .firstGreaterOrEqual(bEnd)
            )
            #expect(typeBItems.count == 3)
        }
        try await tx3.commit()
    }

    /// Mirrors ItemStorage.delete() pattern:
    /// clearAllBlobs(for: key) + clear(key: key)
    @Test func itemStorage_deletePattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "user-delete")

        // Write
        let tx1 = try engine.createTransaction()
        tx1.setValue(serializeItem(name: "ToDelete", email: "del@test.com"), for: key)
        // Simulate blob chunks
        let blobKey1: Bytes = [SubspacePrefix.blobs] + key + [0x00, 0x00]
        let blobKey2: Bytes = [SubspacePrefix.blobs] + key + [0x00, 0x01]
        tx1.setValue(Array(repeating: 0xAA, count: 100), for: blobKey1)
        tx1.setValue(Array(repeating: 0xBB, count: 100), for: blobKey2)
        try await tx1.commit()

        // Delete: clear blobs range + clear item
        let tx2 = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx2) {
            // Clear blob range
            let blobRangeBegin: Bytes = [SubspacePrefix.blobs] + key + [0x00]
            let blobRangeEnd: Bytes = [SubspacePrefix.blobs] + key + [0x01]
            tx2.clearRange(beginKey: blobRangeBegin, endKey: blobRangeEnd)
            // Clear item
            tx2.clear(key: key)
            try await tx2.commit()
        }

        // Verify all gone
        let tx3 = try engine.createTransaction()
        let itemResult = try await tx3.getValue(for: key)
        let blob1Result = try await tx3.getValue(for: blobKey1)
        let blob2Result = try await tx3.getValue(for: blobKey2)
        try await tx3.commit()

        #expect(itemResult == nil)
        #expect(blob1Result == nil)
        #expect(blob2Result == nil)
    }

    // =========================================================================
    // MARK: - Nested Transaction (ActiveTransactionScope)
    // =========================================================================

    /// Mirrors nested transaction detection in TransactionRunner + FDBDataStore.
    /// When ActiveTransactionScope.current is set, createTransaction() returns
    /// a nested transaction reusing the parent connection.
    @Test func nestedTransaction_reuseParentConnection() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let outerKey = itemKey(type: "Outer", id: "o-1")
        let innerKey = itemKey(type: "Inner", id: "i-1")

        // Outer transaction (like TransactionRunner)
        let outerTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(outerTx) {
            outerTx.setValue(Array("outer-data".utf8), for: outerKey)

            // Inner transaction (like FDBDataStore calling createTransaction inside operation)
            let innerTx = try engine.createTransaction()
            // Should be nested — reuses outer connection
            innerTx.setValue(Array("inner-data".utf8), for: innerKey)
            try await innerTx.commit() // Flushes buffer only (nested)

            try await outerTx.commit() // Actual COMMIT
        }

        // Both keys should be persisted
        let readTx = try engine.createTransaction()
        let outerResult = try await readTx.getValue(for: outerKey)
        let innerResult = try await readTx.getValue(for: innerKey)
        try await readTx.commit()

        #expect(outerResult != nil)
        #expect(innerResult != nil)
        #expect(String(bytes: outerResult!, encoding: .utf8) == "outer-data")
        #expect(String(bytes: innerResult!, encoding: .utf8) == "inner-data")
    }

    /// Mirrors nested transaction cancel: inner cancel discards inner writes only.
    @Test func nestedTransaction_innerCancelDoesNotAffectOuter() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let outerKey = itemKey(type: "Outer", id: "o-2")
        let innerKey = itemKey(type: "Inner", id: "i-2")

        let outerTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(outerTx) {
            outerTx.setValue(Array("outer-kept".utf8), for: outerKey)

            let innerTx = try engine.createTransaction()
            innerTx.setValue(Array("inner-discarded".utf8), for: innerKey)
            innerTx.cancel() // Discard inner writes only

            try await outerTx.commit()
        }

        let readTx = try engine.createTransaction()
        let outerResult = try await readTx.getValue(for: outerKey)
        let innerResult = try await readTx.getValue(for: innerKey)
        try await readTx.commit()

        #expect(outerResult != nil) // Outer committed
        #expect(innerResult == nil) // Inner was cancelled
    }

    // =========================================================================
    // MARK: - TransactionRunner Retry Pattern
    // =========================================================================

    /// Mirrors TransactionRunner retry loop:
    /// for attempt in 0..<maxRetries {
    ///     let transaction = try database.createTransaction()
    ///     ... operation ...
    ///     try await transaction.commit()
    ///     // on retryable error: transaction.cancel(); continue
    /// }
    @Test func transactionRunner_retryLoop() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        var attempts = 0
        let maxRetries = 3
        let key = itemKey(type: "Retry", id: "r-1")

        // Simulate retry loop (fail twice, succeed on third)
        for attempt in 0..<maxRetries {
            let tx = try engine.createTransaction()
            do {
                try await ActiveTransactionScope.$current.withValue(tx) {
                    tx.setValue(Array("attempt-\(attempt)".utf8), for: key)

                    if attempt < 2 {
                        // Simulate retryable failure
                        throw StorageError.transactionConflict
                    }

                    try await tx.commit()
                }
                attempts = attempt + 1
                break
            } catch let error as StorageError where error.isRetryable {
                tx.cancel()
                continue
            }
        }

        #expect(attempts == 3) // Succeeded on third attempt

        let readTx = try engine.createTransaction()
        let result = try await readTx.getValue(for: key)
        try await readTx.commit()

        #expect(String(bytes: result!, encoding: .utf8) == "attempt-2")
    }

    // =========================================================================
    // MARK: - Read-your-writes within createTransaction
    // =========================================================================

    /// Mirrors FDBDataStore pattern: write then read within the same transaction.
    /// ItemStorage.write(data, for: key) then ItemStorage.read(for: key)
    @Test func readYourWrites_withinTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "ryw-1")
        let value = serializeItem(name: "ReadYourWrites", email: "ryw@test.com")

        let tx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx) {
            // Write (buffered)
            tx.setValue(value, for: key)

            // Read before commit — should see buffered write
            let result = try await tx.getValue(for: key)
            let item = try #require(result.flatMap { deserializeItem($0) })
            #expect(item.name == "ReadYourWrites")

            try await tx.commit()
        }
    }

    /// Mirrors overwrite-then-read pattern.
    /// FDBDataStore reads old value, writes new value, then may read again.
    @Test func readYourWrites_overwriteWithinTransaction() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let key = itemKey(type: "User", id: "ryw-2")

        // Pre-populate
        let setup = try engine.createTransaction()
        setup.setValue(serializeItem(name: "Original", email: "orig@test.com"), for: key)
        try await setup.commit()

        // Overwrite + read within same transaction
        let tx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx) {
            // Read original
            let oldData = try await tx.getValue(for: key)
            let oldItem = try #require(oldData.flatMap { deserializeItem($0) })
            #expect(oldItem.name == "Original")

            // Overwrite
            tx.setValue(serializeItem(name: "Modified", email: "mod@test.com"), for: key)

            // Read again — should see new value from write buffer
            let newData = try await tx.getValue(for: key)
            let newItem = try #require(newData.flatMap { deserializeItem($0) })
            #expect(newItem.name == "Modified")

            try await tx.commit()
        }
    }

    // =========================================================================
    // MARK: - Atomic Operations (.add for counters)
    // =========================================================================

    /// Mirrors AggregationIndex atomicOp(.add) usage for counters.
    @Test func atomicAdd_counterPattern() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let counterKey: Bytes = [SubspacePrefix.indexes, 0x10, 0x01] // Aggregation counter key

        // Initialize counter to 0
        let tx1 = try engine.createTransaction()
        var buf = [UInt8](repeating: 0, count: 8)
        withUnsafeBytes(of: Int64(0).littleEndian) { ptr in
            buf = Array(ptr)
        }
        tx1.setValue(buf, for: counterKey)
        try await tx1.commit()

        // Increment by 5 using atomicOp(.add)
        let tx2 = try engine.createTransaction()
        var addend = [UInt8](repeating: 0, count: 8)
        withUnsafeBytes(of: Int64(5).littleEndian) { ptr in
            addend = Array(ptr)
        }
        tx2.atomicOp(key: counterKey, param: addend, mutationType: .add)
        try await tx2.commit()

        // Increment by 3 more
        let tx3 = try engine.createTransaction()
        withUnsafeBytes(of: Int64(3).littleEndian) { ptr in
            addend = Array(ptr)
        }
        tx3.atomicOp(key: counterKey, param: addend, mutationType: .add)
        try await tx3.commit()

        // Read final value: should be 8
        let tx4 = try engine.createTransaction()
        let result = try await tx4.getValue(for: counterKey)
        try await tx4.commit()

        let finalValue = try #require(result)
        #expect(finalValue.count == 8)
        let value = finalValue.withUnsafeBytes { ptr in
            ptr.loadUnaligned(as: Int64.self)
        }
        #expect(Int64(littleEndian: value) == 8)
    }

    // =========================================================================
    // MARK: - Full CRUD lifecycle (insert → read → update → delete)
    // =========================================================================

    /// Mirrors a complete database-framework lifecycle:
    /// context.insert(item) → context.save() (write item + indexes)
    /// context.fetch() (read via index scan)
    /// context.insert(updated) → context.save() (update item + diff indexes)
    /// context.delete(item) → context.save() (delete item + clear indexes)
    @Test func fullCRUDLifecycle() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let userId = "lifecycle-001"
        let itemKeyBytes = itemKey(type: "User", id: userId)

        // ---- INSERT ----
        let insertTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(insertTx) {
            // Write item
            insertTx.setValue(serializeItem(name: "Alice", email: "alice@test.com"), for: itemKeyBytes)
            // Write index (name)
            insertTx.setValue([], for: indexKey(name: "user_name", value: "Alice", id: userId))
            // Write index (email)
            insertTx.setValue([], for: indexKey(name: "user_email", value: "alice@test.com", id: userId))
            try await insertTx.commit()
        }

        // ---- READ ----
        let readTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(readTx) {
            let data = try await readTx.getValue(for: itemKeyBytes, snapshot: true)
            let item = try #require(data.flatMap { deserializeItem($0) })
            #expect(item.name == "Alice")

            // Verify indexes exist
            let nameIdx = try await readTx.getValue(
                for: indexKey(name: "user_name", value: "Alice", id: userId))
            #expect(nameIdx != nil)
        }
        try await readTx.commit()

        // ---- UPDATE (diff-based) ----
        let updateTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(updateTx) {
            // Write updated item
            updateTx.setValue(serializeItem(name: "Alice Smith", email: "alice@test.com"), for: itemKeyBytes)
            // Diff: remove old name index, add new
            updateTx.clear(key: indexKey(name: "user_name", value: "Alice", id: userId))
            updateTx.setValue([], for: indexKey(name: "user_name", value: "Alice Smith", id: userId))
            // email index unchanged — no action needed
            try await updateTx.commit()
        }

        // ---- DELETE ----
        let deleteTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(deleteTx) {
            // Clear item
            deleteTx.clear(key: itemKeyBytes)
            // Clear all indexes for this item
            deleteTx.clear(key: indexKey(name: "user_name", value: "Alice Smith", id: userId))
            deleteTx.clear(key: indexKey(name: "user_email", value: "alice@test.com", id: userId))
            try await deleteTx.commit()
        }

        // ---- VERIFY DELETION ----
        let verifyTx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(verifyTx) {
            let item = try await verifyTx.getValue(for: itemKeyBytes)
            #expect(item == nil)

            let nameIdx = try await verifyTx.getValue(
                for: indexKey(name: "user_name", value: "Alice Smith", id: userId))
            #expect(nameIdx == nil)

            let emailIdx = try await verifyTx.getValue(
                for: indexKey(name: "user_email", value: "alice@test.com", id: userId))
            #expect(emailIdx == nil)
        }
        try await verifyTx.commit()
    }

    // =========================================================================
    // MARK: - Lazy connection: read-only transaction (no writes)
    // =========================================================================

    /// Verifies that createTransaction with no writes commits cleanly
    /// (no connection acquired = no-op commit).
    @Test func lazyConnection_readOnlyNoOp() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // Pre-populate
        let setup = try engine.createTransaction()
        setup.setValue(Array("data".utf8), for: itemKey(type: "LazyTest", id: "lt-1"))
        try await setup.commit()

        // Read-only transaction that touches DB via getValue
        let tx = try engine.createTransaction()
        let result = try await tx.getValue(for: itemKey(type: "LazyTest", id: "lt-1"))
        try await tx.commit()

        #expect(result != nil)
    }

    /// Verifies that createTransaction with no operations at all commits as no-op.
    @Test func lazyConnection_emptyCommit() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        // No reads, no writes — should be a pure no-op
        let tx = try engine.createTransaction()
        try await tx.commit()
        // No crash = success
    }

    // =========================================================================
    // MARK: - Multi-type subspace isolation
    // =========================================================================

    /// Mirrors database-framework's multi-type storage: each Persistable type
    /// has its own subspace. Operations on one type do not affect another.
    @Test func multiType_subspaceIsolation() async throws {
        let engine = try await makeEngine()
        defer { engine.shutdown() }

        let tx = try engine.createTransaction()
        try await ActiveTransactionScope.$current.withValue(tx) {
            // Write User items
            for i in 0..<3 {
                tx.setValue(Array("user-\(i)".utf8), for: itemKey(type: "User", id: "u-\(i)"))
            }
            // Write Order items
            for i in 0..<5 {
                tx.setValue(Array("order-\(i)".utf8), for: itemKey(type: "Order", id: "o-\(i)"))
            }
            try await tx.commit()
        }

        // Scan only Users
        let scanTx = try engine.createTransaction()
        let (userBegin, userEnd) = subspaceRange(prefix: SubspacePrefix.items, name: "User")
        let users = try await scanTx.collectRange(
            from: .firstGreaterOrEqual(userBegin),
            to: .firstGreaterOrEqual(userEnd)
        )
        try await scanTx.commit()

        #expect(users.count == 3)

        // Scan only Orders
        let scanTx2 = try engine.createTransaction()
        let (orderBegin, orderEnd) = subspaceRange(prefix: SubspacePrefix.items, name: "Order")
        let orders = try await scanTx2.collectRange(
            from: .firstGreaterOrEqual(orderBegin),
            to: .firstGreaterOrEqual(orderEnd)
        )
        try await scanTx2.commit()

        #expect(orders.count == 5)
    }
}
} // extension AllPostgreSQLTests
