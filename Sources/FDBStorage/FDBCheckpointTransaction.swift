import DatabaseTypes
import FoundationDB
import StorageKit
import Synchronization

/// Projects the native Directory transaction onto the transaction-owned journal.
final class FDBCheckpointTransaction: TransactionProtocol, Sendable {
    let native: any TransactionProtocol
    let journal: TransactionJournal
    private let systemKeyAccess: Mutex<Bool>
    var raw: FDBJournalBackendAccess {
        FDBJournalBackendAccess(native: native, includesSystemKeys: systemKeyAccess.withLock { $0 })
    }
    private let pageSize: Int

    init(native: any TransactionProtocol, journal: TransactionJournal, limits: TransactionCheckpointLimits, includesSystemKeys: Bool) {
        self.native = native
        self.journal = journal
        self.systemKeyAccess = Mutex(includesSystemKeys)
        self.pageSize = limits.maximumReadPageRows
    }

    func getValue<Key: FDB.ByteInput>(for key: Key, snapshot: Bool) async throws -> ByteString? {
        try await journal.getValue(for: retained(key), snapshot: snapshot, maximumByteCount: nil, using: raw)
    }

    func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(_ value: Value, for key: Key) throws {
        try append(key, value) { .set(key: $0, value: $1, suppressWriteConflict: false) }
    }

    func clear<Key: FDB.ByteInput>(key: Key) throws {
        try append(key) { .clear(key: $0, suppressWriteConflict: false) }
    }

    func clearRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(beginKey: Begin, endKey: End) throws {
        try append(beginKey, endKey) { .clearRange(begin: $0, end: $1, suppressWriteConflict: false) }
    }

    func atomicOp<Key: FDB.ByteInput, Parameter: FDB.ByteInput>(key: Key, param: Parameter, mutationType: FDB.MutationType) throws {
        let type: StorageKit.MutationType
        switch mutationType {
        case .add: type = .add
        case .bitAnd: type = .bitAnd
        case .bitOr: type = .bitOr
        case .bitXor: type = .bitXor
        case .min: type = .min
        case .max: type = .max
        case .compareAndClear: type = .compareAndClear
        case .setVersionstampedKey: type = .setVersionstampedKey
        case .setVersionstampedValue: type = .setVersionstampedValue
        default:
            throw StorageError.unsupportedOperation("Atomic mutation has no portable checkpoint algebra", operation: .write, backend: .foundationDB)
        }
        try append(key, param) { .atomic(key: $0, parameter: $1, type: type, suppressWriteConflict: false) }
    }

    func getKey(selector: FDB.KeySelector, snapshot: Bool) async throws -> ByteString {
        let key = try await journal.getKey(selector: storageSelector(selector), snapshot: snapshot, using: raw)
        return key ?? (selector.offset <= 0 ? ByteString() : ByteString([0xff]))
    }

    func readRangeBatch(from begin: FDB.KeySelector, to end: FDB.KeySelector, limit: Int, targetBytes: Int, streamingMode: FDB.StreamingMode, iteration: Int, reverse: Bool, snapshot: Bool) async throws -> RangeBatch {
        guard limit >= 0, targetBytes >= 0, iteration > 0 else {
            throw StorageError(code: .invalidOperation, operation: .rangeRead, backend: .foundationDB, message: "Invalid FoundationDB checkpoint range parameters")
        }
        let count = limit == 0 ? pageSize : min(limit, pageSize)
        var cursor = journal.rangeCursor(from: storageSelector(begin), to: storageSelector(end), limit: 0, reverse: reverse, snapshot: snapshot, streamingMode: .iterator, using: raw)
        var records: [FDB.KeyValue] = []
        records.reserveCapacity(count)
        var bytes = 0
        do {
            while records.count < count, let row = try await cursor.next() {
                records.append(FDB.KeyValue(key: row.0, value: row.1))
                let next = bytes.addingReportingOverflow(row.0.count)
                let total = next.partialValue.addingReportingOverflow(row.1.count)
                guard !next.overflow, !total.overflow else {
                    throw StorageError(code: .backendContractViolation, operation: .rangeRead, backend: .foundationDB, message: "Range byte count overflow")
                }
                bytes = total.partialValue
                if targetBytes > 0, bytes >= targetBytes { break }
            }
            // A full page may require one final empty native-style page. The
            // caller resumes by the last returned key, so no row is discarded.
            let hasMore = records.count == count || (targetBytes > 0 && bytes >= targetBytes)
            try await cursor.finish()
            return RangeBatch(records: records, hasMore: hasMore)
        } catch {
            let operationError = error
            do { try await cursor.finish() }
            catch { throw StorageRangeCleanupError(iterationError: operationError, cleanupError: error) }
            throw operationError
        }
    }

    func replay() async throws {
        for mutation in try await journal.sealForCommit() {
            switch mutation {
            case .set(let key, let value, let suppress):
                if suppress { try native.setNextWriteNoWriteConflictRange() }
                try native.setValue(value, for: key)
            case .clear(let key, let suppress):
                if suppress { try native.setNextWriteNoWriteConflictRange() }
                try native.clear(key: key)
            case .clearRange(let begin, let end, let suppress):
                if suppress { try native.setNextWriteNoWriteConflictRange() }
                try native.clearRange(beginKey: begin, endKey: end)
            case .atomic(let key, let parameter, let type, let suppress):
                if suppress { try native.setNextWriteNoWriteConflictRange() }
                try native.atomicOp(key: key, param: parameter, mutationType: nativeMutation(type))
            case .writeConflict(let begin, let end):
                try native.addConflictRange(beginKey: begin, endKey: end, type: .write)
            }
        }
    }

    func addConflictRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(beginKey: Begin, endKey: End, type: FDB.ConflictRangeType) throws {
        if type == .write {
            try append(beginKey, endKey) { .writeConflict(begin: $0, end: $1) }
        } else {
            try journal.performSynchronousOperation {
                try native.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
            }
        }
    }

    func setOption<Value: FDB.ByteInput>(to value: Value, forOption option: FDB.TransactionOption) throws {
        guard option != .nextWriteNoWriteConflictRange else {
            throw StorageError(code: .invalidOperation, operation: .execute, backend: .foundationDB, message: "Write-conflict suppression takes no value")
        }
        try journal.performSynchronousOperation {
            try native.setOption(to: value, forOption: option)
            if option == .accessSystemKeys || option == .readSystemKeys { systemKeyAccess.withLock { $0 = true } }
        }
    }

    func setOption(forOption option: FDB.TransactionOption) throws {
        if option == .nextWriteNoWriteConflictRange { try journal.suppressNextWriteConflict() }
        else {
            try journal.performSynchronousOperation {
                try native.setOption(forOption: option)
                if option == .accessSystemKeys || option == .readSystemKeys { systemKeyAccess.withLock { $0 = true } }
            }
        }
    }

    func commit() async throws { try await native.commit() }
    func cancel() { native.cancel() }
    func requestVersionstamp() -> any FDB.PendingTransactionVersionstamp { native.requestVersionstamp() }
    func setReadVersion(_ version: FDB.Version) { native.setReadVersion(version) }
    func getReadVersion() async throws -> FDB.Version { try await native.getReadVersion() }
    func getCommittedVersion() throws -> FDB.Version { try native.getCommittedVersion() }
    func approximateSize() async throws -> Int64 { try await native.approximateSize() }

    func onError(_ error: FDBError) async throws {
        throw StorageError.unsupportedOperation("Checkpoint retries require a fresh transaction attempt", operation: .execute, backend: .foundationDB)
    }

    func getEstimatedRangeSizeBytes<Begin: FDB.ByteInput, End: FDB.ByteInput>(beginKey: Begin, endKey: End) async throws -> Int64 {
        let value = try await StorageRangeMetrics.exactSize(journal.rangeCursor(from: .firstGreaterOrEqual(retained(beginKey)), to: .firstGreaterOrEqual(retained(endKey)), limit: 0, reverse: false, snapshot: true, streamingMode: .wantAll, using: raw))
        return Int64(value)
    }

    func getRangeSplitPoints<Begin: FDB.ByteInput, End: FDB.ByteInput>(beginKey: Begin, endKey: End, chunkSize: Int64) async throws -> [ByteString] {
        guard let chunk = Int(exactly: chunkSize) else {
            throw StorageError(code: .invalidOperation, operation: .rangeRead, backend: .foundationDB, message: "Split size is out of range")
        }
        return try await StorageRangeMetrics.splitPoints(beginKey: retained(beginKey), endKey: retained(endKey), chunkSize: chunk, maximumPointCount: StorageRangeMetrics.defaultMaximumSplitPointCount, source: journal.rangeCursor(from: .firstGreaterOrEqual(retained(beginKey)), to: .firstGreaterOrEqual(retained(endKey)), limit: 0, reverse: false, snapshot: true, streamingMode: .wantAll, using: raw))
    }

    private func append<First: FDB.ByteInput, Second: FDB.ByteInput>(
        _ first: First, _ second: Second,
        _ mutation: (ByteString, ByteString) -> TransactionJournalMutation
    ) throws {
        try first.withUnsafeBytes { firstBytes in
            try second.withUnsafeBytes { secondBytes in
                try validate(firstBytes)
                try validate(secondBytes)
                let firstCount = first is ByteString ? 0 : firstBytes.count
                let secondCount = second is ByteString ? 0 : secondBytes.count
                let count = firstCount.addingReportingOverflow(secondCount)
                guard !count.overflow else { throw TransactionCheckpointError.invalidLimits }
                try journal.append(copyingByteCount: count.partialValue) {
                    mutation(owned(first, bytes: firstBytes), owned(second, bytes: secondBytes))
                }
            }
        }
    }

    private func append<Source: FDB.ByteInput>(
        _ source: Source, _ mutation: (ByteString) -> TransactionJournalMutation
    ) throws {
        try source.withUnsafeBytes { bytes in
            try validate(bytes)
            try journal.append(copyingByteCount: source is ByteString ? 0 : bytes.count) {
                mutation(owned(source, bytes: bytes))
            }
        }
    }

    private func validate(_ bytes: UnsafeRawBufferPointer) throws {
        guard Int32(exactly: bytes.count) != nil else { throw FDB.InputError.byteCountOutOfRange(bytes.count) }
        guard bytes.isEmpty || bytes.baseAddress != nil else { throw FDB.InputError.missingByteAddress(count: bytes.count) }
    }

    private func owned<Source: FDB.ByteInput>(_ source: Source, bytes: UnsafeRawBufferPointer) -> ByteString {
        if let bytes = source as? ByteString { return bytes }
        // The journal reserves the transfer before this generic input copy.
        return ByteString.copying(count: bytes.count) { $0.copyMemory(from: bytes) }
    }

    private func retained<Source: FDB.ByteInput>(_ source: Source) -> ByteString {
        if let bytes = source as? ByteString { return bytes }
        // Generic bindings inputs may borrow mutable arrays or stack storage;
        // this is their single immutable ownership transfer into the journal.
        return source.withUnsafeBytes { bytes in
            ByteString.copying(count: bytes.count) { $0.copyMemory(from: bytes) }
        }
    }

    private func storageSelector(_ value: FDB.KeySelector) -> StorageKit.KeySelector {
        StorageKit.KeySelector(key: value.key, orEqual: value.orEqual, offset: value.offset)
    }

    private func nativeMutation(_ value: StorageKit.MutationType) -> FDB.MutationType {
        switch value {
        case .add: .add
        case .bitAnd: .bitAnd
        case .bitOr: .bitOr
        case .bitXor: .bitXor
        case .min: .min
        case .max: .max
        case .compareAndClear: .compareAndClear
        case .setVersionstampedKey: .setVersionstampedKey
        case .setVersionstampedValue: .setVersionstampedValue
        }
    }
}
