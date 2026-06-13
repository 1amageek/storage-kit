import StorageKit
import Foundation
import Synchronization

/// StorageKit.Transaction implementation for SQLite.
///
/// Write operations (`setValue`/`clear`/`clearRange`) are non-throwing per the protocol,
/// so writes are buffered and flushed on commit.
/// `getValue` replays the visible buffer in order to provide read-your-writes semantics.
/// Top-level `getRange` flushes the buffer before executing the SQL query.
///
/// ## Nested Transaction Support
///
/// Nested children created by `SQLiteStorageEngine.createTransaction()` keep
/// their own write buffer. `commit()` validates and merges that buffer into the
/// parent; `cancel()` discards it. Nested range reads materialize the current
/// database contents plus visible parent/child buffers without writing child
/// data into SQLite, so child rollback cannot affect parent writes.
public final class SQLiteStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = KeyValueRangeResult

    private let connection: SQLiteConnectionHandle
    private let lock: NSLock?
    private let parent: SQLiteStorageTransaction?

    private struct MutableState: Sendable {
        var writeBuffer: [WriteOp] = []
        var lifecycle: Lifecycle = .open
        var lockReleased: Bool
    }

    private enum Lifecycle: Sendable {
        case open
        case committing
        case committed
        case cancelled
        case failed(StorageError)
    }
    private let _state: Mutex<MutableState>

    private enum WriteOp: Sendable {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
        case atomic(key: Bytes, param: Bytes, mutationType: MutationType)
    }

    init(connection: SQLiteConnectionHandle, lock: NSLock?) {
        self.connection = connection
        self.lock = lock
        self.parent = nil
        self._state = Mutex(MutableState(lockReleased: lock == nil))
    }

    init(parent: SQLiteStorageTransaction) {
        self.connection = parent.connection
        self.lock = nil
        self.parent = parent
        self._state = Mutex(MutableState(lockReleased: true))
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        let writeBuffer = try visibleWriteBuffer()
        var value = try connection.get(key: key)
        for op in writeBuffer {
            switch op {
            case .set(let k, let v) where k == key:
                value = v
            case .clear(let k) where k == key:
                value = nil
            case .clearRange(let b, let e)
                where compareBytes(key, b) >= 0 && compareBytes(key, e) < 0:
                value = nil
            case .atomic(let k, let param, let mutationType) where k == key:
                switch try mutationType.apply(to: value, param: param) {
                case .set(let bytes):
                    value = bytes
                case .clear:
                    value = nil
                case .unchanged:
                    break
                }
            default:
                continue
            }
        }
        return value
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueRangeResult {
        do {
            try _state.withLock { state in
                try Self.validateReadable(state.lifecycle)
            }
        } catch {
            return KeyValueRangeResult(error: error)
        }

        // Resolve KeySelectors to SQL boundary conditions (see SQLRangeBoundary
        // for the full FDB-semantics mapping, including the lastLess* selectors
        // that require scalar-subquery resolution).
        do {
            if parent == nil {
                let beginBoundary = try SQLRangeBoundary.begin(begin)
                let endBoundary = try SQLRangeBoundary.end(end)
                try flushWriteBuffer()
                let results = try connection.getRange(
                    begin: beginBoundary,
                    end: endBoundary,
                    limit: limit, reverse: reverse
                )
                return KeyValueRangeResult(results)
            }

            let results = try nestedRangeResults(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse
            )
            return KeyValueRangeResult(results)
        } catch {
            return KeyValueRangeResult(error: error)
        }
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        _state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) {
        _state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        _state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    // MARK: - Atomic Operations

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        _state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    // MARK: - Transaction Management

    public func commit() async throws {
        let start = _state.withLock { state -> CommitStart in
            switch state.lifecycle {
            case .open:
                state.lifecycle = .committing
                return .proceed
            case .committing:
                return .throw(Self.invalidStateError("Transaction commit already in progress"))
            case .committed:
                return .alreadyCommitted
            case .cancelled:
                return .throw(Self.invalidStateError("Transaction cancelled"))
            case .failed(let error):
                return .throw(error)
            }
        }

        switch start {
        case .proceed:
            break
        case .alreadyCommitted:
            return
        case .throw(let error):
            cleanupFailedTransactionIfNeeded()
            throw error
        }

        if parent != nil {
            try commitNested()
        } else if lock != nil {
            // Top-level transaction: flush buffer, COMMIT, release lock
            defer { releaseLockOnce() }
            do {
                try flushWriteBuffer()
                try connection.execute("COMMIT", operation: .commit)
                _state.withLock { $0.lifecycle = .committed }
            } catch let originalError {
                let error = markFailed(originalError, operation: .commit)
                do {
                    try connection.execute("ROLLBACK", operation: .rollback)
                } catch {
                    // Preserve the original commit/write error. Rollback failure is
                    // secondary because the transaction lock is still released below.
                }
                throw error
            }
        } else {
            _state.withLock { $0.lifecycle = .committed }
        }
    }

    public func cancel() {
        enum Action {
            case none
            case rollbackAndRelease
        }

        let action = _state.withLock { state -> Action in
            switch state.lifecycle {
            case .committed, .cancelled:
                return .none
            case .open, .committing:
                state.lifecycle = .cancelled
                state.writeBuffer.removeAll()
            case .failed:
                state.writeBuffer.removeAll()
            }
            if parent != nil {
                return .none
            }
            return state.lockReleased ? .none : .rollbackAndRelease
        }

        switch action {
        case .none:
            return
        case .rollbackAndRelease:
            rollbackBestEffort()
            releaseLockOnce()
        }
        // Nested transaction: just discard buffer (parent controls ROLLBACK)
    }

    // MARK: - Internal

    private enum CommitStart: Sendable {
        case proceed
        case alreadyCommitted
        case `throw`(StorageError)
    }

    private func visibleWriteBuffer() throws -> [WriteOp] {
        let inherited = try parent?.visibleWriteBuffer() ?? []
        let local = try _state.withLock { state in
            try Self.validateReadable(state.lifecycle)
            return state.writeBuffer
        }
        return inherited + local
    }

    private func commitNested() throws {
        do {
            let ops = try _state.withLock { state -> [WriteOp] in
                switch state.lifecycle {
                case .committing:
                    return state.writeBuffer
                case .open:
                    return state.writeBuffer
                case .committed:
                    return []
                case .cancelled:
                    throw Self.invalidStateError("Transaction cancelled")
                case .failed(let error):
                    throw error
                }
            }
            try Self.validateMergeable(ops)
            try parent?.appendBufferedWrites(ops)
            _state.withLock { state in
                state.writeBuffer.removeAll()
                state.lifecycle = .committed
            }
        } catch {
            throw markFailed(error, operation: .commit)
        }
    }

    private func appendBufferedWrites(_ ops: [WriteOp]) throws {
        guard !ops.isEmpty else { return }
        try _state.withLock { state in
            switch state.lifecycle {
            case .open, .committing:
                state.writeBuffer.append(contentsOf: ops)
            case .committed:
                throw Self.invalidStateError("Transaction committed")
            case .cancelled:
                throw Self.invalidStateError("Transaction cancelled")
            case .failed(let error):
                throw error
            }
        }
    }

    private static func validateMergeable(_ ops: [WriteOp]) throws {
        for op in ops {
            guard case .atomic(_, let param, let mutationType) = op else {
                continue
            }
            _ = try mutationType.apply(to: nil, param: param)
        }
    }

    private func nestedRangeResults(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool
    ) throws -> [(key: Bytes, value: Bytes)] {
        _ = try SQLRangeBoundary.begin(begin)
        _ = try SQLRangeBoundary.end(end)
        let visibleBuffer = try visibleWriteBuffer()
        let entries = try connection.getAllEntries()
        return try Self.materializeRange(
            entries: entries,
            applying: visibleBuffer,
            begin: begin,
            end: end,
            limit: limit,
            reverse: reverse
        )
    }

    private static func materializeRange(
        entries: [(key: Bytes, value: Bytes)],
        applying writeBuffer: [WriteOp],
        begin: KeySelector,
        end: KeySelector,
        limit: Int,
        reverse: Bool
    ) throws -> [(key: Bytes, value: Bytes)] {
        var values: [Bytes: Bytes] = [:]
        for entry in entries {
            values[entry.key] = entry.value
        }

        for op in writeBuffer {
            switch op {
            case .set(let key, let value):
                values[key] = value
            case .clear(let key):
                values[key] = nil
            case .clearRange(let begin, let end):
                let keysToRemove = values.keys.filter {
                    compareBytes($0, begin) >= 0 && compareBytes($0, end) < 0
                }
                for key in keysToRemove {
                    values[key] = nil
                }
            case .atomic(let key, let param, let mutationType):
                switch try mutationType.apply(to: values[key], param: param) {
                case .set(let bytes):
                    values[key] = bytes
                case .clear:
                    values[key] = nil
                case .unchanged:
                    break
                }
            }
        }

        let sorted = values
            .map { (key: $0.key, value: $0.value) }
            .sorted { compareBytes($0.key, $1.key) < 0 }
        let keys = sorted.map(\.key)
        let start = begin.resolve(in: keys)
        let end = end.resolve(in: keys)
        guard start < end else { return [] }

        var results = Array(sorted[start..<end])
        if reverse {
            results.reverse()
        }
        if limit > 0 && results.count > limit {
            results = Array(results.prefix(limit))
        }
        return results
    }

    private static func validateReadable(_ lifecycle: Lifecycle) throws {
        switch lifecycle {
        case .open:
            return
        case .committing:
            throw invalidStateError("Transaction commit already in progress")
        case .committed:
            throw invalidStateError("Transaction committed")
        case .cancelled:
            throw invalidStateError("Transaction cancelled")
        case .failed(let error):
            throw error
        }
    }

    private static func invalidStateError(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .unknown,
            backend: .sqlite,
            message: message
        )
    }

    private func markFailed(_ error: any Error, operation: StorageOperation) -> StorageError {
        let storageError = storageError(from: error, operation: operation)
        _state.withLock { state in
            state.lifecycle = .failed(storageError)
            state.writeBuffer.removeAll()
        }
        return storageError
    }

    private func storageError(from error: any Error, operation: StorageOperation) -> StorageError {
        if let storageError = error as? StorageError {
            return storageError
        }

        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .sqlite,
            message: "SQLite transaction failed",
            underlyingDescription: String(describing: error)
        )
    }

    private func cleanupFailedTransactionIfNeeded() {
        let shouldRollbackAndRelease = _state.withLock { state in
            guard case .failed = state.lifecycle else { return false }
            return !state.lockReleased
        }
        guard shouldRollbackAndRelease else { return }

        rollbackBestEffort()
        releaseLockOnce()
    }

    private func rollbackBestEffort() {
        do {
            try connection.execute("ROLLBACK", operation: .rollback)
        } catch {
            // Cancellation and failed-transaction cleanup cannot surface rollback errors.
        }
    }

    private func releaseLockOnce() {
        guard lock != nil else { return }
        let shouldRelease = _state.withLock { state in
            guard !state.lockReleased else { return false }
            state.lockReleased = true
            return true
        }
        guard shouldRelease else { return }
        lock?.unlock()
    }

    private func flushWriteBuffer() throws {
        let ops = try _state.withLock { state -> [WriteOp] in
            switch state.lifecycle {
            case .open, .committing:
                return state.writeBuffer
            case .committed:
                throw Self.invalidStateError("Transaction committed")
            case .cancelled:
                throw Self.invalidStateError("Transaction cancelled")
            case .failed(let error):
                throw error
            }
        }

        do {
            for op in ops {
                switch op {
                case .set(let key, let value):
                    try connection.insertOrReplace(key: key, value: value)
                case .clear(let key):
                    try connection.delete(key: key)
                case .clearRange(let begin, let end):
                    try connection.deleteRange(begin: begin, end: end)
                case .atomic(let key, let param, let mutationType):
                    // Read-modify-write is safe here: the SQLite transaction holds
                    // an exclusive lock, and preceding buffered ops are already
                    // flushed, so the database value is the correct base.
                    // A mid-flush throw is undone by commit()'s ROLLBACK.
                    switch try mutationType.apply(to: connection.get(key: key), param: param) {
                    case .set(let bytes):
                        try connection.insertOrReplace(key: key, value: bytes)
                    case .clear:
                        try connection.delete(key: key)
                    case .unchanged:
                        break
                    }
                }
            }
        } catch {
            throw markFailed(error, operation: .write)
        }
        _state.withLock { $0.writeBuffer.removeAll() }
    }
}
