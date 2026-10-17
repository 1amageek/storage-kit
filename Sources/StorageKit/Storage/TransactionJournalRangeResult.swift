import DatabaseTypes

/// A bounded merge of raw snapshot rows and the surviving mutation journal.
struct TransactionJournalRangeResult: TransactionRangeResult {
    let begin: KeySelector
    let end: KeySelector
    let limit: Int
    let reverse: Bool
    let snapshot: Bool
    let access: any TransactionJournalBackendAccess
    let mutations: [TransactionJournalMutation]
    let activity: TransactionJournalActivity
    let backend: StorageBackend
    let lifetime: TransactionJournalCursorLifetime

    func makeCursor() -> Cursor {
        let retention = TransactionJournalReadRetention(activity: activity)
        return Cursor(
            begin: begin, end: end, limit: limit, reverse: reverse,
            view: TransactionJournalView(
                mutations: mutations, access: access, activity: activity,
                backend: backend, snapshot: snapshot, retention: retention
            ),
            owner: TransactionJournalCursorOwner(retention: retention, lifetime: lifetime)
        )
    }

    struct Cursor: TransactionRangeCursor {
        let begin: KeySelector
        let end: KeySelector
        let limit: Int
        let reverse: Bool
        var view: TransactionJournalView?
        fileprivate let owner: TransactionJournalCursorOwner
        var raw: KeyValueCursor?
        var rawRow: (ByteString, ByteString)?
        var rawCharge = 0
        var localRows: [(ByteString, ByteString)] = []
        var localIndex = 0
        var emitted = 0
        var prepared = false
        var rawExhausted = false
        var finished = false

        mutating func next() async throws -> (ByteString, ByteString)? {
            guard !finished else { return nil }
            guard let activity = view?.activity else { throw TransactionCheckpointError.closed }
            let operation = TransactionJournalContext.operation
            let nested = operation?.isActive == true && operation?.journal.activity === activity
            try activity.beginOperation(nested: nested)
            defer { activity.endOperation() }
            do {
                try ensureStorageTaskIsActive()
                if !prepared { try await prepare() }
                if limit > 0 && emitted >= limit {
                    try await finish(isolation: #isolation)
                    return nil
                }
                try await loadRawIfNeeded()
                let local = localIndex < localRows.count ? localRows[localIndex] : nil
                let result: (ByteString, ByteString)?
                switch (rawRow, local) {
                case (.none, .none):
                    try await finish(isolation: #isolation)
                    return nil
                case (.some(let row), .none):
                    result = row
                    consumeRaw()
                case (.none, .some(let row)):
                    result = row
                    localIndex += 1
                case (.some(let raw), .some(let local)):
                    if (compareBytes(raw.0, local.0) < 0) == !reverse {
                        result = raw
                        consumeRaw()
                    } else {
                        result = local
                        localIndex += 1
                    }
                }
                emitted += 1
                return result
            } catch {
                let body = error
                do { try await finish(isolation: #isolation) }
                catch { throw StorageRangeCleanupError(iterationError: body, cleanupError: error) }
                throw body
            }
        }

        private mutating func prepare() async throws {
            guard limit >= 0 else { throw TransactionCheckpointError.invalidLimits }
            guard var preparedView = view else { throw TransactionCheckpointError.closed }
            try await preparedView.prepare()
            let lower = try await preparedView.resolve(begin)
            let upper = try await preparedView.resolve(end)
            localRows = preparedView.localRows.filter {
                TransactionJournalView.contains($0.0, begin: lower, end: upper)
            }
            if reverse { localRows.reverse() }
            let empty: Bool
            switch (lower, upper) {
            case (.pastEnd, _), (_, .beforeAll): empty = true
            case (.key(let lower), .key(let upper)): empty = compareBytes(lower, upper) >= 0
            default: empty = false
            }
            if empty {
                rawExhausted = true
                localRows.removeAll(keepingCapacity: false)
            } else {
                let lowerBound: TransactionJournalBoundary
                if case .key(let key) = lower { lowerBound = .inclusive(key) }
                else { lowerBound = .unbounded }
                let upperBound: TransactionJournalBoundary
                if case .key(let key) = upper { upperBound = .exclusive(key) }
                else { upperBound = .unbounded }
                raw = preparedView.access.rangeCursor(
                    in: TransactionJournalRange(lower: lowerBound, upper: upperBound),
                    reverse: reverse, snapshot: preparedView.snapshot,
                    pageSize: preparedView.activity.limits.maximumReadPageRows,
                    maximumByteCount: preparedView.activity.availableBytes
                )
            }
            view = preparedView
            prepared = true
        }

        private mutating func loadRawIfNeeded() async throws {
            guard rawRow == nil, !rawExhausted, let view else { return }
            while true {
                guard var cursor = raw else { rawExhausted = true; return }
                let row: (ByteString, ByteString)?
                do {
                    row = try await cursor.next()
                    raw = cursor
                } catch {
                    raw = cursor
                    throw error
                }
                guard let row else {
                    rawExhausted = true
                    return
                }
                try view.activity.consumeWork(1)
                if view.localRows.contains(where: { $0.0 == row.0 }) { continue }
                guard let value = try TransactionJournalView.apply(
                    key: row.0, base: row.1, mutations: view.mutations[...], activity: view.activity
                ) else { continue }
                let (key, keyCharge) = try view.activity.retaining(row.0)
                do {
                    let (value, valueCharge) = try view.activity.retaining(value)
                    rawCharge = keyCharge + valueCharge
                    rawRow = (key, value)
                } catch {
                    view.activity.release(keyCharge)
                    throw error
                }
                return
            }
        }

        private mutating func consumeRaw() {
            rawRow = nil
            if let activity = view?.activity { activity.release(rawCharge) }
            rawCharge = 0
        }

        mutating func finish(isolation actor: isolated (any Actor)?) async throws {
            guard !finished else { return }
            finished = true
            consumeRaw()
            localRows.removeAll(keepingCapacity: false)
            view = nil
            defer { owner.finish() }
            if var cursor = raw {
                raw = nil
                try await cursor.finish()
            }
        }
    }
}

/// Releases payload charges and marks a registration complete on every exit.
private final class TransactionJournalCursorOwner: Sendable {
    private let retention: TransactionJournalReadRetention
    private let lifetime: TransactionJournalCursorLifetime

    init(retention: TransactionJournalReadRetention, lifetime: TransactionJournalCursorLifetime) {
        self.retention = retention
        self.lifetime = lifetime
    }

    func finish() {
        retention.release()
        lifetime.finish()
    }

    deinit { finish() }
}
