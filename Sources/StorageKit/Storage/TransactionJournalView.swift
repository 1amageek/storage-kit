import DatabaseTypes

/// Bounded overlay algorithms shared by every checkpoint adapter.
struct TransactionJournalView: Sendable {
    enum Position: Sendable {
        case beforeAll
        case key(ByteString)
        case pastEnd
    }

    let mutations: [TransactionJournalMutation]
    let access: any TransactionJournalBackendAccess
    let activity: TransactionJournalActivity
    let backend: StorageBackend
    let snapshot: Bool
    var localRows: [(ByteString, ByteString)] = []
    let retention: TransactionJournalReadRetention

    static func requireResolved(_ mutations: [TransactionJournalMutation], backend: StorageBackend) throws {
        for mutation in mutations {
            if case .atomic(_, _, let type, _) = mutation {
                switch type {
                case .setVersionstampedKey, .setVersionstampedValue:
                    throw StorageError.unsupportedOperation(
                        "Unresolved versionstamped mutations cannot be read before commit",
                        operation: .read, backend: backend
                    )
                default: break
                }
            }
        }
    }

    static func value(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int?,
        mutations: [TransactionJournalMutation],
        access: any TransactionJournalBackendAccess,
        activity: TransactionJournalActivity,
        backend: StorageBackend
    ) async throws -> ByteString? {
        if let maximumByteCount, maximumByteCount < 0 {
            throw StorageError.invalidPointReadMaximum(maximumByteCount, backend: backend)
        }
        try requireResolved(mutations, backend: backend)
        var first = mutations.startIndex
        var base: ByteString?
        var determined = false
        var hasAtomic = false
        for index in mutations.indices.reversed() {
            switch mutations[index] {
            case .set(let candidate, let bytes, _) where candidate == key:
                base = bytes; determined = true; first = index + 1
            case .clear(let candidate, _) where candidate == key:
                base = nil; determined = true; first = index + 1
            case .clearRange(let begin, let end, _)
                where compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0:
                base = nil; determined = true; first = index + 1
            case .atomic(let candidate, _, _, _) where candidate == key:
                hasAtomic = true
            default: break
            }
            if determined { break }
        }
        if !determined {
            let workspace = activity.availableBytes
            let bound = hasAtomic ? workspace : min(maximumByteCount ?? workspace, workspace)
            base = try await access.getValue(for: key, snapshot: snapshot, maximumByteCount: bound)
        }
        let result = try apply(key: key, base: base, mutations: mutations[first...], activity: activity)
        if let maximumByteCount, let result, result.count > maximumByteCount {
            throw StorageError.pointReadValueTooLarge(
                observedByteCount: result.count, maximumByteCount: maximumByteCount, backend: backend
            )
        }
        return result
    }

    static func apply(
        key: ByteString,
        base: ByteString?,
        mutations: ArraySlice<TransactionJournalMutation>,
        activity: TransactionJournalActivity
    ) throws -> ByteString? {
        var value = base
        var ownedCharge = 0
        defer { activity.release(ownedCharge) }
        for mutation in mutations {
            try activity.consumeWork(1)
            switch mutation {
            case .set(let candidate, let bytes, _) where candidate == key:
                value = bytes
                activity.release(ownedCharge); ownedCharge = 0
            case .clear(let candidate, _) where candidate == key:
                value = nil
                activity.release(ownedCharge); ownedCharge = 0
            case .clearRange(let begin, let end, _)
                where compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0:
                value = nil
                activity.release(ownedCharge); ownedCharge = 0
            case .atomic(let candidate, let parameter, let type, _) where candidate == key:
                // Width bounds are known before the shared algebra allocates.
                // max/min may hold an adjusted operand and its output together.
                let (bytes, overflow) = parameter.count.multipliedReportingOverflow(by: 2)
                guard !overflow else {
                    throw TransactionCheckpointError.limitExceeded(
                        resource: .retainedBytes, actual: Int.max,
                        maximum: activity.limits.maximumRetainedBytes
                    )
                }
                try activity.reserve(bytes)
                do {
                    switch try type.apply(to: value, param: parameter) {
                    case .set(let result):
                        value = result
                        activity.release(ownedCharge)
                        ownedCharge = bytes
                    case .clear:
                        value = nil
                        activity.release(ownedCharge)
                        ownedCharge = 0
                        activity.release(bytes)
                    case .unchanged:
                        activity.release(bytes)
                    }
                } catch {
                    activity.release(bytes)
                    throw error
                }
            default: break
            }
        }
        return value
    }

    mutating func prepare() async throws {
        try Self.requireResolved(mutations, backend: backend)
        var keys: [ByteString] = []
        for mutation in mutations {
            switch mutation {
            case .set(let key, _, _), .clear(let key, _), .atomic(let key, _, _, _): keys.append(key)
            case .clearRange, .writeConflict: break
            }
        }
        keys.sort { compareBytes($0, $1) < 0 }
        var previous: ByteString?
        for key in keys {
            guard previous != key else { continue }
            previous = key
            try ensureStorageTaskIsActive()
            if let value = try await Self.value(
                for: key, snapshot: snapshot, maximumByteCount: nil,
                mutations: mutations, access: access, activity: activity, backend: backend
            ) {
                localRows.append((try retention.retain(key), try retention.retain(value)))
            }
        }
    }

    func resolve(_ selector: KeySelector) async throws -> Position {
        // Check magnitudes without negating Int.min or advancing an Int.max offset.
        guard selector.offset.magnitude <= UInt(activity.limits.maximumWorkUnits) else {
            throw TransactionCheckpointError.limitExceeded(
                resource: .workUnits, actual: Int.max, maximum: activity.limits.maximumWorkUnits
            )
        }
        var position: Position
        if selector.offset > 0 {
            position = try await nearest(to: selector.key, inclusive: !selector.orEqual, reverse: false)
            if selector.offset > 1 {
                for _ in 1..<selector.offset {
                    try activity.consumeWork(1)
                    switch position {
                    case .key(let key): position = try await nearest(to: key, inclusive: false, reverse: false)
                    case .pastEnd: return .pastEnd
                    case .beforeAll: preconditionFailure("Successor returned a lower sentinel")
                    }
                }
            }
        } else {
            position = try await nearest(to: selector.key, inclusive: selector.orEqual, reverse: true)
            for _ in 0..<selector.offset.magnitude {
                try activity.consumeWork(1)
                switch position {
                case .key(let key): position = try await nearest(to: key, inclusive: false, reverse: true)
                case .beforeAll: return .beforeAll
                case .pastEnd: preconditionFailure("Predecessor returned an upper sentinel")
                }
            }
        }
        return position
    }

    private func nearest(to reference: ByteString, inclusive: Bool, reverse: Bool) async throws -> Position {
        let bound: TransactionJournalBoundary = inclusive ? .inclusive(reference) : .exclusive(reference)
        let range = TransactionJournalRange(
            lower: reverse ? .unbounded : bound,
            upper: reverse ? bound : .unbounded
        )
        let raw = try await firstVisible(in: range, reverse: reverse)
        let local = reverse
            ? localRows.last(where: { compareBytes($0.0, reference) < 0 || (inclusive && $0.0 == reference) })?.0
            : localRows.first(where: { compareBytes($0.0, reference) > 0 || (inclusive && $0.0 == reference) })?.0
        switch (raw, local) {
        case (.none, .none): return reverse ? .beforeAll : .pastEnd
        case (.some(let key), .none), (.none, .some(let key)): return .key(key)
        case (.some(let raw), .some(let local)):
            return .key((compareBytes(raw, local) < 0) == !reverse ? raw : local)
        }
    }

    private func firstVisible(in range: TransactionJournalRange, reverse: Bool) async throws -> ByteString? {
        var cursor = access.rangeCursor(
            in: range, reverse: reverse, snapshot: snapshot, pageSize: 1,
            maximumByteCount: activity.availableBytes
        )
        do {
            while let row = try await cursor.next() {
                try activity.consumeWork(1)
                if try Self.apply(key: row.0, base: row.1, mutations: mutations[...], activity: activity) != nil {
                    // A selector key survives native cursor closure; detach a
                    // bounded key rather than retaining an enclosing page.
                    let key = try retention.retain(row.0)
                    try await cursor.finish()
                    return key
                }
            }
            try await cursor.finish()
            return nil
        } catch {
            let body = error
            do { try await cursor.finish() }
            catch { throw StorageRangeCleanupError(iterationError: body, cleanupError: error) }
            throw body
        }
    }

    static func contains(_ key: ByteString, begin: Position, end: Position) -> Bool {
        let after: Bool
        switch begin {
        case .beforeAll: after = true
        case .pastEnd: after = false
        case .key(let lower): after = compareBytes(key, lower) >= 0
        }
        let before: Bool
        switch end {
        case .beforeAll: before = false
        case .pastEnd: before = true
        case .key(let upper): before = compareBytes(key, upper) < 0
        }
        return after && before
    }
}
