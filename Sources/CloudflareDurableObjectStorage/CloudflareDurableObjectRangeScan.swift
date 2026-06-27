import StorageKit

struct CloudflareDurableObjectRangeScan: CloudflareDurableObjectRangeScanning {
    private let client: any CloudflareDurableObjectStorageClient
    private let scope: CloudflareDurableObjectStorageScope
    private let begin: KeySelector
    private let end: KeySelector
    private let snapshot: Bool
    private let initialExpectedReadVersion: Int64?
    private let pageLimit: Int
    private let userLimit: Int
    private let reverse: Bool
    private let writeBuffer: [CloudflareDurableObjectWriteOp]
    private let ensureOpen: @Sendable () throws -> Void
    private let recordReadVersion: @Sendable (Int64) -> Void
    private let recordReadConflictRange: @Sendable (CloudflareDurableObjectConflictRange) -> Void

    private var cursor: String?
    private var stableReadVersion: Int64?
    private var finishedHostPages = false
    private var hostRows: [(Bytes, Bytes)] = []
    private var hostIndex = 0
    private var localRows: [(Bytes, Bytes)] = []
    private var localIndex = 0
    private var localRowKeys = Set<Bytes>()
    private var localRowsPrepared = false
    private var lastEmittedKey: Bytes?
    private var emittedCount = 0

    init(
        client: any CloudflareDurableObjectStorageClient,
        scope: CloudflareDurableObjectStorageScope,
        begin: KeySelector,
        end: KeySelector,
        snapshot: Bool,
        initialExpectedReadVersion: Int64?,
        pageLimit: Int,
        userLimit: Int,
        reverse: Bool,
        writeBuffer: [CloudflareDurableObjectWriteOp],
        ensureOpen: @escaping @Sendable () throws -> Void,
        recordReadVersion: @escaping @Sendable (Int64) -> Void,
        recordReadConflictRange: @escaping @Sendable (CloudflareDurableObjectConflictRange) -> Void
    ) {
        self.client = client
        self.scope = scope
        self.begin = begin
        self.end = end
        self.snapshot = snapshot
        self.initialExpectedReadVersion = initialExpectedReadVersion
        self.pageLimit = pageLimit
        self.userLimit = userLimit
        self.reverse = reverse
        self.writeBuffer = writeBuffer
        self.ensureOpen = ensureOpen
        self.recordReadVersion = recordReadVersion
        self.recordReadConflictRange = recordReadConflictRange
    }

    mutating func next() async throws -> (Bytes, Bytes)? {
        try ensureOpen()
        try validateLimits()
        try await prepareLocalRowsIfNeeded()

        while !isUserLimitReached {
            try await ensureHostRowIfNeeded()

            let hostRow = currentHostRow
            let localRow = currentLocalRow

            switch (hostRow, localRow) {
            case (.none, .none):
                return nil
            case (.some(let row), .none):
                consumeHostRow()
                return try emit(row)
            case (.none, .some(let row)):
                consumeLocalRow()
                return try emit(row)
            case (.some(let hostRow), .some(let localRow)):
                let comparison = CloudflareDurableObjectByteOrdering.compare(hostRow.0, localRow.0)
                if comparison == 0 {
                    consumeHostRow()
                    consumeLocalRow()
                    return try emit(localRow)
                } else if hostRowShouldWin(comparison) {
                    consumeHostRow()
                    return try emit(hostRow)
                } else {
                    consumeLocalRow()
                    return try emit(localRow)
                }
            }
        }

        return nil
    }

    private func validateLimits() throws {
        guard userLimit >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Range limit must not be negative"
            )
        }
        guard pageLimit > 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Configured range page size must be positive"
            )
        }
    }

    private var isUserLimitReached: Bool {
        userLimit > 0 && emittedCount >= userLimit
    }

    private var currentHostRow: (Bytes, Bytes)? {
        guard hostIndex < hostRows.count else {
            return nil
        }
        return hostRows[hostIndex]
    }

    private var currentLocalRow: (Bytes, Bytes)? {
        guard localIndex < localRows.count else {
            return nil
        }
        return localRows[localIndex]
    }

    private mutating func consumeHostRow() {
        hostIndex += 1
    }

    private mutating func consumeLocalRow() {
        localIndex += 1
    }

    private func hostRowShouldWin(_ comparison: Int) -> Bool {
        reverse ? comparison > 0 : comparison < 0
    }

    private mutating func emit(_ row: (Bytes, Bytes)) throws -> (Bytes, Bytes) {
        if let lastEmittedKey {
            let comparison = CloudflareDurableObjectByteOrdering.compare(row.0, lastEmittedKey)
            let isStrictlyOrdered = reverse ? comparison < 0 : comparison > 0
            guard isStrictlyOrdered else {
                throw StorageError(
                    code: .backendFailure,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object range rows were not strictly ordered"
                )
            }
        }
        lastEmittedKey = row.0
        emittedCount += 1
        return row
    }

    private mutating func ensureHostRowIfNeeded() async throws {
        while hostIndex >= hostRows.count && !finishedHostPages {
            try await loadNextHostPage()
        }
    }

    private mutating func loadNextHostPage() async throws {
        let response = try await Self.mapHostError(operation: .rangeRead) {
            try await client.range(
                CloudflareDurableObjectRangeRequest(
                    scope: scope,
                    begin: CloudflareDurableObjectKeySelector(begin),
                    end: CloudflareDurableObjectKeySelector(end),
                    limit: pageLimit,
                    reverse: reverse,
                    snapshot: snapshot,
                    expectedReadVersion: expectedReadVersionForRequest,
                    cursor: cursor
                )
            )
        }

        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot, let conflictRange = response.conflictRange {
            recordReadConflictRange(conflictRange)
        }
        try updateCursor(response.nextCursor, rowCount: response.rows.count)

        var rows: [(Bytes, Bytes)] = []
        rows.reserveCapacity(response.rows.count)
        for row in response.rows {
            let key = row.key.rawValue
            guard !localRowKeys.contains(key) else {
                continue
            }
            let value = try value(for: key, committed: row.value.rawValue)
            guard let value else {
                continue
            }
            rows.append((key, value))
        }

        hostRows = rows
        hostIndex = 0
    }

    private mutating func updateCursor(_ nextCursor: String?, rowCount: Int) throws {
        guard let nextCursor else {
            cursor = nil
            finishedHostPages = true
            return
        }
        guard rowCount > 0 else {
            throw StorageError(
                code: .backendFailure,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object host returned a range cursor with an empty page"
            )
        }
        guard nextCursor != cursor else {
            throw StorageError(
                code: .backendFailure,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object host returned a repeated range cursor"
            )
        }
        cursor = nextCursor
    }

    private var expectedReadVersionForRequest: Int64? {
        stableReadVersion ?? initialExpectedReadVersion
    }

    private mutating func acceptReadVersion(_ version: Int64) throws {
        if let stableReadVersion {
            guard stableReadVersion == version else {
                throw StorageError(
                    code: .transactionConflict,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Range page read version changed during pagination"
                )
            }
        } else {
            stableReadVersion = version
        }

        if !snapshot {
            recordReadVersion(version)
        }
    }

    private mutating func prepareLocalRowsIfNeeded() async throws {
        guard !localRowsPrepared else {
            return
        }
        localRowsPrepared = true

        var keys: [Bytes] = []
        var keySet = Set<Bytes>()
        for op in writeBuffer {
            switch op {
            case .set(let key, _), .clear(let key), .atomic(let key, _, _):
                if keySet.insert(key).inserted {
                    keys.append(key)
                }
            case .clearRange:
                continue
            }
        }

        var rows: [(Bytes, Bytes)] = []
        rows.reserveCapacity(keys.count)
        for key in keys where rangeContainsLocalKey(key) {
            let committed = try await readCommittedValue(for: key)
            guard let value = try value(for: key, committed: committed) else {
                continue
            }
            rows.append((key, value))
        }

        rows.sort {
            let comparison = CloudflareDurableObjectByteOrdering.compare($0.0, $1.0)
            return reverse ? comparison > 0 : comparison < 0
        }
        localRows = rows
        localRowKeys = Set(rows.map { $0.0 })
    }

    private mutating func readCommittedValue(for key: Bytes) async throws -> Bytes? {
        let response = try await Self.mapHostError(operation: .read) {
            try await client.read(
                CloudflareDurableObjectReadRequest(
                    scope: scope,
                    key: CloudflareDurableObjectBytes(key),
                    snapshot: snapshot,
                    expectedReadVersion: expectedReadVersionForRequest
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        return response.value?.rawValue
    }

    private func value(for key: Bytes, committed: Bytes?) throws -> Bytes? {
        var value = committed
        for op in writeBuffer {
            switch op {
            case .set(let opKey, let opValue) where opKey == key:
                value = opValue
            case .clear(let opKey) where opKey == key:
                value = nil
            case .clearRange(let beginKey, let endKey)
                where CloudflareDurableObjectByteOrdering.compare(key, beginKey) >= 0
                    && CloudflareDurableObjectByteOrdering.compare(key, endKey) < 0:
                value = nil
            case .atomic(let opKey, let param, let mutationType) where opKey == key:
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

    private func rangeContainsLocalKey(_ key: Bytes) -> Bool {
        let keys = [key]
        let startIndex = begin.resolve(in: keys)
        let endIndex = end.resolve(in: keys)
        return startIndex == 0 && endIndex > 0
    }

    private static func mapHostError<T>(
        operation: StorageOperation,
        _ body: () async throws -> T
    ) async throws -> T {
        do {
            return try await body()
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as StorageError {
            throw error
        } catch {
            throw StorageError(
                code: .backendFailure,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object client operation failed",
                underlyingDescription: String(describing: error)
            )
        }
    }
}
