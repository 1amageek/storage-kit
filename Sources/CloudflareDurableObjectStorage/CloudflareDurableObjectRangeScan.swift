import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit

struct CloudflareDurableObjectRangeScan: CloudflareDurableObjectRangeScanning {
    private struct RangeStorageAccess: Sendable {
        let read: @Sendable (StorageWireReadRequest) async throws -> StorageWireReadResponse
        let range: @Sendable (StorageWireRangeRequest) async throws -> StorageWireRangeResponse
        let ensureOpen: @Sendable () throws -> Void
        let recordReadVersion: @Sendable (Int64) throws -> Void
        let recordReadConflictRange: @Sendable (StorageWireKeyRange) throws -> Void
    }

    private enum ResolvedBoundary: Sendable, Equatable {
        case beforeAll
        case key(ByteString)
        case pastEnd
    }

    private var storageAccess: RangeStorageAccess?
    private let scope: StorageWireScope
    private let begin: KeySelector
    private let end: KeySelector
    private let snapshot: Bool
    private let initialExpectedReadVersion: Int64?
    private let pageLimit: Int
    private let maxSelectorResolutionSteps: Int
    private let userLimit: Int
    private let reverse: Bool
    private var mutations: [StorageWireWriteOperation]
    private var cursorKey: ByteString?
    private var stableReadVersion: Int64?
    private var finishedHostPages = false
    private var hostRows: [(ByteString, ByteString)] = []
    private var hostIndex = 0
    private var localRows: [(ByteString, ByteString)] = []
    private var localIndex = 0
    private var allLocalRows: [(ByteString, ByteString)] = []
    private var viewPrepared = false
    private var hostBegin = StorageWireRangeBoundary.unbounded
    private var hostEnd = StorageWireRangeBoundary.unbounded
    private var lastEmittedKey: ByteString?
    private var emittedCount = 0
    private var selectorResolutionSteps = 0
    private var finished = false

    init(
        read:
            @escaping @Sendable (
                StorageWireReadRequest
            ) async throws -> StorageWireReadResponse,
        range:
            @escaping @Sendable (
                StorageWireRangeRequest
            ) async throws -> StorageWireRangeResponse,
        scope: StorageWireScope,
        begin: KeySelector,
        end: KeySelector,
        snapshot: Bool,
        initialExpectedReadVersion: Int64?,
        pageLimit: Int,
        maxSelectorResolutionSteps: Int,
        userLimit: Int,
        reverse: Bool,
        mutations: [StorageWireWriteOperation],
        ensureOpen: @escaping @Sendable () throws -> Void,
        recordReadVersion: @escaping @Sendable (Int64) throws -> Void,
        recordReadConflictRange:
            @escaping @Sendable (
                StorageWireKeyRange
            ) throws -> Void
    ) {
        self.storageAccess = RangeStorageAccess(
            read: read,
            range: range,
            ensureOpen: ensureOpen,
            recordReadVersion: recordReadVersion,
            recordReadConflictRange: recordReadConflictRange
        )
        self.scope = scope
        self.begin = begin
        self.end = end
        self.snapshot = snapshot
        self.initialExpectedReadVersion = initialExpectedReadVersion
        self.pageLimit = pageLimit
        self.maxSelectorResolutionSteps = maxSelectorResolutionSteps
        self.userLimit = userLimit
        self.reverse = reverse
        self.mutations = mutations
    }

    mutating func next() async throws -> (ByteString, ByteString)? {
        guard !finished else { return nil }
        do {
            guard let storageAccess else {
                throw invalidRange("Range storage access is unavailable")
            }
            try storageAccess.ensureOpen()
            try validateLimits()
            try await prepareViewIfNeeded()

            while !isUserLimitReached {
                try ensureStorageTaskIsActive()
                try await ensureHostRowIfNeeded()
                let hostRow = currentHostRow
                let localRow = currentLocalRow

                switch (hostRow, localRow) {
                case (.none, .none):
                    finishScan()
                    return nil
                case (.some(let row), .none):
                    consumeHostRow()
                    return try emit(row)
                case (.none, .some(let row)):
                    consumeLocalRow()
                    return try emit(row)
                case (.some(let hostRow), .some(let localRow)):
                    let comparison = CloudflareDurableObjectByteOrdering.compare(
                        hostRow.0,
                        localRow.0
                    )
                    if comparison == 0 {
                        consumeHostRow()
                        consumeLocalRow()
                        return try emit(localRow)
                    }
                    if hostRowShouldWin(comparison) {
                        consumeHostRow()
                        return try emit(hostRow)
                    }
                    consumeLocalRow()
                    return try emit(localRow)
                }
            }
            finishScan()
            return nil
        } catch {
            finishScan()
            throw error
        }
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        finishScan()
    }

    private mutating func finishScan() {
        storageAccess = nil
        cursorKey = nil
        hostRows.removeAll(keepingCapacity: false)
        hostIndex = 0
        localRows.removeAll(keepingCapacity: false)
        localIndex = 0
        allLocalRows.removeAll(keepingCapacity: false)
        mutations.removeAll(keepingCapacity: false)
        lastEmittedKey = nil
        finishedHostPages = true
        finished = true
    }

    private func validateLimits() throws {
        guard userLimit >= 0 else {
            throw invalidRange("Range limit must not be negative")
        }
        guard pageLimit > 0 else {
            throw invalidRange("Configured range page size must be positive")
        }
        guard maxSelectorResolutionSteps > 0 else {
            throw invalidRange(
                "Configured selector resolution limit must be positive"
            )
        }
        guard begin.offset.magnitude <= UInt(maxSelectorResolutionSteps),
            end.offset.magnitude <= UInt(maxSelectorResolutionSteps)
        else {
            throw invalidRange("KeySelector offset exceeds configured limit")
        }
    }

    private func invalidRange(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .rangeRead,
            backend: .cloudflareDurableObject,
            message: message
        )
    }

    private var isUserLimitReached: Bool {
        userLimit > 0 && emittedCount >= userLimit
    }

    private var currentHostRow: (ByteString, ByteString)? {
        hostIndex < hostRows.count ? hostRows[hostIndex] : nil
    }

    private var currentLocalRow: (ByteString, ByteString)? {
        localIndex < localRows.count ? localRows[localIndex] : nil
    }

    private mutating func consumeHostRow() { hostIndex += 1 }
    private mutating func consumeLocalRow() { localIndex += 1 }

    private func hostRowShouldWin(_ comparison: Int) -> Bool {
        reverse ? comparison > 0 : comparison < 0
    }

    private mutating func emit(_ row: (ByteString, ByteString)) throws -> (ByteString, ByteString) {
        if let lastEmittedKey {
            let comparison = CloudflareDurableObjectByteOrdering.compare(
                row.0,
                lastEmittedKey
            )
            let ordered = reverse ? comparison < 0 : comparison > 0
            guard ordered else {
                throw StorageError(
                    code: .backendFailure,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Range rows were not strictly ordered"
                )
            }
        }
        lastEmittedKey = row.0
        emittedCount += 1
        return row
    }

    private mutating func prepareViewIfNeeded() async throws {
        guard !viewPrepared else { return }
        viewPrepared = true
        guard !mutations.isEmpty else {
            hostBegin = .selector(StorageWireKeySelector(begin))
            hostEnd = .selector(StorageWireKeySelector(end))
            return
        }
        try await prepareAllLocalRows()
        let resolvedBegin = try await resolve(begin)
        let resolvedEnd = try await resolve(end)
        guard !rangeIsEmpty(begin: resolvedBegin, end: resolvedEnd) else {
            finishedHostPages = true
            return
        }
        hostBegin = selector(for: resolvedBegin)
        hostEnd = selector(for: resolvedEnd)
        localRows = allLocalRows.filter {
            contains($0.0, begin: resolvedBegin, end: resolvedEnd)
        }
        if reverse { localRows.reverse() }
    }

    private mutating func prepareAllLocalRows() async throws {
        var keys: [ByteString] = []
        keys.reserveCapacity(mutations.count)
        for operation in mutations {
            switch operation {
            case .set(let key, _), .clear(let key), .atomic(let key, _, _):
                keys.append(key)
            case .clearRange:
                continue
            }
        }
        keys.sort {
            CloudflareDurableObjectByteOrdering.compare($0, $1) < 0
        }
        removeDuplicateKeys(from: &keys)
        var rows: [(ByteString, ByteString)] = []
        rows.reserveCapacity(keys.count)
        for key in keys {
            try ensureStorageTaskIsActive()
            let committed = try await readCommittedValue(for: key)
            if let value = try value(for: key, committed: committed) {
                rows.append((key, value))
            }
        }
        allLocalRows = rows
    }

    private func removeDuplicateKeys(from keys: inout [ByteString]) {
        guard keys.count > 1 else { return }
        var uniqueCount = 1
        for index in 1..<keys.count {
            guard
                CloudflareDurableObjectByteOrdering.compare(
                    keys[index],
                    keys[uniqueCount - 1]
                ) != 0
            else {
                continue
            }
            if index != uniqueCount {
                keys[uniqueCount] = keys[index]
            }
            uniqueCount += 1
        }
        keys.removeLast(keys.count - uniqueCount)
    }

    private mutating func resolve(
        _ selector: KeySelector
    ) async throws -> ResolvedBoundary {
        var boundary = try await predecessor(
            of: selector.key,
            inclusive: selector.orEqual
        )
        if selector.offset > 0 {
            for _ in 0..<selector.offset {
                try ensureStorageTaskIsActive()
                boundary = try await successor(after: boundary)
            }
        } else if selector.offset < 0 {
            for _ in 0..<selector.offset.magnitude {
                try ensureStorageTaskIsActive()
                boundary = try await predecessor(before: boundary)
            }
        }
        return boundary
    }

    private mutating func successor(
        after boundary: ResolvedBoundary
    ) async throws -> ResolvedBoundary {
        switch boundary {
        case .beforeAll:
            return try await successor(of: [], inclusive: true)
        case .key(let key):
            return try await successor(of: key, inclusive: false)
        case .pastEnd:
            return .pastEnd
        }
    }

    private mutating func predecessor(
        before boundary: ResolvedBoundary
    ) async throws -> ResolvedBoundary {
        switch boundary {
        case .beforeAll:
            return .beforeAll
        case .key(let key):
            return try await predecessor(of: key, inclusive: false)
        case .pastEnd:
            return try await lastBoundary()
        }
    }

    private mutating func predecessor(
        of reference: ByteString,
        inclusive: Bool
    ) async throws -> ResolvedBoundary {
        let committed = try await committedPredecessor(
            of: reference,
            inclusive: inclusive
        )
        let local = localPredecessor(of: reference, inclusive: inclusive)
        return maxBoundary(committed, local) ?? .beforeAll
    }

    private mutating func successor(
        of reference: ByteString,
        inclusive: Bool
    ) async throws -> ResolvedBoundary {
        let committed = try await committedSuccessor(
            of: reference,
            inclusive: inclusive
        )
        let local = localSuccessor(of: reference, inclusive: inclusive)
        return minBoundary(committed, local) ?? .pastEnd
    }

    private mutating func lastBoundary() async throws -> ResolvedBoundary {
        let committed = try await lastCommittedBoundary()
        let local = allLocalRows.last.map { ResolvedBoundary.key($0.0) }
        return maxBoundary(committed, local) ?? .beforeAll
    }

    private func localPredecessor(
        of reference: ByteString,
        inclusive: Bool
    ) -> ResolvedBoundary? {
        allLocalRows.reversed().first {
            let comparison = CloudflareDurableObjectByteOrdering.compare(
                $0.0,
                reference
            )
            return comparison < 0 || (inclusive && comparison == 0)
        }.map { .key($0.0) }
    }

    private func localSuccessor(
        of reference: ByteString,
        inclusive: Bool
    ) -> ResolvedBoundary? {
        allLocalRows.first {
            let comparison = CloudflareDurableObjectByteOrdering.compare(
                $0.0,
                reference
            )
            return comparison > 0 || (inclusive && comparison == 0)
        }.map { .key($0.0) }
    }

    private mutating func committedPredecessor(
        of reference: ByteString,
        inclusive: Bool
    ) async throws -> ResolvedBoundary? {
        var current = reference
        var includeCurrent = inclusive
        while true {
            try consumeSelectorResolutionStep()
            let response = try await rangeResponse(
                begin: .unbounded,
                end: includeCurrent
                    ? .selector(
                        StorageWireKeySelector(
                            .firstGreaterThan(current)
                        )
                    )
                    : .selector(
                        StorageWireKeySelector(
                            .firstGreaterOrEqual(current)
                        )
                    ),
                limit: 1,
                reverse: true,
                cursorKey: nil
            )
            guard let row = response.rows.first else { return nil }
            let key = row.key
            if try value(for: key, committed: row.value) != nil {
                return .key(key.detached())
            }
            current = key.detached()
            includeCurrent = false
        }
    }

    private mutating func committedSuccessor(
        of reference: ByteString,
        inclusive: Bool
    ) async throws -> ResolvedBoundary? {
        var current = reference
        var includeCurrent = inclusive
        while true {
            try consumeSelectorResolutionStep()
            let response = try await rangeResponse(
                begin: includeCurrent
                    ? .selector(
                        StorageWireKeySelector(
                            .firstGreaterOrEqual(current)
                        )
                    )
                    : .selector(
                        StorageWireKeySelector(
                            .firstGreaterThan(current)
                        )
                    ),
                end: .unbounded,
                limit: 1,
                reverse: false,
                cursorKey: nil
            )
            guard let row = response.rows.first else { return nil }
            let key = row.key
            if try value(for: key, committed: row.value) != nil {
                return .key(key.detached())
            }
            current = key.detached()
            includeCurrent = false
        }
    }

    private mutating func lastCommittedBoundary() async throws -> ResolvedBoundary? {
        var end = StorageWireRangeBoundary.unbounded
        while true {
            try consumeSelectorResolutionStep()
            let response = try await rangeResponse(
                begin: .unbounded,
                end: end,
                limit: 1,
                reverse: true,
                cursorKey: nil
            )
            guard let row = response.rows.first else { return nil }
            let key = row.key
            if try value(for: key, committed: row.value) != nil {
                return .key(key.detached())
            }
            let detachedKey = key.detached()
            end = .selector(
                StorageWireKeySelector(
                    .firstGreaterOrEqual(detachedKey)
                )
            )
        }
    }

    private mutating func consumeSelectorResolutionStep() throws {
        selectorResolutionSteps += 1
        guard selectorResolutionSteps <= maxSelectorResolutionSteps else {
            throw invalidRange("KeySelector resolution work limit exceeded")
        }
    }

    private func maxBoundary(
        _ left: ResolvedBoundary?,
        _ right: ResolvedBoundary?
    ) -> ResolvedBoundary? {
        switch (left, right) {
        case (.none, _): return right
        case (_, .none): return left
        case (.some(.key(let leftKey)), .some(.key(let rightKey))):
            return CloudflareDurableObjectByteOrdering.compare(
                leftKey,
                rightKey
            ) >= 0 ? left : right
        default:
            return left
        }
    }

    private func minBoundary(
        _ left: ResolvedBoundary?,
        _ right: ResolvedBoundary?
    ) -> ResolvedBoundary? {
        switch (left, right) {
        case (.none, _): return right
        case (_, .none): return left
        case (.some(.key(let leftKey)), .some(.key(let rightKey))):
            return CloudflareDurableObjectByteOrdering.compare(
                leftKey,
                rightKey
            ) <= 0 ? left : right
        default:
            return left
        }
    }

    private func rangeIsEmpty(
        begin: ResolvedBoundary,
        end: ResolvedBoundary
    ) -> Bool {
        switch (begin, end) {
        case (.pastEnd, _), (_, .beforeAll):
            return true
        case (.key(let beginKey), .key(let endKey)):
            return CloudflareDurableObjectByteOrdering.compare(
                beginKey,
                endKey
            ) >= 0
        default:
            return false
        }
    }

    private func contains(
        _ key: ByteString,
        begin: ResolvedBoundary,
        end: ResolvedBoundary
    ) -> Bool {
        let afterBegin: Bool
        switch begin {
        case .beforeAll: afterBegin = true
        case .pastEnd: afterBegin = false
        case .key(let beginKey):
            afterBegin =
                CloudflareDurableObjectByteOrdering.compare(
                    key,
                    beginKey
                ) >= 0
        }
        let beforeEnd: Bool
        switch end {
        case .beforeAll: beforeEnd = false
        case .pastEnd: beforeEnd = true
        case .key(let endKey):
            beforeEnd =
                CloudflareDurableObjectByteOrdering.compare(
                    key,
                    endKey
                ) < 0
        }
        return afterBegin && beforeEnd
    }

    private func selector(
        for boundary: ResolvedBoundary
    ) -> StorageWireRangeBoundary {
        switch boundary {
        case .beforeAll, .pastEnd:
            return .unbounded
        case .key(let key):
            return .selector(
                StorageWireKeySelector(
                    .firstGreaterOrEqual(key)
                )
            )
        }
    }

    private mutating func ensureHostRowIfNeeded() async throws {
        while hostIndex >= hostRows.count && !finishedHostPages {
            try await loadNextHostPage()
        }
    }

    private mutating func loadNextHostPage() async throws {
        // The ordering key survives release of the previous page. Detaching it
        // avoids retaining that complete response frame between page requests.
        lastEmittedKey = lastEmittedKey?.detached()
        hostRows.removeAll(keepingCapacity: false)
        hostIndex = 0
        let response = try await rangeResponse(
            begin: hostBegin,
            end: hostEnd,
            limit: pageLimit,
            reverse: reverse,
            cursorKey: cursorKey
        )
        try validateHostPageOrder(response.rows)
        try updateCursor(hasMore: response.hasMore, rows: response.rows)
        var rows: [(ByteString, ByteString)] = []
        rows.reserveCapacity(response.rows.count)
        for row in response.rows {
            let key = row.key
            guard !containsLocalRow(for: key) else { continue }
            if let value = try value(for: key, committed: row.value) {
                rows.append((key, value))
            }
        }
        hostRows = rows
        hostIndex = 0
    }

    private func containsLocalRow(for key: ByteString) -> Bool {
        var lowerBound = 0
        var upperBound = allLocalRows.count
        while lowerBound < upperBound {
            let index = lowerBound + (upperBound - lowerBound) / 2
            let comparison = CloudflareDurableObjectByteOrdering.compare(
                allLocalRows[index].0,
                key
            )
            if comparison < 0 {
                lowerBound = index + 1
            } else if comparison > 0 {
                upperBound = index
            } else {
                return true
            }
        }
        return false
    }

    private mutating func rangeResponse(
        begin: StorageWireRangeBoundary,
        end: StorageWireRangeBoundary,
        limit: Int,
        reverse: Bool,
        cursorKey: ByteString?
    ) async throws -> StorageWireRangeResponse {
        guard let storageAccess else {
            throw invalidRange("Range storage access is unavailable")
        }
        let response = try await storageAccess.range(
            StorageWireRangeRequest(
                scope: scope,
                begin: begin,
                end: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                expectedReadVersion: expectedReadVersionForRequest,
                cursorKey: cursorKey
            )
        )
        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot {
            for conflictRange in response.readConflictRanges {
                try storageAccess.recordReadConflictRange(conflictRange)
            }
        }
        return response
    }

    private mutating func updateCursor(
        hasMore: Bool,
        rows: [StorageWireKeyValue]
    ) throws {
        guard hasMore else {
            cursorKey = nil
            finishedHostPages = true
            return
        }
        guard let lastRow = rows.last else {
            throw StorageError(
                code: .dataCorruption,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Range continuation was returned with an empty page"
            )
        }
        // The continuation survives the current response page and is the only
        // part of that frame needed by the next request.
        cursorKey = lastRow.key.detached()
    }

    private func validateHostPageOrder(
        _ rows: [StorageWireKeyValue]
    ) throws {
        var previous = cursorKey
        for row in rows {
            let key = row.key
            if let previous {
                let comparison = CloudflareDurableObjectByteOrdering.compare(
                    key,
                    previous
                )
                let ordered = reverse ? comparison < 0 : comparison > 0
                guard ordered else {
                    throw StorageError(
                        code: .dataCorruption,
                        operation: .rangeRead,
                        backend: .cloudflareDurableObject,
                        message: "Range page did not advance its key cursor"
                    )
                }
            }
            previous = key
        }
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
                    message: "Range read version changed"
                )
            }
        } else {
            stableReadVersion = version
        }
        guard let storageAccess else {
            throw invalidRange("Range storage access is unavailable")
        }
        try storageAccess.recordReadVersion(version)
    }

    private mutating func readCommittedValue(for key: ByteString) async throws -> ByteString? {
        guard let storageAccess else {
            throw invalidRange("Range storage access is unavailable")
        }
        let response = try await storageAccess.read(
            StorageWireReadRequest(
                scope: scope,
                key: key,
                snapshot: snapshot,
                expectedReadVersion: expectedReadVersionForRequest
            )
        )
        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot {
            try storageAccess.recordReadConflictRange(
                .singleKey(key)
            )
        }
        return response.value
    }

    private func value(for key: ByteString, committed: ByteString?) throws -> ByteString? {
        var value = committed
        for operation in mutations {
            switch operation {
            case .set(let operationKey, let operationValue)
            where operationKey == key:
                value = operationValue
            case .clear(let operationKey)
            where operationKey == key:
                value = nil
            case .clearRange(let begin, let end)
            where CloudflareDurableObjectByteOrdering.compare(
                key,
                begin
            ) >= 0
                && CloudflareDurableObjectByteOrdering.compare(
                    key,
                    end
                ) < 0:
                value = nil
            case .atomic(let operationKey, let parameter, let mutationType)
            where operationKey == key:
                switch try mutationType.mutationType.apply(
                    to: value,
                    param: parameter
                ) {
                case .set(let bytes): value = bytes
                case .clear: value = nil
                case .unchanged: break
                }
            default:
                continue
            }
        }
        return value
    }

}
