import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import Synchronization

public final class InMemoryCloudflareDurableObjectStorageClient:
    CloudflareDurableObjectStorageClient,
    Sendable
{
    public var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private struct State: Sendable {
        var rowsByPartitionIdentity: [StoragePartitionIdentity: [ByteString: ByteString]] = [:]
        var versionsByPartitionIdentity: [StoragePartitionIdentity: Int64] = [:]
        var conflictsByPartitionIdentity: [StoragePartitionIdentity: [ConflictEntry]] = [:]
    }

    private struct ConflictEntry: Sendable {
        let version: Int64
        let begin: ByteString
        let end: ByteString
    }

    private let state = Mutex(State())
    private let onCommit: (@Sendable () -> Void)?
    private let onRangeResponse: (@Sendable (StorageWireRangeRequest) throws -> Void)?
    private let rangeResponseOverride:
        (@Sendable (StorageWireRangeRequest) throws -> StorageWireRangeResponse?)?
    private let rangeSplitPointsResponseOverride:
        (
            @Sendable (StorageWireRangeSplitPointsRequest) throws ->
                StorageWireRangeSplitPointsResponse?
        )?
    private let commitResponseOverride:
        (@Sendable (StorageWireCommitRequest) throws -> StorageWireCommitResponse?)?

    public init(
        onCommit: (@Sendable () -> Void)? = nil,
        onRangeResponse: (@Sendable (StorageWireRangeRequest) throws -> Void)? = nil,
        rangeResponseOverride:
            (@Sendable (StorageWireRangeRequest) throws -> StorageWireRangeResponse?)? = nil,
        rangeSplitPointsResponseOverride:
            (
                @Sendable (StorageWireRangeSplitPointsRequest) throws ->
                    StorageWireRangeSplitPointsResponse?
            )? = nil,
        commitResponseOverride:
            (@Sendable (StorageWireCommitRequest) throws -> StorageWireCommitResponse?)? = nil
    ) {
        self.onCommit = onCommit
        self.onRangeResponse = onRangeResponse
        self.rangeResponseOverride = rangeResponseOverride
        self.rangeSplitPointsResponseOverride = rangeSplitPointsResponseOverride
        self.commitResponseOverride = commitResponseOverride
    }

    public func read(
        _ request: StorageWireReadRequest
    ) async throws -> StorageWireReadResponse {
        return try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            let rows = state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            return StorageWireReadResponse(
                value: rows[request.key],
                currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
            )
        }
    }

    public func range(
        _ request: StorageWireRangeRequest
    ) async throws -> StorageWireRangeResponse {
        if let override = try rangeResponseOverride?(request) {
            try onRangeResponse?(request)
            return override
        }
        let response = try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state,
                operation: .rangeRead
            )
            let rows = state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            let sortedRows =
                rows
                .map { (key: $0.key, value: $0.value) }
                .sorted { compare($0.key, $1.key) < 0 }
            let keys = sortedRows.map(\.key)
            let startIndex = resolvedIndex(
                request.begin,
                in: keys,
                unboundedIndex: 0
            )
            let endIndex = resolvedIndex(
                request.end,
                in: keys,
                unboundedIndex: keys.count
            )

            var selected: [(key: ByteString, value: ByteString)] = []
            if startIndex < endIndex {
                selected = Array(sortedRows[startIndex..<endIndex])
            }
            if request.reverse {
                selected.reverse()
            }
            if let cursorKey = request.cursorKey {
                selected = selected.filter {
                    let ordering = compare($0.key, cursorKey)
                    return request.reverse ? ordering < 0 : ordering > 0
                }
            }
            let limit = request.limit > 0 ? request.limit : selected.count
            let page = Array(selected.prefix(limit))
            return StorageWireRangeResponse(
                rows: page.map {
                    StorageWireKeyValue(
                        key: $0.key,
                        value: $0.value
                    )
                },
                hasMore: page.count < selected.count,
                currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0,
                readConflictRanges: [
                    conflictRange(
                        for: request,
                        keys: keys,
                        startIndex: startIndex,
                        endIndex: endIndex
                    )
                ]
            )
        }
        try onRangeResponse?(request)
        return response
    }

    public func commit(
        _ request: StorageWireCommitRequest
    ) async throws -> StorageWireCommitResponse {
        if let override = try commitResponseOverride?(request) {
            return override
        }
        return try commitForTesting(request)
    }

    public func commitForTesting(
        _ request: StorageWireCommitRequest
    ) throws -> StorageWireCommitResponse {
        try state.withLock { state in
            try verifyReadConflicts(
                readVersion: request.observedReadVersion,
                readConflictRanges: request.readConflictRanges,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            onCommit?()
            var rows = state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            let currentVersion = state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
            let committedVersion = currentVersion + 1

            let materializedMutations = try request.mutations.map {
                try materialized($0, committedVersion: committedVersion)
            }
            for mutation in materializedMutations {
                recordWriteConflict(
                    mutation, version: committedVersion, partitionIdentity: request.partitionIdentity, state: &state)
                switch mutation {
                case .set(let key, let value):
                    rows[key] = value
                case .clear(let key):
                    rows.removeValue(forKey: key)
                case .clearRange(let begin, let end):
                    for key in Array(rows.keys)
                    where compare(key, begin) >= 0 && compare(key, end) < 0 {
                        rows.removeValue(forKey: key)
                    }
                case .atomic(let key, let param, let mutationType):
                    switch try mutationType.mutationType.apply(to: rows[key], param: param) {
                    case .set(let bytes):
                        rows[key] = bytes
                    case .clear:
                        rows.removeValue(forKey: key)
                    case .unchanged:
                        break
                    }
                }
            }
            for range in request.writeConflictRanges {
                recordWriteConflict(
                    range,
                    version: committedVersion,
                    partitionIdentity: request.partitionIdentity,
                    state: &state
                )
            }

            state.rowsByPartitionIdentity[request.partitionIdentity] = rows
            state.versionsByPartitionIdentity[request.partitionIdentity] = committedVersion
            return StorageWireCommitResponse(committedVersion: committedVersion)
        }
    }

    public func readiness(
        _ request: StorageWireReadinessRequest
    ) async throws -> StorageWireReadinessResponse {
        state.withLock { state in
            StorageWireReadinessResponse(
                schemaVersion: 1,
                commitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0,
                metadataInitialized: true
            )
        }
    }

    public func rangeSize(
        _ request: StorageWireRangeSizeRequest
    ) async throws -> StorageWireRangeSizeResponse {
        try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state,
                operation: .rangeRead
            )
            var total: Int64 = 0
            for (key, value) in state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            where compare(key, request.begin) >= 0
                && compare(key, request.end) < 0
            {
                total += Int64(key.count + value.count)
            }
            return StorageWireRangeSizeResponse(
                byteCount: total,
                currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
            )
        }
    }

    public func rangeSplitPoints(
        _ request: StorageWireRangeSplitPointsRequest
    ) async throws -> StorageWireRangeSplitPointsResponse {
        if let override = try rangeSplitPointsResponseOverride?(request) {
            return override
        }
        return try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state,
                operation: .rangeRead
            )
            guard request.chunkSize > 0 else {
                throw StorageError.invalidOperation(
                    "Split point chunk size must be positive"
                )
            }
            let rows = (state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:])
                .filter {
                    compare($0.key, request.begin) >= 0
                        && compare($0.key, request.end) < 0
                }
                .sorted { compare($0.key, $1.key) < 0 }
            var points = [request.begin]
            var chunkBytes: Int64 = 0
            for row in rows {
                let rowSize = Int64(row.key.count + row.value.count)
                if chunkBytes > 0,
                    rowSize > request.chunkSize
                        - min(
                            chunkBytes,
                            request.chunkSize
                        )
                {
                    points.append(row.key)
                    chunkBytes = 0
                }
                chunkBytes += rowSize
            }
            if request.begin != request.end {
                points.append(request.end)
            }
            guard points.count <= 10_000 else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Split point result exceeds the protocol limit"
                )
            }
            return StorageWireRangeSplitPointsResponse(
                splitPoints: points,
                currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
            )
        }
    }

    private func verifyReadVersion(
        _ expectedReadVersion: Int64?,
        partitionIdentity: StoragePartitionIdentity,
        state: State,
        operation: StorageOperation = .read
    ) throws {
        guard let expectedReadVersion else { return }
        let currentVersion = state.versionsByPartitionIdentity[partitionIdentity] ?? 0
        guard currentVersion == expectedReadVersion else {
            throw StorageError(
                code: .transactionConflict,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Observed read version does not match current committed version"
            )
        }
    }

    private func verifyReadConflicts(
        readVersion: Int64?,
        readConflictRanges: [StorageWireKeyRange],
        partitionIdentity: StoragePartitionIdentity,
        state: State
    ) throws {
        guard let readVersion else { return }
        let conflicts = state.conflictsByPartitionIdentity[partitionIdentity] ?? []
        for readRange in readConflictRanges {
            for conflict in conflicts
            where conflict.version > readVersion && overlaps(conflict, readRange) {
                throw StorageError(
                    code: .transactionConflict,
                    operation: .commit,
                    backend: .cloudflareDurableObject,
                    message: "Read conflict range was modified after the transaction read version"
                )
            }
        }
    }

    private func recordWriteConflict(
        _ mutation: StorageWireWriteOperation,
        version: Int64,
        partitionIdentity: StoragePartitionIdentity,
        state: inout State
    ) {
        guard let range = writeConflictRange(for: mutation) else {
            return
        }
        state.conflictsByPartitionIdentity[partitionIdentity, default: []].append(
            ConflictEntry(version: version, begin: range.begin, end: range.end)
        )
    }

    private func recordWriteConflict(
        _ range: StorageWireKeyRange,
        version: Int64,
        partitionIdentity: StoragePartitionIdentity,
        state: inout State
    ) {
        guard let begin = range.begin,
            let end = range.end,
            compare(begin, end) < 0
        else {
            return
        }
        state.conflictsByPartitionIdentity[partitionIdentity, default: []].append(
            ConflictEntry(version: version, begin: begin, end: end)
        )
    }

    private func writeConflictRange(for mutation: StorageWireWriteOperation) -> (
        begin: ByteString, end: ByteString
    )? {
        switch mutation {
        case .set(let key, _), .clear(let key), .atomic(let key, _, _):
            return singleKeyRange(key)
        case .clearRange(let begin, let end):
            guard compare(begin, end) < 0 else {
                return nil
            }
            return (begin, end)
        }
    }

    private func materialized(
        _ mutation: StorageWireWriteOperation,
        committedVersion: Int64
    ) throws -> StorageWireWriteOperation {
        guard case .atomic(let key, let param, let mutationType) = mutation else {
            return mutation
        }
        switch mutationType {
        case .setVersionstampedKey:
            return .set(
                key: try materializedVersionstampOperand(
                    key,
                    committedVersion: committedVersion,
                    maximumResultBytes: 1_024
                ),
                value: param
            )
        case .setVersionstampedValue:
            return .set(
                key: key,
                value: try materializedVersionstampOperand(
                    param,
                    committedVersion: committedVersion,
                    maximumResultBytes: 1_048_576
                )
            )
        default:
            return mutation
        }
    }

    private func materializedVersionstampOperand(
        _ operand: ByteString,
        committedVersion: Int64,
        maximumResultBytes: Int
    ) throws -> ByteString {
        guard operand.count >= 14 else {
            throw StorageError.invalidOperation(
                "Versionstamp operand is shorter than fourteen bytes"
            )
        }
        let payloadCount = operand.count - 4
        guard payloadCount <= maximumResultBytes else {
            throw StorageError.invalidOperation(
                "Materialized versionstamp exceeds the byte limit"
            )
        }
        let offset = operand.withUnsafeBytes { bytes in
            Int(bytes[payloadCount])
                | (Int(bytes[payloadCount + 1]) << 8)
                | (Int(bytes[payloadCount + 2]) << 16)
                | (Int(bytes[payloadCount + 3]) << 24)
        }
        guard offset <= payloadCount - 10 else {
            throw StorageError.invalidOperation(
                "Versionstamp offset does not identify ten payload bytes"
            )
        }
        let version = UInt64(committedVersion)
        return ByteString.copying(count: payloadCount) { destination in
            operand.withUnsafeBytes { source in
                destination.copyMemory(
                    from: UnsafeRawBufferPointer(
                        rebasing: source[0..<payloadCount]
                    )
                )
            }
            destination[offset] = UInt8(truncatingIfNeeded: version >> 56)
            destination[offset + 1] = UInt8(truncatingIfNeeded: version >> 48)
            destination[offset + 2] = UInt8(truncatingIfNeeded: version >> 40)
            destination[offset + 3] = UInt8(truncatingIfNeeded: version >> 32)
            destination[offset + 4] = UInt8(truncatingIfNeeded: version >> 24)
            destination[offset + 5] = UInt8(truncatingIfNeeded: version >> 16)
            destination[offset + 6] = UInt8(truncatingIfNeeded: version >> 8)
            destination[offset + 7] = UInt8(truncatingIfNeeded: version)
            destination[offset + 8] = 0
            destination[offset + 9] = 0
        }
    }

    private func singleKeyRange(_ key: ByteString) -> (begin: ByteString, end: ByteString) {
        let end = ByteString.copying(count: key.count + 1) { destination in
            key.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            destination[key.count] = 0
        }
        return (key, end)
    }

    private func overlaps(_ conflict: ConflictEntry, _ readRange: StorageWireKeyRange) -> Bool {
        if let readEnd = readRange.end, compare(conflict.begin, readEnd) >= 0 {
            return false
        }
        if let readBegin = readRange.begin, compare(conflict.end, readBegin) <= 0 {
            return false
        }
        return true
    }

    /// Reports the key interval the response actually depended on.
    ///
    /// The host contract requires the union of the selector resolution
    /// dependencies and the effective scan interval, not the raw selector
    /// keys. A selector resolves by comparing stored keys against its own key
    /// and then walking `offset` positions, so it observes every key between
    /// the two, and a walk cut short at the end of the store observes the
    /// absence of every key above it. Raw selector keys lose both facts. They
    /// also collapse an emptiness probe, which arrives as the boundary pair
    /// `[selector, selector + 1)` sharing one key, into an empty range that
    /// reports no dependency at all.
    ///
    /// Widening the reported interval is safe because it can only raise a
    /// spurious conflict, while narrowing it silently drops a real one. Every
    /// adjustment below therefore widens.
    private func conflictRange(
        for request: StorageWireRangeRequest,
        keys: [ByteString],
        startIndex: Int,
        endIndex: Int
    ) -> StorageWireKeyRange {
        var lower: ByteString?
        var upper: ByteString?
        var lowerIsUnbounded = false
        var upperIsUnbounded = false

        func widenLower(to candidate: ByteString) {
            guard let current = lower else {
                lower = candidate
                return
            }
            if compare(candidate, current) < 0 {
                lower = candidate
            }
        }

        func widenUpper(to candidate: ByteString) {
            guard let current = upper else {
                upper = candidate
                return
            }
            if compare(candidate, current) > 0 {
                upper = candidate
            }
        }

        func absorb(_ boundary: StorageWireRangeBoundary, resolvedIndex: Int) {
            switch boundary {
            case .unbounded:
                return
            case .selector(let selector):
                widenLower(to: selector.key)
                widenUpper(to: selector.key)
                if resolvedIndex < keys.count {
                    widenLower(to: keys[resolvedIndex])
                    widenUpper(to: keys[resolvedIndex])
                }
                if walkStoppedAtEndOfStore(selector, in: keys) {
                    upperIsUnbounded = true
                }
            }
        }

        if case .unbounded = request.begin {
            lowerIsUnbounded = true
        }
        if case .unbounded = request.end {
            upperIsUnbounded = true
        }
        absorb(request.begin, resolvedIndex: startIndex)
        absorb(request.end, resolvedIndex: endIndex)

        // The effective scan interval covers every row the response read, so a
        // later deletion of one of those rows has to conflict.
        if startIndex < endIndex {
            widenLower(to: keys[startIndex])
            widenUpper(to: singleKeyRange(keys[endIndex - 1]).end)
        }

        let begin = lowerIsUnbounded ? nil : lower
        var end = upperIsUnbounded ? nil : upper
        if let begin, let resolvedEnd = end, compare(begin, resolvedEnd) >= 0 {
            // A reported range is half-open, so equal bounds would describe an
            // empty dependency. Extend past the single observed key instead.
            end = singleKeyRange(begin).end
        }
        return StorageWireKeyRange(begin: begin, end: end)
    }

    /// Reports whether the selector's offset walk was cut short at the end of
    /// the store rather than settling one position past the last key.
    ///
    /// `KeySelector.resolve(in:)` owns the resolution rule and clamps its
    /// answer to the key count, which hides that difference. Only a walk that
    /// was cut short observed the absence of further keys, so only it carries
    /// an unbounded dependency; a walk that settles exactly one position past
    /// the last key is already described by its own selector key.
    private func walkStoppedAtEndOfStore(
        _ selector: StorageWireKeySelector,
        in keys: [ByteString]
    ) -> Bool {
        let lastMatchingIndex =
            selector.orEqual
            ? upperBound(keys, for: selector.key) - 1
            : lowerBound(keys, for: selector.key) - 1
        return lastMatchingIndex + selector.offset > keys.count
    }

    private func resolvedIndex(
        _ boundary: StorageWireRangeBoundary,
        in keys: [ByteString],
        unboundedIndex: Int
    ) -> Int {
        switch boundary {
        case .unbounded:
            return unboundedIndex
        case .selector(let selector):
            return selector.keySelector.resolve(in: keys)
        }
    }

    private func compare(_ lhs: ByteString, _ rhs: ByteString) -> Int {
        compareBytes(lhs, rhs)
    }
}
