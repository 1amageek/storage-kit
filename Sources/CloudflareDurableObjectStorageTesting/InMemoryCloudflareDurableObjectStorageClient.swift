import DatabaseTypes
import CloudflareDurableObjectStorage
import StorageKit
import Synchronization

public final class InMemoryCloudflareDurableObjectStorageClient:
    CloudflareDurableObjectStorageClient,
    Sendable {
    public var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private struct State: Sendable {
        var rowsByScope: [CloudflareDurableObjectStorageScope: [ByteString: ByteString]] = [:]
        var versionsByScope: [CloudflareDurableObjectStorageScope: Int64] = [:]
        var conflictsByScope: [CloudflareDurableObjectStorageScope: [ConflictEntry]] = [:]
    }

    private struct ConflictEntry: Sendable {
        let version: Int64
        let begin: ByteString
        let end: ByteString
    }

    private let state = Mutex(State())
    private let onCommit: (@Sendable () -> Void)?
    private let onRangeResponse: (@Sendable (CloudflareDurableObjectRangeRequest) throws -> Void)?
    private let rangeResponseOverride:
        (@Sendable (CloudflareDurableObjectRangeRequest) throws -> CloudflareDurableObjectRangeResponse?)?
    private let rangeSplitPointsResponseOverride:
        (@Sendable (CloudflareDurableObjectRangeSplitPointsRequest) throws -> CloudflareDurableObjectRangeSplitPointsResponse?)?
    private let commitResponseOverride:
        (@Sendable (CloudflareDurableObjectCommitRequest) throws -> CloudflareDurableObjectCommitResponse?)?

    public init(
        onCommit: (@Sendable () -> Void)? = nil,
        onRangeResponse: (@Sendable (CloudflareDurableObjectRangeRequest) throws -> Void)? = nil,
        rangeResponseOverride:
            (@Sendable (CloudflareDurableObjectRangeRequest) throws -> CloudflareDurableObjectRangeResponse?)? = nil,
        rangeSplitPointsResponseOverride:
            (@Sendable (CloudflareDurableObjectRangeSplitPointsRequest) throws -> CloudflareDurableObjectRangeSplitPointsResponse?)? = nil,
        commitResponseOverride:
            (@Sendable (CloudflareDurableObjectCommitRequest) throws -> CloudflareDurableObjectCommitResponse?)? = nil
    ) {
        self.onCommit = onCommit
        self.onRangeResponse = onRangeResponse
        self.rangeResponseOverride = rangeResponseOverride
        self.rangeSplitPointsResponseOverride = rangeSplitPointsResponseOverride
        self.commitResponseOverride = commitResponseOverride
    }

    public func read(
        _ request: CloudflareDurableObjectReadRequest
    ) async throws -> CloudflareDurableObjectReadResponse {
        return try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state
            )
            let rows = state.rowsByScope[request.scope] ?? [:]
            return CloudflareDurableObjectReadResponse(
                value: rows[request.key.rawValue].map(CloudflareDurableObjectBytes.init),
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0
            )
        }
    }

    public func range(
        _ request: CloudflareDurableObjectRangeRequest
    ) async throws -> CloudflareDurableObjectRangeResponse {
        if let override = try rangeResponseOverride?(request) {
            try onRangeResponse?(request)
            return override
        }
        let response = try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state,
                operation: .rangeRead
            )
            let rows = state.rowsByScope[request.scope] ?? [:]
            let sortedRows = rows
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
            if let cursorKey = request.cursorKey?.rawValue {
                selected = selected.filter {
                    let ordering = compare($0.key, cursorKey)
                    return request.reverse ? ordering < 0 : ordering > 0
                }
            }
            let limit = request.limit > 0 ? request.limit : selected.count
            let page = Array(selected.prefix(limit))
            return CloudflareDurableObjectRangeResponse(
                rows: page.map {
                    CloudflareDurableObjectKeyValue(
                        key: CloudflareDurableObjectBytes($0.key),
                        value: CloudflareDurableObjectBytes($0.value)
                    )
                },
                hasMore: page.count < selected.count,
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0,
                readConflictRanges: [conflictRange(for: request)]
            )
        }
        try onRangeResponse?(request)
        return response
    }

    public func commit(
        _ request: CloudflareDurableObjectCommitRequest
    ) async throws -> CloudflareDurableObjectCommitResponse {
        if let override = try commitResponseOverride?(request) {
            return override
        }
        return try commitForTesting(request)
    }

    public func commitForTesting(
        _ request: CloudflareDurableObjectCommitRequest
    ) throws -> CloudflareDurableObjectCommitResponse {
        try state.withLock { state in
            try verifyReadConflicts(
                readVersion: request.observedReadVersion,
                readConflictRanges: request.readConflictRanges,
                scope: request.scope,
                state: state
            )
            onCommit?()
            var rows = state.rowsByScope[request.scope] ?? [:]
            let currentVersion = state.versionsByScope[request.scope] ?? 0
            let committedVersion = currentVersion + 1

            let materializedMutations = try request.mutations.map {
                try materialized($0, committedVersion: committedVersion)
            }
            for mutation in materializedMutations {
                recordWriteConflict(mutation, version: committedVersion, scope: request.scope, state: &state)
                switch mutation {
                case .set(let key, let value):
                    rows[key.rawValue] = value.rawValue
                case .clear(let key):
                    rows.removeValue(forKey: key.rawValue)
                case .clearRange(let begin, let end):
                    for key in Array(rows.keys) where compare(key, begin.rawValue) >= 0 && compare(key, end.rawValue) < 0 {
                        rows.removeValue(forKey: key)
                    }
                case .atomic(let key, let param, let mutationType):
                    switch try mutationType.storageKitMutationType.apply(to: rows[key.rawValue], param: param.rawValue) {
                    case .set(let bytes):
                        rows[key.rawValue] = bytes
                    case .clear:
                        rows.removeValue(forKey: key.rawValue)
                    case .unchanged:
                        break
                    }
                }
            }
            for range in request.writeConflictRanges {
                recordWriteConflict(
                    range,
                    version: committedVersion,
                    scope: request.scope,
                    state: &state
                )
            }

            state.rowsByScope[request.scope] = rows
            state.versionsByScope[request.scope] = committedVersion
            return CloudflareDurableObjectCommitResponse(committedVersion: committedVersion)
        }
    }

    public func readiness(
        _ request: CloudflareDurableObjectReadinessRequest
    ) async throws -> CloudflareDurableObjectReadinessResponse {
        state.withLock { state in
            CloudflareDurableObjectReadinessResponse(
                schemaVersion: 1,
                commitVersion: state.versionsByScope[request.scope] ?? 0,
                metadataInitialized: true
            )
        }
    }

    public func rangeSize(
        _ request: CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse {
        try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state,
                operation: .rangeRead
            )
            var total: Int64 = 0
            for (key, value) in state.rowsByScope[request.scope] ?? [:]
                where compare(key, request.begin.rawValue) >= 0
                    && compare(key, request.end.rawValue) < 0 {
                total += Int64(key.count + value.count)
            }
            return CloudflareDurableObjectRangeSizeResponse(
                byteCount: total,
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0
            )
        }
    }

    public func rangeSplitPoints(
        _ request: CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse {
        if let override = try rangeSplitPointsResponseOverride?(request) {
            return override
        }
        return try state.withLock { state in
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state,
                operation: .rangeRead
            )
            guard request.chunkSize > 0 else {
                throw StorageError.invalidOperation(
                    "Split point chunk size must be positive"
                )
            }
            let rows = (state.rowsByScope[request.scope] ?? [:])
                .filter {
                    compare($0.key, request.begin.rawValue) >= 0
                        && compare($0.key, request.end.rawValue) < 0
                }
                .sorted { compare($0.key, $1.key) < 0 }
            var points = [request.begin.rawValue]
            var chunkBytes: Int64 = 0
            for row in rows {
                let rowSize = Int64(row.key.count + row.value.count)
                if chunkBytes > 0,
                   rowSize > request.chunkSize - min(
                    chunkBytes,
                    request.chunkSize
                   ) {
                    points.append(row.key)
                    chunkBytes = 0
                }
                chunkBytes += rowSize
            }
            if request.begin != request.end {
                points.append(request.end.rawValue)
            }
            guard points.count <= 10_000 else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Split point result exceeds the protocol limit"
                )
            }
            return CloudflareDurableObjectRangeSplitPointsResponse(
                splitPoints: points.map(CloudflareDurableObjectBytes.init),
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0
            )
        }
    }

    private func verifyReadVersion(
        _ expectedReadVersion: Int64?,
        scope: CloudflareDurableObjectStorageScope,
        state: State,
        operation: StorageOperation = .read
    ) throws {
        guard let expectedReadVersion else { return }
        let currentVersion = state.versionsByScope[scope] ?? 0
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
        readConflictRanges: [CloudflareDurableObjectConflictRange],
        scope: CloudflareDurableObjectStorageScope,
        state: State
    ) throws {
        guard let readVersion else { return }
        let conflicts = state.conflictsByScope[scope] ?? []
        for readRange in readConflictRanges {
            for conflict in conflicts where conflict.version > readVersion && overlaps(conflict, readRange) {
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
        _ mutation: CloudflareDurableObjectMutation,
        version: Int64,
        scope: CloudflareDurableObjectStorageScope,
        state: inout State
    ) {
        guard let range = writeConflictRange(for: mutation) else {
            return
        }
        state.conflictsByScope[scope, default: []].append(
            ConflictEntry(version: version, begin: range.begin, end: range.end)
        )
    }

    private func recordWriteConflict(
        _ range: CloudflareDurableObjectConflictRange,
        version: Int64,
        scope: CloudflareDurableObjectStorageScope,
        state: inout State
    ) {
        guard let begin = range.begin?.rawValue,
              let end = range.end?.rawValue,
              compare(begin, end) < 0 else {
            return
        }
        state.conflictsByScope[scope, default: []].append(
            ConflictEntry(version: version, begin: begin, end: end)
        )
    }

    private func writeConflictRange(for mutation: CloudflareDurableObjectMutation) -> (begin: ByteString, end: ByteString)? {
        switch mutation {
        case .set(let key, _), .clear(let key), .atomic(let key, _, _):
            return singleKeyRange(key.rawValue)
        case .clearRange(let begin, let end):
            guard compare(begin.rawValue, end.rawValue) < 0 else {
                return nil
            }
            return (begin.rawValue, end.rawValue)
        }
    }

    private func materialized(
        _ mutation: CloudflareDurableObjectMutation,
        committedVersion: Int64
    ) throws -> CloudflareDurableObjectMutation {
        guard case .atomic(let key, let param, let mutationType) = mutation else {
            return mutation
        }
        switch mutationType {
        case .setVersionstampedKey:
            return .set(
                key: CloudflareDurableObjectBytes(
                    try materializedVersionstampOperand(
                        key.rawValue,
                        committedVersion: committedVersion,
                        maximumResultBytes: 1_024
                    )
                ),
                value: param
            )
        case .setVersionstampedValue:
            return .set(
                key: key,
                value: CloudflareDurableObjectBytes(
                    try materializedVersionstampOperand(
                        param.rawValue,
                        committedVersion: committedVersion,
                        maximumResultBytes: 1_048_576
                    )
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

    private func overlaps(_ conflict: ConflictEntry, _ readRange: CloudflareDurableObjectConflictRange) -> Bool {
        if let readEnd = readRange.end, compare(conflict.begin, readEnd.rawValue) >= 0 {
            return false
        }
        if let readBegin = readRange.begin, compare(conflict.end, readBegin.rawValue) <= 0 {
            return false
        }
        return true
    }

    private func conflictRange(for request: CloudflareDurableObjectRangeRequest) -> CloudflareDurableObjectConflictRange {
        CloudflareDurableObjectConflictRange(
            begin: boundaryKey(request.begin).map(
                CloudflareDurableObjectBytes.init
            ),
            end: boundaryKey(request.end).map(
                CloudflareDurableObjectBytes.init
            )
        )
    }

    private func resolvedIndex(
        _ boundary: CloudflareDurableObjectRangeBoundary,
        in keys: [ByteString],
        unboundedIndex: Int
    ) -> Int {
        switch boundary {
        case .unbounded:
            return unboundedIndex
        case .selector(let selector):
            return selector.storageKitSelector.resolve(in: keys)
        }
    }

    private func boundaryKey(
        _ boundary: CloudflareDurableObjectRangeBoundary
    ) -> ByteString? {
        switch boundary {
        case .unbounded:
            return nil
        case .selector(let selector):
            return selector.key.rawValue
        }
    }

    private func compare(_ lhs: ByteString, _ rhs: ByteString) -> Int {
        compareBytes(lhs, rhs)
    }
}
