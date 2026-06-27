import CloudflareDurableObjectStorage
import StorageKit
import Synchronization

final class FakeCloudflareDurableObjectStorageClient: CloudflareDurableObjectStorageClient, Sendable {
    private struct State: Sendable {
        var rowsByScope: [CloudflareDurableObjectStorageScope: [Bytes: Bytes]] = [:]
        var versionsByScope: [CloudflareDurableObjectStorageScope: Int64] = [:]
        var conflictsByScope: [CloudflareDurableObjectStorageScope: [ConflictEntry]] = [:]
    }

    private struct ConflictEntry: Sendable {
        let version: Int64
        let begin: Bytes
        let end: Bytes
    }

    private let state = Mutex(State())
    private let onCommit: (@Sendable () -> Void)?
    private let onRangeResponse: (@Sendable (CloudflareDurableObjectRangeRequest) throws -> Void)?
    private let rangeResponseOverride:
        (@Sendable (CloudflareDurableObjectRangeRequest) throws -> CloudflareDurableObjectRangeResponse?)?

    init(
        onCommit: (@Sendable () -> Void)? = nil,
        onRangeResponse: (@Sendable (CloudflareDurableObjectRangeRequest) throws -> Void)? = nil,
        rangeResponseOverride:
            (@Sendable (CloudflareDurableObjectRangeRequest) throws -> CloudflareDurableObjectRangeResponse?)? = nil
    ) {
        self.onCommit = onCommit
        self.onRangeResponse = onRangeResponse
        self.rangeResponseOverride = rangeResponseOverride
    }

    func read(_ request: CloudflareDurableObjectReadRequest) async throws -> CloudflareDurableObjectReadResponse {
        try state.withLock { state in
            try verifyReadVersion(request.expectedReadVersion, scope: request.scope, state: state)
            let rows = state.rowsByScope[request.scope] ?? [:]
            return CloudflareDurableObjectReadResponse(
                value: rows[request.key.rawValue].map(CloudflareDurableObjectBytes.init),
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0
            )
        }
    }

    func range(_ request: CloudflareDurableObjectRangeRequest) async throws -> CloudflareDurableObjectRangeResponse {
        if let override = try rangeResponseOverride?(request) {
            try onRangeResponse?(request)
            return override
        }
        let response = try state.withLock { state in
            try verifyReadVersion(request.expectedReadVersion, scope: request.scope, state: state)
            let rows = state.rowsByScope[request.scope] ?? [:]
            let sortedRows = rows
                .map { (key: $0.key, value: $0.value) }
                .sorted { compare($0.key, $1.key) < 0 }
            let keys = sortedRows.map(\.key)
            let startIndex = request.begin.storageKitSelector.resolve(in: keys)
            let endIndex = request.end.storageKitSelector.resolve(in: keys)

            var selected: [(key: Bytes, value: Bytes)] = []
            if startIndex < endIndex {
                selected = Array(sortedRows[startIndex..<endIndex])
            }
            if request.reverse {
                selected.reverse()
            }
            let offset = try decodedCursor(request.cursor)
            let limit = request.limit > 0 ? request.limit : selected.count
            let page = Array(selected.dropFirst(offset).prefix(limit))
            let nextOffset = offset + page.count
            let nextCursor = nextOffset < selected.count ? String(nextOffset) : nil

            return CloudflareDurableObjectRangeResponse(
                rows: page.map {
                    CloudflareDurableObjectKeyValue(
                        key: CloudflareDurableObjectBytes($0.key),
                        value: CloudflareDurableObjectBytes($0.value)
                    )
                },
                nextCursor: nextCursor,
                currentCommitVersion: state.versionsByScope[request.scope] ?? 0,
                conflictRange: conflictRange(for: request)
            )
        }
        try onRangeResponse?(request)
        return response
    }

    func commit(_ request: CloudflareDurableObjectCommitRequest) async throws -> CloudflareDurableObjectCommitResponse {
        try commitForTesting(request)
    }

    func commitForTesting(_ request: CloudflareDurableObjectCommitRequest) throws -> CloudflareDurableObjectCommitResponse {
        try state.withLock { state in
            try verifyReadConflicts(
                readVersion: request.observedReadVersion,
                readConflictRanges: request.readConflictRanges,
                scope: request.scope,
                state: state
            )
            onCommit?()
            var rows = state.rowsByScope[request.scope] ?? [:]
            let committedVersion = (state.versionsByScope[request.scope] ?? 0) + 1

            for mutation in request.mutations {
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

            state.rowsByScope[request.scope] = rows
            state.versionsByScope[request.scope] = committedVersion
            return CloudflareDurableObjectCommitResponse(committedVersion: committedVersion)
        }
    }

    func readiness(_ request: CloudflareDurableObjectReadinessRequest) async throws -> CloudflareDurableObjectReadinessResponse {
        state.withLock { state in
            CloudflareDurableObjectReadinessResponse(
                schemaVersion: 1,
                commitVersion: state.versionsByScope[request.scope] ?? 0,
                metadataInitialized: state.rowsByScope[request.scope] != nil
            )
        }
    }

    private func verifyReadVersion(
        _ expectedReadVersion: Int64?,
        scope: CloudflareDurableObjectStorageScope,
        state: State
    ) throws {
        guard let expectedReadVersion else { return }
        let currentVersion = state.versionsByScope[scope] ?? 0
        guard currentVersion == expectedReadVersion else {
            throw StorageError(
                code: .transactionConflict,
                operation: .commit,
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

    private func writeConflictRange(for mutation: CloudflareDurableObjectMutation) -> (begin: Bytes, end: Bytes)? {
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

    private func singleKeyRange(_ key: Bytes) -> (begin: Bytes, end: Bytes) {
        (key, key + [0x00])
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
            begin: CloudflareDurableObjectBytes(request.begin.storageKitSelector.key),
            end: CloudflareDurableObjectBytes(request.end.storageKitSelector.key)
        )
    }

    private func decodedCursor(_ cursor: String?) throws -> Int {
        guard let cursor else { return 0 }
        guard let offset = Int(cursor), offset >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Invalid range cursor"
            )
        }
        return offset
    }

    private func compare(_ lhs: Bytes, _ rhs: Bytes) -> Int {
        let minCount = min(lhs.count, rhs.count)
        var index = 0
        while index < minCount {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
            index += 1
        }
        if lhs.count == rhs.count {
            return 0
        }
        return lhs.count < rhs.count ? -1 : 1
    }
}
