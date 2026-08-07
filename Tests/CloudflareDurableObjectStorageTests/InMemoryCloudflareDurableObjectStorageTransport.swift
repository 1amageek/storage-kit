import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import Synchronization

final class InMemoryCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport, Sendable
{
    var callExecution: CloudflareDurableObjectCallExecution {
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

    func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        let request = try decodeStorageTransportRequest(requestBytes)
        let response: StorageWireResponse
        do {
            response = try state.withLock { state in
                try handle(request, state: &state)
            }
        } catch let error as StorageError {
            response =
                .failure(status: statusCode(for: error), message: error.message)
        } catch {
            response =
                .failure(status: .invalidOperation, message: String(describing: error))
        }
        return try encodeStorageTransportResponse(response)
    }

    private func handle(
        _ request: StorageWireRequest,
        state: inout State
    ) throws -> StorageWireResponse {
        switch request {
        case .readiness(let request):
            return .readiness(
                StorageWireReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0,
                    metadataInitialized: true
                )
            )
        case .read(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            return .read(
                StorageWireReadResponse(
                    value: state.rowsByPartitionIdentity[request.partitionIdentity]?[request.key],
                    currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
                )
            )
        case .range(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            let rows = try pageRows(for: request, state: state)
            return .range(
                StorageWireRangeResponse(
                    rows: rows.page,
                    hasMore: rows.hasMore,
                    currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0,
                    readConflictRanges: [conflictRange(for: request, rows: rows.page)]
                )
            )
        case .commit(let request):
            try verifyReadConflicts(
                readVersion: request.observedReadVersion,
                readConflictRanges: request.readConflictRanges,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            var rows = state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            let currentVersion = state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
            let committedVersion = currentVersion + 1
            for mutation in request.mutations {
                let materializedMutation = try materialized(
                    mutation,
                    committedVersion: committedVersion
                )
                recordWriteConflict(
                    materializedMutation,
                    version: committedVersion,
                    partitionIdentity: request.partitionIdentity,
                    state: &state
                )
                try apply(materializedMutation, to: &rows)
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
            return .commit(StorageWireCommitResponse(committedVersion: committedVersion))
        case .rangeSize(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            var total: Int64 = 0
            for (key, value) in state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]
            where StorageWireByteOrdering.compare(key, request.begin) >= 0
                && StorageWireByteOrdering.compare(key, request.end) < 0
            {
                total += Int64(key.count + value.count)
            }
            return .rangeSize(
                StorageWireRangeSizeResponse(
                    byteCount: total,
                    currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
                )
            )
        case .rangeSplitPoints(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                partitionIdentity: request.partitionIdentity,
                state: state
            )
            let rows = (state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:])
                .filter {
                    StorageWireByteOrdering.compare($0.key, request.begin) >= 0
                        && StorageWireByteOrdering.compare($0.key, request.end) < 0
                }
                .sorted {
                    StorageWireByteOrdering.compare($0.key, $1.key) < 0
                }
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
            guard points.count <= StorageWireLimits.cloudflareDurableObject.maxSplitPoints else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Split point result exceeds the protocol limit"
                )
            }
            return .rangeSplitPoints(
                StorageWireRangeSplitPointsResponse(
                    splitPoints: points,
                    currentCommitVersion: state.versionsByPartitionIdentity[request.partitionIdentity] ?? 0
                )
            )
        }
    }

    private func pageRows(
        for request: StorageWireRangeRequest,
        state: State
    ) throws -> (page: [StorageWireKeyValue], hasMore: Bool) {
        var selected = (state.rowsByPartitionIdentity[request.partitionIdentity] ?? [:]).map {
            StorageWireKeyValue(key: $0.key, value: $0.value)
        }
        selected.sort {
            StorageWireByteOrdering.compare($0.key, $1.key) < 0
        }
        let keys = selected.map(\.key)
        let start = resolvedIndex(for: request.begin, in: keys, unbounded: 0)
        let finish = resolvedIndex(
            for: request.end,
            in: keys,
            unbounded: selected.count
        )
        selected = start < finish ? Array(selected[start..<finish]) : []
        if request.reverse {
            selected.reverse()
        }
        var remaining = selected
        if let cursorKey = request.cursorKey {
            remaining = selected.filter {
                let ordering = StorageWireByteOrdering.compare($0.key, cursorKey)
                return request.reverse ? ordering < 0 : ordering > 0
            }
        }
        let pageLimit = request.limit > 0 ? request.limit : selected.count
        let page = Array(remaining.prefix(pageLimit))
        return (page, page.count < remaining.count)
    }

    private func apply(
        _ mutation: StorageWireWriteOperation,
        to rows: inout [ByteString: ByteString]
    ) throws {
        switch mutation {
        case .set(let key, let value):
            rows[key] = value
        case .clear(let key):
            rows.removeValue(forKey: key)
        case .clearRange(let begin, let end):
            for key in Array(rows.keys)
            where StorageWireByteOrdering.compare(key, begin) >= 0
                && StorageWireByteOrdering.compare(key, end) < 0
            {
                rows.removeValue(forKey: key)
            }
        case .atomic(let key, let param, let mutationType):
            switch try mutationType.mutationType.apply(
                to: rows[key],
                param: param
            ) {
            case .set(let value):
                rows[key] = value
            case .clear:
                rows.removeValue(forKey: key)
            case .unchanged:
                break
            }
        }
    }

    private func resolvedIndex(
        for boundary: StorageWireRangeBoundary,
        in keys: [ByteString],
        unbounded: Int
    ) -> Int {
        switch boundary {
        case .unbounded:
            return unbounded
        case .selector(let selector):
            return KeySelector(
                key: selector.key,
                orEqual: selector.orEqual,
                offset: selector.offset
            ).resolve(in: keys)
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
        var result = operand.withUnsafeBytes { source in
            Array(source[0..<payloadCount])
        }
        result.replaceSubrange(
            offset..<(offset + 10),
            with: versionstamp(for: committedVersion)
        )
        return ByteString(result)
    }

    private func versionstamp(
        for committedVersion: Int64
    ) -> ByteString {
        let version = UInt64(committedVersion)
        return [
            UInt8(truncatingIfNeeded: version >> 56),
            UInt8(truncatingIfNeeded: version >> 48),
            UInt8(truncatingIfNeeded: version >> 40),
            UInt8(truncatingIfNeeded: version >> 32),
            UInt8(truncatingIfNeeded: version >> 24),
            UInt8(truncatingIfNeeded: version >> 16),
            UInt8(truncatingIfNeeded: version >> 8),
            UInt8(truncatingIfNeeded: version),
            0,
            0,
        ]
    }

    private func verifyReadVersion(
        _ expectedReadVersion: Int64?,
        partitionIdentity: StoragePartitionIdentity,
        state: State
    ) throws {
        guard let expectedReadVersion else { return }
        let currentVersion = state.versionsByPartitionIdentity[partitionIdentity] ?? 0
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
            StorageWireByteOrdering.compare(begin, end) < 0
        else {
            return
        }
        state.conflictsByPartitionIdentity[partitionIdentity, default: []].append(
            ConflictEntry(version: version, begin: begin, end: end)
        )
    }

    private func writeConflictRange(
        for mutation: StorageWireWriteOperation
    ) -> (begin: ByteString, end: ByteString)? {
        switch mutation {
        case .set(let key, _), .clear(let key), .atomic(let key, _, _):
            return singleKeyRange(key)
        case .clearRange(let begin, let end):
            guard StorageWireByteOrdering.compare(begin, end) < 0 else {
                return nil
            }
            return (begin, end)
        }
    }

    private func singleKeyRange(
        _ key: ByteString
    ) -> (begin: ByteString, end: ByteString) {
        (key, keySuccessor(key))
    }

    private func overlaps(_ conflict: ConflictEntry, _ readRange: StorageWireKeyRange) -> Bool {
        if let readEnd = readRange.end,
            StorageWireByteOrdering.compare(conflict.begin, readEnd) >= 0
        {
            return false
        }
        if let readBegin = readRange.begin,
            StorageWireByteOrdering.compare(conflict.end, readBegin) <= 0
        {
            return false
        }
        return true
    }

    private func conflictRange(
        for request: StorageWireRangeRequest,
        rows: [StorageWireKeyValue]
    ) -> StorageWireKeyRange {
        let requestedBegin = boundaryKey(request.begin)
        let requestedEnd = boundaryKey(request.end)
        if let requestedBegin,
            let requestedEnd,
            StorageWireByteOrdering.compare(requestedBegin, requestedEnd) < 0
        {
            return StorageWireKeyRange(
                begin: requestedBegin,
                end: requestedEnd
            )
        }

        let orderedKeys = rows.map(\.key).sorted {
            StorageWireByteOrdering.compare($0, $1) < 0
        }
        let begin = minimumKey(requestedBegin, orderedKeys.first)
        let end = maximumKey(
            requestedEnd.map(keySuccessor),
            orderedKeys.last.map(keySuccessor)
        )
        return StorageWireKeyRange(begin: begin, end: end)
    }

    private func boundaryKey(
        _ boundary: StorageWireRangeBoundary
    ) -> ByteString? {
        switch boundary {
        case .unbounded:
            return nil
        case .selector(let selector):
            return selector.key
        }
    }

    private func minimumKey(
        _ left: ByteString?,
        _ right: ByteString?
    ) -> ByteString? {
        guard let left else { return right }
        guard let right else { return left }
        return StorageWireByteOrdering.compare(left, right) <= 0 ? left : right
    }

    private func maximumKey(
        _ left: ByteString?,
        _ right: ByteString?
    ) -> ByteString? {
        guard let left else { return right }
        guard let right else { return left }
        return StorageWireByteOrdering.compare(left, right) >= 0 ? left : right
    }

    private func keySuccessor(_ key: ByteString) -> ByteString {
        ByteString.copying(count: key.count + 1) { destination in
            key.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[0..<source.count]
                ).copyMemory(from: source)
            }
            destination[key.count] = 0
        }
    }

    private func statusCode(
        for error: StorageError
    ) -> StorageWireFailureStatus {
        switch error.code {
        case .transactionConflict:
            return .transactionConflict
        case .invalidOperation:
            return .invalidOperation
        case .resourceUnavailable:
            return .resourceUnavailable
        case .backendContractViolation:
            return .backendContractViolation
        default:
            return .backendFailure
        }
    }
}
