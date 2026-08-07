import DatabaseTypes

enum StorageWireValidator {
    static func validate(
        _ request: StorageWireRequest
    ) throws(StorageWireProtocolError) {
        var counter = Counter()
        try counter.add(2)
        switch request {
        case .readiness(let request):
            try counter.add(request.partitionIdentity)
        case .read(let request):
            try counter.add(request.partitionIdentity)
            try counter.addBytes(request.key, maximum: limits.maxKeyBytes)
            try counter.add(1)
            try counter.addOptionalVersion(request.expectedReadVersion)
        case .range(let request):
            try counter.add(request.partitionIdentity)
            try counter.add(request.begin)
            try counter.add(request.end)
            try counter.add(6)
            try counter.addOptionalVersion(request.expectedReadVersion)
            try counter.addOptionalBytes(
                request.cursorKey,
                maximum: limits.maxKeyBytes
            )
        case .commit(let request):
            try counter.add(request.partitionIdentity)
            try counter.addOptionalVersion(request.observedReadVersion)
            try counter.addCollectionHeader(
                count: request.mutations.count,
                maximum: limits.maxMutationsPerCommit
            )
            for mutation in request.mutations {
                try counter.add(mutation)
            }
            try counter.addCollectionHeader(
                count: request.readConflictRanges.count,
                maximum: limits.maxConflictRangesPerCommit
            )
            for range in request.readConflictRanges {
                try counter.add(range)
            }
            try counter.addCollectionHeader(
                count: request.writeConflictRanges.count,
                maximum: limits.maxConflictRangesPerCommit
            )
            for range in request.writeConflictRanges {
                try counter.add(range)
            }
        case .rangeSize(let request):
            try counter.add(request.partitionIdentity)
            try counter.addBytes(
                request.begin,
                maximum: limits.maxBoundaryBytes
            )
            try counter.addBytes(
                request.end,
                maximum: limits.maxBoundaryBytes
            )
            try counter.addOptionalVersion(request.expectedReadVersion)
        case .rangeSplitPoints(let request):
            try counter.add(request.partitionIdentity)
            try counter.addBytes(
                request.begin,
                maximum: limits.maxBoundaryBytes
            )
            try counter.addBytes(
                request.end,
                maximum: limits.maxBoundaryBytes
            )
            try counter.add(8)
            try counter.addOptionalVersion(request.expectedReadVersion)
        }
    }

    static func validate(
        _ response: StorageWireResponse
    ) throws(StorageWireProtocolError) {
        var counter = Counter()
        try counter.add(2)
        switch response {
        case .readiness:
            try counter.add(14)
        case .read(let response):
            try counter.add(2)
            if let value = response.value {
                try counter.addBytes(value, maximum: limits.maxValueBytes)
            }
            try counter.add(8)
        case .range(let response):
            try counter.add(1)
            try counter.addCollectionHeader(
                count: response.rows.count,
                maximum: limits.maxRangeLimit
            )
            for row in response.rows {
                try counter.validateStoredPair(
                    keyBytes: row.key.count,
                    valueBytes: row.value.count
                )
                try counter.addBytes(row.key, maximum: limits.maxKeyBytes)
                try counter.addBytes(row.value, maximum: limits.maxValueBytes)
            }
            guard !response.hasMore || !response.rows.isEmpty else {
                throw .wire(.invalidRangeContinuation)
            }
            try counter.add(1)
            try counter.add(8)
            try counter.addCollectionHeader(
                count: response.readConflictRanges.count,
                maximum: limits.maxConflictRangesPerCommit
            )
            for range in response.readConflictRanges {
                try counter.add(range)
            }
        case .commit:
            try counter.add(9)
        case .rangeSize:
            try counter.add(17)
        case .rangeSplitPoints(let response):
            try counter.add(1)
            try counter.addCollectionHeader(
                count: response.splitPoints.count,
                maximum: limits.maxSplitPoints
            )
            for point in response.splitPoints {
                try counter.addBytes(
                    point,
                    maximum: limits.maxBoundaryBytes
                )
            }
            try counter.add(8)
        case .failure(_, let message):
            try counter.addString(message, maximum: limits.maxErrorMessageBytes)
        }
    }

    static func validateFrameBytes<ByteString: Collection>(
        _ bytes: ByteString
    ) throws(StorageWireProtocolError) where ByteString.Element == UInt8 {
        guard bytes.count <= limits.maxFrameBytes else {
            throw .wire(
                .byteCountExceedsLimit(
                    count: bytes.count,
                    maximum: limits.maxFrameBytes
                )
            )
        }
    }

    private static let limits = StorageWireLimits.cloudflareDurableObject

    private struct Counter {
        private var count = 0

        mutating func add(_ byteCount: Int) throws(StorageWireProtocolError) {
            guard byteCount >= 0,
                  count <= StorageWireLimits.cloudflareDurableObject.maxFrameBytes - byteCount else {
                let attemptedCount = count.addingReportingOverflow(byteCount)
                throw .wire(
                    .byteCountExceedsLimit(
                        count: attemptedCount.overflow ? Int.max : attemptedCount.partialValue,
                        maximum: StorageWireLimits.cloudflareDurableObject.maxFrameBytes
                    )
                )
            }
            count += byteCount
        }

        mutating func add(
            _ partitionIdentity: StoragePartitionIdentity
        ) throws(StorageWireProtocolError) {
            try addString(partitionIdentity.databaseID, maximum: StorageWireLimits.cloudflareDurableObject.maxPartitionIdentityComponentBytes)
            try addOptionalString(
                partitionIdentity.tenantID,
                maximum: StorageWireLimits.cloudflareDurableObject.maxPartitionIdentityComponentBytes
            )
            try addOptionalString(
                partitionIdentity.workspaceID,
                maximum: StorageWireLimits.cloudflareDurableObject.maxPartitionIdentityComponentBytes
            )
        }

        mutating func add(
            _ boundary: StorageWireRangeBoundary
        ) throws(StorageWireProtocolError) {
            try add(1)
            guard case .selector(let selector) = boundary else {
                return
            }
            try addBytes(selector.key, maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes)
            try add(9)
        }

        mutating func add(
            _ mutation: StorageWireWriteOperation
        ) throws(StorageWireProtocolError) {
            try add(1)
            switch mutation {
            case .set(let key, let value):
                try addBytes(key, maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes)
                try addBytes(value, maximum: StorageWireLimits.cloudflareDurableObject.maxValueBytes)
                try validateStoredPair(
                    keyBytes: key.count,
                    valueBytes: value.count
                )
            case .clear(let key):
                try addBytes(key, maximum: StorageWireLimits.cloudflareDurableObject.maxKeyBytes)
            case .clearRange(let begin, let end):
                try addBytes(
                    begin,
                    maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
                )
                try addBytes(
                    end,
                    maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
                )
            case .atomic(let key, let param, let mutationType):
                if mutationType == .setVersionstampedKey, key.count >= 4 {
                    try validateStoredPair(
                        keyBytes: key.count - 4,
                        valueBytes: param.count
                    )
                } else if mutationType == .setVersionstampedValue,
                          param.count >= 4 {
                    try validateStoredPair(
                        keyBytes: key.count,
                        valueBytes: param.count - 4
                    )
                } else if mutationType != .compareAndClear {
                    try validateStoredPair(
                        keyBytes: key.count,
                        valueBytes: param.count
                    )
                }
                try addBytes(
                    key,
                    maximum: mutationType == .setVersionstampedKey
                        ? StorageWireLimits.cloudflareDurableObject
                            .maxVersionstampedKeyOperandBytes
                        : StorageWireLimits.cloudflareDurableObject.maxKeyBytes
                )
                try addBytes(
                    param,
                    maximum: mutationType == .setVersionstampedValue
                        ? StorageWireLimits.cloudflareDurableObject
                            .maxVersionstampedValueOperandBytes
                        : StorageWireLimits.cloudflareDurableObject.maxValueBytes
                )
                try add(1)
            }
        }

        mutating func add(
            _ range: StorageWireKeyRange
        ) throws(StorageWireProtocolError) {
            try addOptionalBytes(
                range.begin,
                maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
            )
            try addOptionalBytes(
                range.end,
                maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
            )
        }

        mutating func addBytes(
            _ bytes: ByteString,
            maximum: Int
        ) throws(StorageWireProtocolError) {
            guard bytes.count <= maximum else {
                throw .wire(
                    .byteCountExceedsLimit(
                        count: bytes.count,
                        maximum: maximum
                    )
                )
            }
            try add(4)
            try add(bytes.count)
        }

        func validateStoredPair(
            keyBytes: Int,
            valueBytes: Int
        ) throws(StorageWireProtocolError) {
            let total = keyBytes.addingReportingOverflow(valueBytes)
            let maximum = StorageWireLimits.cloudflareDurableObject
                .maxStoredKeyValueBytes
            guard !total.overflow, total.partialValue <= maximum else {
                throw .wire(
                    .byteCountExceedsLimit(
                        count: total.overflow ? Int.max : total.partialValue,
                        maximum: maximum
                    )
                )
            }
        }

        mutating func addString(
            _ value: String,
            maximum: Int
        ) throws(StorageWireProtocolError) {
            let byteCount = value.utf8.count
            guard byteCount <= maximum else {
                throw .wire(
                    .byteCountExceedsLimit(
                        count: byteCount,
                        maximum: maximum
                    )
                )
            }
            try add(4)
            try add(byteCount)
        }

        mutating func addOptionalBytes(
            _ bytes: ByteString?,
            maximum: Int
        ) throws(StorageWireProtocolError) {
            try add(1)
            if let bytes {
                try addBytes(bytes, maximum: maximum)
            }
        }

        mutating func addOptionalString(
            _ value: String?,
            maximum: Int
        ) throws(StorageWireProtocolError) {
            try add(1)
            if let value {
                try addString(value, maximum: maximum)
            }
        }

        mutating func addOptionalVersion(
            _ version: Int64?
        ) throws(StorageWireProtocolError) {
            try add(1)
            guard let version else {
                return
            }
            guard version >= 0 else {
                throw .invalidVersion(version)
            }
            try add(8)
        }

        mutating func addCollectionHeader(
            count: Int,
            maximum: Int
        ) throws(StorageWireProtocolError) {
            guard count <= maximum else {
                throw .wire(
                    .collectionCountExceedsLimit(
                        count: count,
                        maximum: maximum
                    )
                )
            }
            try add(4)
        }
    }
}
