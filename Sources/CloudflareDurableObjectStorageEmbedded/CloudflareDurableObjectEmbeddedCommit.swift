import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedCommitRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let observedReadVersion: Int64?
    public let mutations: [EmbeddedWriteOperation]
    public let readConflictRanges: [EmbeddedKeyRange]
    public let writeConflictRanges: [EmbeddedKeyRange]

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        observedReadVersion: Int64?,
        mutations: [EmbeddedWriteOperation],
        readConflictRanges: [EmbeddedKeyRange] = [],
        writeConflictRanges: [EmbeddedKeyRange] = []
    ) {
        self.scope = scope
        self.observedReadVersion = observedReadVersion
        self.mutations = mutations
        self.readConflictRanges = readConflictRanges
        self.writeConflictRanges = writeConflictRanges
    }

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedReadRequest.writeOptionalVersion(observedReadVersion, into: &writer)
        let maximum = EmbeddedLimits.cloudflareDurableObject.maxMutationsPerCommit
        try CloudflareDurableObjectEmbeddedError.writeCount(
            mutations.count,
            maximum: maximum,
            into: &writer
        )
        for mutation in mutations {
            try CloudflareDurableObjectEmbeddedError.encode(mutation, into: &writer)
        }
        try CloudflareDurableObjectEmbeddedError.writeCount(
            readConflictRanges.count,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in readConflictRanges {
            try CloudflareDurableObjectEmbeddedError.encode(range, into: &writer)
        }
        try CloudflareDurableObjectEmbeddedError.writeCount(
            writeConflictRanges.count,
            maximum: EmbeddedLimits.cloudflareDurableObject.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in writeConflictRanges {
            try CloudflareDurableObjectEmbeddedError.encode(range, into: &writer)
        }
    }

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.observedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest.readOptionalVersion(from: &reader)
        let limits = EmbeddedLimits.cloudflareDurableObject
        let count = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: limits.maxMutationsPerCommit
        )
        var mutations: [EmbeddedWriteOperation] = []
        mutations.reserveCapacity(count)
        for _ in 0..<count {
            mutations.append(try CloudflareDurableObjectEmbeddedError.readWriteOperation(from: &reader))
        }
        self.mutations = mutations
        let rangeCount = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: limits.maxConflictRangesPerCommit
        )
        var readConflictRanges: [EmbeddedKeyRange] = []
        readConflictRanges.reserveCapacity(rangeCount)
        for _ in 0..<rangeCount {
            readConflictRanges.append(try CloudflareDurableObjectEmbeddedError.readKeyRange(from: &reader))
        }
        self.readConflictRanges = readConflictRanges
        let writeRangeCount = try CloudflareDurableObjectEmbeddedError.readCount(
            from: &reader,
            maximum: limits.maxConflictRangesPerCommit
        )
        var writeConflictRanges: [EmbeddedKeyRange] = []
        writeConflictRanges.reserveCapacity(writeRangeCount)
        for _ in 0..<writeRangeCount {
            writeConflictRanges.append(
                try CloudflareDurableObjectEmbeddedError.readKeyRange(
                    from: &reader
                )
            )
        }
        self.writeConflictRanges = writeConflictRanges
    }
}
