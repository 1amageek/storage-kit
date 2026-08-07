
public struct StorageWireCommitRequest: Sendable, Hashable {
    public let partitionIdentity: StoragePartitionIdentity
    public let observedReadVersion: Int64?
    public let mutations: [StorageWireWriteOperation]
    public let readConflictRanges: [StorageWireKeyRange]
    public let writeConflictRanges: [StorageWireKeyRange]

    public init(
        partitionIdentity: StoragePartitionIdentity,
        observedReadVersion: Int64?,
        mutations: [StorageWireWriteOperation],
        readConflictRanges: [StorageWireKeyRange] = [],
        writeConflictRanges: [StorageWireKeyRange] = []
    ) {
        self.partitionIdentity = partitionIdentity
        self.observedReadVersion = observedReadVersion
        self.mutations = mutations
        self.readConflictRanges = readConflictRanges
        self.writeConflictRanges = writeConflictRanges
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        try partitionIdentity.encode(into: &writer)
        try StorageWireReadRequest.writeOptionalVersion(observedReadVersion, into: &writer)
        let maximum = StorageWireLimits.cloudflareDurableObject.maxMutationsPerCommit
        try StorageWireProtocolError.writeCount(
            mutations.count,
            maximum: maximum,
            into: &writer
        )
        for mutation in mutations {
            try StorageWireProtocolError.encode(mutation, into: &writer)
        }
        try StorageWireProtocolError.writeCount(
            readConflictRanges.count,
            maximum: StorageWireLimits.cloudflareDurableObject.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in readConflictRanges {
            try StorageWireProtocolError.encode(range, into: &writer)
        }
        try StorageWireProtocolError.writeCount(
            writeConflictRanges.count,
            maximum: StorageWireLimits.cloudflareDurableObject.maxConflictRangesPerCommit,
            into: &writer
        )
        for range in writeConflictRanges {
            try StorageWireProtocolError.encode(range, into: &writer)
        }
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.partitionIdentity = try StoragePartitionIdentity(from: &reader)
        self.observedReadVersion = try StorageWireReadRequest.readOptionalVersion(from: &reader)
        let limits = StorageWireLimits.cloudflareDurableObject
        let count = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: limits.maxMutationsPerCommit
        )
        var mutations: [StorageWireWriteOperation] = []
        mutations.reserveCapacity(count)
        for _ in 0..<count {
            mutations.append(try StorageWireProtocolError.readWriteOperation(from: &reader))
        }
        self.mutations = mutations
        let rangeCount = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: limits.maxConflictRangesPerCommit
        )
        var readConflictRanges: [StorageWireKeyRange] = []
        readConflictRanges.reserveCapacity(rangeCount)
        for _ in 0..<rangeCount {
            readConflictRanges.append(try StorageWireProtocolError.readKeyRange(from: &reader))
        }
        self.readConflictRanges = readConflictRanges
        let writeRangeCount = try StorageWireProtocolError.readCount(
            from: &reader,
            maximum: limits.maxConflictRangesPerCommit
        )
        var writeConflictRanges: [StorageWireKeyRange] = []
        writeConflictRanges.reserveCapacity(writeRangeCount)
        for _ in 0..<writeRangeCount {
            writeConflictRanges.append(
                try StorageWireProtocolError.readKeyRange(
                    from: &reader
                )
            )
        }
        self.writeConflictRanges = writeConflictRanges
    }
}
