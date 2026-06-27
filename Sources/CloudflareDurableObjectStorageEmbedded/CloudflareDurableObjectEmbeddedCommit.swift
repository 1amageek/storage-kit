import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedCommitRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope
    public let observedReadVersion: Int64?
    public let mutations: [EmbeddedWriteOperation]
    public let readConflictRanges: [EmbeddedKeyRange]

    public init(
        scope: CloudflareDurableObjectEmbeddedScope,
        observedReadVersion: Int64?,
        mutations: [EmbeddedWriteOperation],
        readConflictRanges: [EmbeddedKeyRange] = []
    ) {
        self.scope = scope
        self.observedReadVersion = observedReadVersion
        self.mutations = mutations
        self.readConflictRanges = readConflictRanges
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
        try CloudflareDurableObjectEmbeddedReadRequest.writeOptionalVersion(observedReadVersion, into: &writer)
        try CloudflareDurableObjectEmbeddedError.writeCount(mutations.count, into: &writer)
        for mutation in mutations {
            try CloudflareDurableObjectEmbeddedError.encode(mutation, into: &writer)
        }
        try CloudflareDurableObjectEmbeddedError.writeCount(readConflictRanges.count, into: &writer)
        for range in readConflictRanges {
            try CloudflareDurableObjectEmbeddedError.encode(range, into: &writer)
        }
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
        self.observedReadVersion = try CloudflareDurableObjectEmbeddedReadRequest.readOptionalVersion(from: &reader)
        let count = try CloudflareDurableObjectEmbeddedError.readCount(from: &reader)
        var mutations: [EmbeddedWriteOperation] = []
        mutations.reserveCapacity(count)
        for _ in 0..<count {
            mutations.append(try CloudflareDurableObjectEmbeddedError.readWriteOperation(from: &reader))
        }
        self.mutations = mutations
        let rangeCount = try CloudflareDurableObjectEmbeddedError.readCount(from: &reader)
        var readConflictRanges: [EmbeddedKeyRange] = []
        readConflictRanges.reserveCapacity(rangeCount)
        for _ in 0..<rangeCount {
            readConflictRanges.append(try CloudflareDurableObjectEmbeddedError.readKeyRange(from: &reader))
        }
        self.readConflictRanges = readConflictRanges
    }
}
