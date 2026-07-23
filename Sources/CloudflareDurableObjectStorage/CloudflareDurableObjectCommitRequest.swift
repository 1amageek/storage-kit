/// Host commit request.
public struct CloudflareDurableObjectCommitRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectStorageScope
    public let observedReadVersion: Int64?
    public let mutations: [CloudflareDurableObjectMutation]
    public let readConflictRanges: [CloudflareDurableObjectConflictRange]
    public let writeConflictRanges: [CloudflareDurableObjectConflictRange]

    public init(
        scope: CloudflareDurableObjectStorageScope,
        observedReadVersion: Int64?,
        mutations: [CloudflareDurableObjectMutation],
        readConflictRanges: [CloudflareDurableObjectConflictRange] = [],
        writeConflictRanges: [CloudflareDurableObjectConflictRange] = []
    ) {
        self.scope = scope
        self.observedReadVersion = observedReadVersion
        self.mutations = mutations
        self.readConflictRanges = readConflictRanges
        self.writeConflictRanges = writeConflictRanges
    }
}
