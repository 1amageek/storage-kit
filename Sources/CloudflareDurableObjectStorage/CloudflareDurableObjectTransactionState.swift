enum CloudflareDurableObjectTransactionPhase: Sendable, Equatable {
    case open
    case committing
    case committed
    case commitUnknown
    case cancelled
}

struct CloudflareDurableObjectTransactionState: Sendable {
    var writeBuffer: [CloudflareDurableObjectWriteOp] = []
    var readConflictRanges: [CloudflareDurableObjectConflictRange] = []
    var phase: CloudflareDurableObjectTransactionPhase = .open
    var observedReadVersion: Int64?
    var committedVersion: Int64?
}
