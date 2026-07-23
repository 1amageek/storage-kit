import StorageKit

enum CloudflareDurableObjectTransactionPhase: Sendable {
    case open
    case committing(TransactionOperationCompletion)
    case committed
    case cancelling(TransactionOperationCompletion)
    case cancelled
    case failed(StorageError)
    case commitUnknown(StorageError)
}

struct CloudflareDurableObjectTransactionState: Sendable {
    var writeBuffer: [CloudflareDurableObjectWriteOp] = []
    var readConflictRanges: [CloudflareDurableObjectConflictRange] = []
    var writeConflictRanges: [CloudflareDurableObjectConflictRange] = []
    var phase: CloudflareDurableObjectTransactionPhase = .open
    var observedReadVersion: Int64?
    var committedVersion: Int64?
    var versionstamp: Bytes?
    var deadline: ContinuousClock.Instant?
}
