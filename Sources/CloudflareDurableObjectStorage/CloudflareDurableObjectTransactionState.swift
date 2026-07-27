import CloudflareDurableObjectStorageWire
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
    var mutations: [StorageWireWriteOperation] = []
    var readConflictRanges: [StorageWireKeyRange] = []
    var writeConflictRanges: [StorageWireKeyRange] = []
    var phase: CloudflareDurableObjectTransactionPhase = .open
    var observedReadVersion: Int64?
    var committedVersion: Int64?
    var deadline: ContinuousClock.Instant?
}
