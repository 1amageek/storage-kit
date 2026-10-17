import CloudflareDurableObjectStorageWire
import StorageKit

enum CloudflareDurableObjectTransactionPhase: Sendable {
    case open
    case sealing(TransactionOperationCompletion)
    case committing(TransactionOperationCompletion)
    case committed
    case cancelling(TransactionOperationCompletion)
    case cancelled
    case failed(any Error)
    case commitUnknown(StorageError)
}

struct CloudflareDurableObjectTransactionState: Sendable {
    var checkpointCompletion: TransactionOperationCompletion?
    var checkpointEnabled = false
    var hasAdmittedAccess = false
    var mutations: [StorageWireWriteOperation] = []
    var readConflictRanges: [StorageWireKeyRange] = []
    var writeConflictRanges: [StorageWireKeyRange] = []
    var phase: CloudflareDurableObjectTransactionPhase = .open
    var observedReadVersion: Int64?
    var committedVersion: Int64?
    var deadline: StorageInstant?
}
