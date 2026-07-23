public protocol CloudflareDurableObjectStorageTransportFailure: Error, Sendable {
    var failureStage: CloudflareDurableObjectStorageTransportFailureStage { get }
}
