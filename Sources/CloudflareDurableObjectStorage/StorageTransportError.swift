import StorageKit

/// Failure contract shared by every Durable Object storage transport.
public enum StorageTransportError: Error, Sendable, Equatable {
    case cancelled
    case storage(StorageError)
    case rejected(
        stage: CloudflareDurableObjectStorageTransportFailureStage
    )
}
