import StorageKit

/// StorageKit engine facade for one Cloudflare Durable Object scope.
public struct CloudflareDurableObjectStorageEngine: StorageEngine {
    public typealias Configuration = CloudflareDurableObjectStorageConfiguration
    public typealias TransactionType = CloudflareDurableObjectStorageTransaction

    public let configuration: CloudflareDurableObjectStorageConfiguration

    public var monotonicClock: any StorageMonotonicClock {
        configuration.monotonicClock
    }

    public init(configuration: CloudflareDurableObjectStorageConfiguration) async throws {
        self.configuration = configuration
        _ = try configuration.scope.durableObjectName(
            maximumBytes: configuration.limits.maxNameBytes
        )
        let readiness = try await configuration.client.readiness(
            CloudflareDurableObjectReadinessRequest(scope: configuration.scope)
        )
        guard readiness.schemaVersion == 1,
              readiness.metadataInitialized else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .initialize,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object storage is not initialized with schema v1"
            )
        }
    }

    public func createTransaction() throws -> CloudflareDurableObjectStorageTransaction {
        CloudflareDurableObjectStorageTransaction(
            scope: configuration.scope,
            client: configuration.client,
            limits: configuration.limits,
            monotonicClock: configuration.monotonicClock
        )
    }

    public func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        try await withTransaction(operation)
    }
}
