import CloudflareDurableObjectStorageWire
import StorageKit

/// StorageKit engine facade for one Cloudflare Durable Object scope.
public final class CloudflareDurableObjectStorageEngine: StorageEngine, Sendable {
    public typealias Configuration = CloudflareDurableObjectStorageConfiguration
    public typealias TransactionType = CloudflareDurableObjectStorageTransaction

    public let configuration: CloudflareDurableObjectStorageConfiguration
    private let transactionDomain: StorageTransactionDomain
    private let storageLifecycle = StorageEngineLifecycle()

    public var monotonicClock: any StorageMonotonicClock {
        configuration.monotonicClock
    }

    public init(configuration: CloudflareDurableObjectStorageConfiguration) async throws {
        self.configuration = configuration
        self.transactionDomain = StorageTransactionDomain()
        let readiness = try await configuration.client.readiness(
            StorageWireReadinessRequest(scope: configuration.scope)
        )
        guard readiness.schemaVersion == 1,
            readiness.metadataInitialized
        else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .initialize,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object storage is not initialized with schema v1"
            )
        }
    }

    public func createTransaction() throws -> CloudflareDurableObjectStorageTransaction {
        try storageLifecycle.withActiveAdmission(
            backend: .cloudflareDurableObject,
            operation: .beginTransaction
        ) {
            CloudflareDurableObjectStorageTransaction(
                scope: configuration.scope,
                client: configuration.client,
                limits: configuration.limits,
                monotonicClock: configuration.monotonicClock,
                transactionDomain: transactionDomain
            )
        }
    }

    public func requestShutdown() {
        storageLifecycle.requestShutdown()
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }
}
