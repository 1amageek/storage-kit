import CloudflareDurableObjectStorageWire
import StorageKit

/// StorageKit engine facade for one Cloudflare Durable Object partition identity.
public final class CloudflareDurableObjectStorageEngine: StorageEngine, Sendable {
    public typealias Configuration = CloudflareDurableObjectStorageConfiguration
    public typealias TransactionType = CloudflareDurableObjectStorageTransaction

    public let configuration: CloudflareDurableObjectStorageConfiguration
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let storageLifecycle = StorageEngineLifecycle()

    public var monotonicClock: any StorageMonotonicClock {
        configuration.monotonicClock
    }

    public init(configuration: CloudflareDurableObjectStorageConfiguration) async throws {
        self.configuration = configuration
        let domain = StorageTransactionDomain()
        self.transactionDomain = domain
        self.directoryAccess = KeyValueDirectoryCatalog(
            transactionDomain: domain,
            backend: .cloudflareDurableObject
        )
        let readiness = try await configuration.client.readiness(
            StorageWireReadinessRequest(partitionIdentity: configuration.partitionIdentity)
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
                partitionIdentity: configuration.partitionIdentity,
                client: configuration.client,
                limits: configuration.limits,
                monotonicClock: configuration.monotonicClock,
                transactionDomain: transactionDomain
            )
        }
    }

    public func requestShutdown() {
        transactionDomain.requestShutdown()
        storageLifecycle.requestShutdown()
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }

    deinit {
        requestShutdown()
    }
}
