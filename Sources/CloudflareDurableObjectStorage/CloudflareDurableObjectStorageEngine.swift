import StorageKit

/// StorageKit engine facade for one Cloudflare Durable Object scope.
public struct CloudflareDurableObjectStorageEngine: StorageEngine {
    public typealias Configuration = CloudflareDurableObjectStorageConfiguration
    public typealias TransactionType = CloudflareDurableObjectStorageTransaction

    public let configuration: CloudflareDurableObjectStorageConfiguration
    public let durableObjectName: String

    public init(configuration: CloudflareDurableObjectStorageConfiguration) async throws {
        self.configuration = configuration
        self.durableObjectName = try configuration.nameCodec.name(for: configuration.scope)
        try CloudflareDurableObjectStorageEngine.validateName(
            durableObjectName,
            limit: configuration.limits.maxNameBytes
        )
    }

    public func createTransaction() throws -> CloudflareDurableObjectStorageTransaction {
        CloudflareDurableObjectStorageTransaction(
            scope: configuration.scope,
            client: configuration.client,
            limits: configuration.limits
        )
    }

    public func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        try await withTransaction(operation)
    }

    private static func validateName(_ name: String, limit: Int) throws {
        let actual = name.utf8.count
        guard actual <= limit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .initialize,
                backend: .cloudflareDurableObject,
                message: "Durable Object name exceeds configured byte limit"
            )
        }
    }
}
