import Foundation
import PostgreSQLStorage
import StorageKit
import StorageKitConformance
import Testing

/// Runs the shared Directory conformance case over the PostgreSQL key-value
/// catalog. Every engine gets its own table so each step starts from an
/// uninitialized keyspace, exactly as the in-memory adapters do.
@Suite(
    "PostgreSQL Directory conformance",
    .serialized,
    .enabled(if: PostgreSQLTestEnvironment.isConfigured)
)
struct PostgreSQLDirectoryConformanceTests {
    private static func uniqueTableName() -> String {
        "directory_conformance_"
            + UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "_")
    }

    private static func makeConfiguration(
        isolationLevel: PostgreSQLIsolationLevel = .serializable
    ) throws -> PostgreSQLConfiguration {
        var configuration = try PostgreSQLTestEnvironment.makeConfiguration()
        configuration.tableName = uniqueTableName()
        configuration.isolationLevel = isolationLevel
        return configuration
    }

    private let conformance = DirectoryConformanceCase<PostgreSQLStorageEngine> {
        try await PostgreSQLStorageEngine(configuration: try makeConfiguration())
    }

    @Test(.timeLimit(.minutes(1))) func rootInitialization() async throws {
        try await conformance.verifyRootInitialization()
    }

    @Test(.timeLimit(.minutes(1))) func layoutRejection() async throws {
        try await conformance.verifyLayoutRejection()
    }

    @Test(.timeLimit(.minutes(1))) func createAndOpen() async throws {
        try await conformance.verifyCreateAndOpen()
    }

    @Test(.timeLimit(.minutes(1))) func listing() async throws {
        try await conformance.verifyListing()
    }

    @Test(.timeLimit(.minutes(1))) func partitionContiguity() async throws {
        try await conformance.verifyPartitionContiguity()
    }

    @Test(.timeLimit(.minutes(1))) func move() async throws {
        try await conformance.verifyMove()
    }

    @Test(.timeLimit(.minutes(1))) func remove() async throws {
        try await conformance.verifyRemove()
    }

    @Test(.timeLimit(.minutes(1))) func domainMismatch() async throws {
        try await conformance.verifyDomainMismatch()
    }

    @Test(.timeLimit(.minutes(1))) func leaseLifecycle() async throws {
        try await conformance.verifyLeaseLifecycle()
    }

    @Test("Lease subtree exclusion", .timeLimit(.minutes(1)))
    func leaseSubtreeExclusion() async throws {
        try await conformance.verifyLeaseSubtreeExclusion()
    }

    @Test(.timeLimit(.minutes(1))) func transactionalAtomicity() async throws {
        try await conformance.verifyTransactionalAtomicity()
    }

    /// READ COMMITTED cannot detect the read-then-write conflict the catalog
    /// relies on, so every catalog mutation is rejected before it writes,
    /// while catalog reads keep working and the store stays uninitialized.
    @Test(.timeLimit(.minutes(1))) func readCommittedRejectsCatalogMutation() async throws {
        let engine = try await PostgreSQLStorageEngine(
            configuration: try Self.makeConfiguration(isolationLevel: .readCommitted)
        )
        defer { engine.requestShutdown() }
        let before = try await engine.withTransaction { transaction in
            try await engine.directoryAccess.openRoot(transaction: transaction)
        }
        #expect(before == nil)

        var rejection: StorageError?
        do {
            _ = try await engine.withTransaction { transaction in
                try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
        } catch let error as StorageError {
            rejection = error
        }
        #expect(rejection?.code == .unsupportedOperation)
        #expect(rejection?.operation == .initialize)

        let after = try await engine.withTransaction { transaction in
            try await engine.directoryAccess.openRoot(transaction: transaction)
        }
        #expect(after == nil)
        await engine.waitUntilShutdown()
    }
}
