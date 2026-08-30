import DatabaseTypes
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

    @Test(.timeLimit(.minutes(1))) func foreignRootRejection() async throws {
        try await conformance.verifyForeignRootRejection()
    }

    @Test(.timeLimit(.minutes(1))) func foreignRootAllocatorRejection() async throws {
        try await conformance.verifyForeignRootAllocatorRejection()
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

    @Test(.timeLimit(.minutes(1))) func staleParentRejection() async throws {
        try await conformance.verifyStaleParentRejection()
    }

    @Test(.timeLimit(.minutes(1))) func recreatedParentPositioning() async throws {
        try await conformance.verifyRecreatedParentPositioning()
    }

    @Test(.timeLimit(.minutes(1))) func domainMismatch() async throws {
        try await conformance.verifyDomainMismatch()
    }

    @Test(.timeLimit(.minutes(1))) func leaseLifecycle() async throws {
        try await conformance.verifyLeaseLifecycle()
    }

    @Test(.timeLimit(.minutes(1))) func leaseStalenessDetection() async throws {
        try await conformance.verifyLeaseStalenessDetection()
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

    /// REPEATABLE READ commits a catalog write against a snapshot that another
    /// transaction has already superseded, because PostgreSQL only detects a
    /// conflict between writes that touch the same row. The catalog's
    /// read-then-write walk is therefore unenforceable at this level, and every
    /// catalog mutation is rejected exactly as it is under READ COMMITTED.
    @Test(.timeLimit(.minutes(1))) func repeatableReadRejectsCatalogMutation() async throws {
        let engine = try await PostgreSQLStorageEngine(
            configuration: try Self.makeConfiguration(isolationLevel: .repeatableRead)
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

    /// A write bound to a Partition depends on the same read-then-write
    /// conflict as a catalog mutation: the generation walk only reads the node
    /// a concurrent removal writes. Under REPEATABLE READ the binding is
    /// therefore rejected before any I/O, while reading the catalog, issuing
    /// the lease, and binding read access stay available.
    @Test(.timeLimit(.minutes(1))) func repeatableReadRejectsPartitionWriteBinding() async throws {
        var configuration = try Self.makeConfiguration(isolationLevel: .serializable)
        let writer = try await PostgreSQLStorageEngine(configuration: configuration)
        let created = try await writer.withTransaction { transaction -> Partition in
            let root = try await writer.directoryAccess.openOrInitializeRoot(
                transaction: transaction
            )
            return try await writer.directoryAccess.openOrCreatePartition(
                "p",
                in: root,
                transaction: transaction
            )
        }
        let dataKey = ByteString(Array(created.root.root.prefix) + [0x6B])
        await writer.shutdown()

        configuration.isolationLevel = .repeatableRead
        let engine = try await PostgreSQLStorageEngine(configuration: configuration)
        defer { engine.requestShutdown() }

        try await engine.withTransaction { transaction in
            guard let root = try await engine.directoryAccess.openRoot(
                transaction: transaction
            ) else {
                Issue.record("The serializable engine's root must still be readable")
                return
            }
            guard let partition = try await engine.directoryAccess.openPartition(
                "p",
                in: root,
                transaction: transaction
            ) else {
                Issue.record("The serializable engine's Partition must still be readable")
                return
            }
            let lease = try await engine.leasePartition(partition, transaction: transaction)
            let existing = try await lease.withReadAccess(transaction) { access in
                try await access.getValue(for: dataKey)
            }
            #expect(existing == nil)

            var rejection: StorageError?
            do {
                try await lease.withWriteAccess(transaction) { access in
                    try access.setValue([0x01], for: dataKey)
                }
            } catch let error as StorageError {
                rejection = error
            }
            #expect(rejection?.code == .unsupportedOperation)
            #expect(rejection?.operation == .write)
            lease.release()
        }

        let unwritten = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: dataKey)
        }
        #expect(unwritten == nil)
        await engine.waitUntilShutdown()
    }

    /// A read binding promises the leased generation for the span of its
    /// closure, and READ COMMITTED takes a fresh snapshot per statement: a
    /// Partition removed after the generation walk would read back as an empty
    /// one. The binding is therefore refused before any I/O, while catalog
    /// reads, lease issuance, and data-row reads outside a binding stay
    /// available.
    @Test(.timeLimit(.minutes(1))) func readCommittedRejectsPartitionReadBinding() async throws {
        var configuration = try Self.makeConfiguration(isolationLevel: .serializable)
        let writer = try await PostgreSQLStorageEngine(configuration: configuration)
        let created = try await writer.withTransaction { transaction -> Partition in
            let root = try await writer.directoryAccess.openOrInitializeRoot(
                transaction: transaction
            )
            return try await writer.directoryAccess.openOrCreatePartition(
                "p",
                in: root,
                transaction: transaction
            )
        }
        let dataKey = ByteString(Array(created.root.root.prefix) + [0x6B])
        try await writer.withTransaction { transaction in
            try transaction.setValue([0x01], for: dataKey)
        }
        await writer.shutdown()

        configuration.isolationLevel = .readCommitted
        let engine = try await PostgreSQLStorageEngine(configuration: configuration)
        defer { engine.requestShutdown() }

        try await engine.withTransaction { transaction in
            guard let root = try await engine.directoryAccess.openRoot(
                transaction: transaction
            ) else {
                Issue.record("The serializable engine's root must still be readable")
                return
            }
            guard let partition = try await engine.directoryAccess.openPartition(
                "p",
                in: root,
                transaction: transaction
            ) else {
                Issue.record("The serializable engine's Partition must still be readable")
                return
            }
            let lease = try await engine.leasePartition(partition, transaction: transaction)

            var readRejection: StorageError?
            do {
                _ = try await lease.withReadAccess(transaction) { access in
                    try await access.getValue(for: dataKey)
                }
            } catch let error as StorageError {
                readRejection = error
            }
            #expect(readRejection?.code == .unsupportedOperation)
            #expect(readRejection?.operation == .read)

            var writeRejection: StorageError?
            do {
                try await lease.withWriteAccess(transaction) { access in
                    try access.setValue([0x02], for: dataKey)
                }
            } catch let error as StorageError {
                writeRejection = error
            }
            #expect(writeRejection?.code == .unsupportedOperation)
            #expect(writeRejection?.operation == .write)
            lease.release()

            // The refusal belongs to the binding, not to the backend: the same
            // key still reads through the transaction itself.
            let direct = try await transaction.getValue(for: dataKey)
            #expect(direct == ByteString([0x01]))
        }
        await engine.waitUntilShutdown()
    }
}
