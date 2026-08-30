import SQLiteStorage
import StorageKit
import StorageKitConformance
import Testing

@Suite("SQLite Directory conformance")
struct SQLiteDirectoryConformanceTests {
    private let conformance = DirectoryConformanceCase<SQLiteStorageEngine> {
        try SQLiteStorageEngine(configuration: .inMemory)
    }

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await conformance.verifyRootInitialization()
    }

    @Test("Foreign root rejection", .timeLimit(.minutes(1)))
    func foreignRootRejection() async throws {
        try await conformance.verifyForeignRootRejection()
    }

    @Test("Foreign root allocator rejection", .timeLimit(.minutes(1)))
    func foreignRootAllocatorRejection() async throws {
        try await conformance.verifyForeignRootAllocatorRejection()
    }

    @Test("Create and open", .timeLimit(.minutes(1)))
    func createAndOpen() async throws {
        try await conformance.verifyCreateAndOpen()
    }

    @Test("Listing", .timeLimit(.minutes(1)))
    func listing() async throws {
        try await conformance.verifyListing()
    }

    @Test("Partition contiguity", .timeLimit(.minutes(1)))
    func partitionContiguity() async throws {
        try await conformance.verifyPartitionContiguity()
    }

    @Test("Move", .timeLimit(.minutes(1)))
    func move() async throws {
        try await conformance.verifyMove()
    }

    @Test("Remove", .timeLimit(.minutes(1)))
    func remove() async throws {
        try await conformance.verifyRemove()
    }

    @Test("Stale parent rejection", .timeLimit(.minutes(1)))
    func staleParentRejection() async throws {
        try await conformance.verifyStaleParentRejection()
    }

    @Test("Domain mismatch", .timeLimit(.minutes(1)))
    func domainMismatch() async throws {
        try await conformance.verifyDomainMismatch()
    }

    @Test("Lease lifecycle", .timeLimit(.minutes(1)))
    func leaseLifecycle() async throws {
        try await conformance.verifyLeaseLifecycle()
    }

    @Test("Lease staleness detection", .timeLimit(.minutes(1)))
    func leaseStalenessDetection() async throws {
        try await conformance.verifyLeaseStalenessDetection()
    }

    @Test("Transactional atomicity", .timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await conformance.verifyTransactionalAtomicity()
    }
}
