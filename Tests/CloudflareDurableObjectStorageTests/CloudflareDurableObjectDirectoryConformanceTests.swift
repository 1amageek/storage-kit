import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageTesting
import CloudflareDurableObjectStorageWire
import StorageKit
import StorageKitConformance
import StorageKitSystemClock
import Testing

@Suite("Cloudflare Durable Object Directory conformance")
struct CloudflareDurableObjectDirectoryConformanceTests {
    private let conformance = DirectoryConformanceCase<CloudflareDurableObjectStorageEngine> {
        let client = InMemoryCloudflareDurableObjectStorageClient()
        let partitionIdentity = try StoragePartitionIdentity(databaseID: "main")
        return try await CloudflareDurableObjectSharedClientRouter(
            client: client,
            monotonicClock: SystemStorageClock()
        ).engine(for: partitionIdentity)
    }

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await conformance.verifyRootInitialization()
    }

    @Test("Foreign root rejection", .timeLimit(.minutes(1)))
    func foreignRootRejection() async throws {
        try await conformance.verifyForeignRootRejection()
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
