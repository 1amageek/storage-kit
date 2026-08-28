import StorageKit
import StorageKitConformance
import Testing

@Suite("InMemory Directory conformance")
struct InMemoryDirectoryConformanceTests {
    private let conformance = DirectoryConformanceCase<InMemoryEngine> { InMemoryEngine() }

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await conformance.verifyRootInitialization()
    }

    @Test("Layout rejection", .timeLimit(.minutes(1)))
    func layoutRejection() async throws {
        try await conformance.verifyLayoutRejection()
    }

    @Test("Create and open", .timeLimit(.minutes(1)))
    func createAndOpen() async throws {
        try await conformance.verifyCreateAndOpen()
    }

    @Test("Listing", .timeLimit(.minutes(1)))
    func listing() async throws {
        try await conformance.verifyListing()
    }

    @Test("Move", .timeLimit(.minutes(1)))
    func move() async throws {
        try await conformance.verifyMove()
    }

    @Test("Remove", .timeLimit(.minutes(1)))
    func remove() async throws {
        try await conformance.verifyRemove()
    }

    @Test("Domain mismatch", .timeLimit(.minutes(1)))
    func domainMismatch() async throws {
        try await conformance.verifyDomainMismatch()
    }

    @Test("Lease lifecycle", .timeLimit(.minutes(1)))
    func leaseLifecycle() async throws {
        try await conformance.verifyLeaseLifecycle()
    }

    @Test("Transactional atomicity", .timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await conformance.verifyTransactionalAtomicity()
    }
}
