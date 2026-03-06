import Testing
import Foundation
@testable import StorageKit

@Suite("DirectoryService Tests")
struct DirectoryServiceTests {

    // =========================================================================
    // MARK: - StaticDirectoryService
    // =========================================================================

    @Test func createOrOpen_returnsSubspaceFromPath() async throws {
        let service = StaticDirectoryService()
        let subspace = try await service.createOrOpen(path: ["User", "email_index"])
        // The subspace prefix should be the Tuple encoding of ["User", "email_index"]
        let expected = Subspace(Tuple("User" as any TupleElement, "email_index" as any TupleElement))
        #expect(subspace == expected)
    }

    @Test func createOrOpen_singleElement() async throws {
        let service = StaticDirectoryService()
        let subspace = try await service.createOrOpen(path: ["orders"])
        let expected = Subspace(Tuple("orders" as any TupleElement))
        #expect(subspace == expected)
    }

    @Test func createOrOpen_emptyPath() async throws {
        let service = StaticDirectoryService()
        let subspace = try await service.createOrOpen(path: [])
        // Empty path → empty Tuple → empty prefix
        #expect(subspace.prefix == Tuple([]).pack())
    }

    @Test func createOrOpen_deterministicMapping() async throws {
        let service = StaticDirectoryService()
        let a = try await service.createOrOpen(path: ["foo", "bar"])
        let b = try await service.createOrOpen(path: ["foo", "bar"])
        #expect(a == b)
    }

    @Test func createOrOpen_differentPathsDifferentSubspaces() async throws {
        let service = StaticDirectoryService()
        let a = try await service.createOrOpen(path: ["foo"])
        let b = try await service.createOrOpen(path: ["bar"])
        #expect(a != b)
    }

    @Test func exists_alwaysReturnsTrue() async throws {
        let service = StaticDirectoryService()
        let result = try await service.exists(path: ["anything"])
        #expect(result == true)
    }

    @Test func exists_emptyPath() async throws {
        let service = StaticDirectoryService()
        let result = try await service.exists(path: [])
        #expect(result == true)
    }

    // =========================================================================
    // MARK: - DirectoryService Default Implementations
    // =========================================================================

    @Test func defaultList_returnsEmptyArray() async throws {
        let service = StaticDirectoryService()
        // StaticDirectoryService doesn't override list, so it uses the default
        let result = try await service.list(path: ["anything"])
        #expect(result.isEmpty)
    }

    @Test func defaultRemove_isNoOp() async throws {
        let service = StaticDirectoryService()
        // Should not throw
        try await service.remove(path: ["anything"])
    }

    // =========================================================================
    // MARK: - StorageEngine.directoryService Default
    // =========================================================================

    @Test func inMemoryEngine_defaultDirectoryService() async throws {
        let engine = InMemoryEngine()
        let service = engine.directoryService
        // Default should be StaticDirectoryService
        #expect(service is StaticDirectoryService)
    }

    @Test func directoryService_createOrOpenThenUseWithEngine() async throws {
        let engine = InMemoryEngine()
        let service = engine.directoryService
        let subspace = try await service.createOrOpen(path: ["users"])

        try await engine.withTransaction { tx in
            tx.setValue([42], for: subspace.pack(Tuple(Int64(1))))
        }

        try await engine.withTransaction { tx in
            let value = try await tx.getValue(for: subspace.pack(Tuple(Int64(1))))
            #expect(value == [42])
        }
    }
}
