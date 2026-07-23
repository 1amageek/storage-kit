import Testing
@testable import StorageKit

@Suite("DirectoryService Tests")
struct DirectoryServiceTests {
    private func withStaticService<T: Sendable>(
        _ operation: (
            StaticDirectoryService,
            any TransactionAccess
        ) async throws -> T
    ) async throws -> T {
        let engine = InMemoryEngine()
        return try await engine.withTransaction { transaction in
            try await operation(StaticDirectoryService(), transaction)
        }
    }

    @Test func createOrOpenReturnsSubspaceFromPath() async throws {
        let subspace = try await withStaticService { service, transaction in
            try await service.createOrOpen(
                path: ["User", "email_index"],
                transaction: transaction
            )
        }
        let expected = Subspace(
            Tuple(
                "User" as any TupleElement,
                "email_index" as any TupleElement
            )
        )
        #expect(subspace == expected)
    }

    @Test func createOrOpenHandlesSingleElementAndEmptyPath() async throws {
        try await withStaticService { service, transaction in
            let single = try await service.createOrOpen(
                path: ["orders"],
                transaction: transaction
            )
            #expect(single == Subspace(Tuple("orders" as any TupleElement)))

            let empty = try await service.createOrOpen(
                path: [],
                transaction: transaction
            )
            #expect(empty.prefix == Tuple([]).pack())
        }
    }

    @Test func createOrOpenMappingIsDeterministicAndDistinct() async throws {
        try await withStaticService { service, transaction in
            let first = try await service.createOrOpen(
                path: ["foo", "bar"],
                transaction: transaction
            )
            let second = try await service.createOrOpen(
                path: ["foo", "bar"],
                transaction: transaction
            )
            let distinct = try await service.createOrOpen(
                path: ["other"],
                transaction: transaction
            )
            #expect(first == second)
            #expect(first != distinct)
        }
    }

    @Test func staticExistsAlwaysReturnsTrue() async throws {
        try await withStaticService { service, transaction in
            let namedPathExists = try await service.exists(
                path: ["anything"],
                transaction: transaction
            )
            let rootExists = try await service.exists(
                path: [],
                transaction: transaction
            )
            #expect(namedPathExists)
            #expect(rootExists)
        }
    }

    @Test func staticListReportsUnsupportedOperation() async throws {
        try await withStaticService { service, transaction in
            do {
                _ = try await service.list(
                    path: ["anything"],
                    transaction: transaction
                )
                Issue.record("Expected list to report an unsupported operation")
            } catch let error as StorageError {
                #expect(error.code == .unsupportedOperation)
                #expect(error.operation == .read)
            }
        }
    }

    @Test func staticRemoveReportsUnsupportedOperation() async throws {
        try await withStaticService { service, transaction in
            do {
                try await service.remove(
                    path: ["anything"],
                    transaction: transaction
                )
                Issue.record("Expected remove to report an unsupported operation")
            } catch let error as StorageError {
                #expect(error.code == .unsupportedOperation)
                #expect(error.operation == .delete)
            }
        }
    }

    @Test func inMemoryEngineUsesStaticDirectoryService() {
        let engine = InMemoryEngine()
        #expect(engine.directoryService is StaticDirectoryService)
    }

    @Test func oneShotDirectoryConvenienceSharesEngineTransaction() async throws {
        let engine = InMemoryEngine()
        let subspace = try await engine.createOrOpenDirectory(path: ["users"])

        try await engine.withTransaction { transaction in
            try transaction.setValue(
                [42],
                for: subspace.pack(Tuple(Int64(1)))
            )
        }

        try await engine.withTransaction { transaction in
            let value = try await transaction.getValue(
                for: subspace.pack(Tuple(Int64(1)))
            )
            #expect(value == [42])
        }
    }
}
