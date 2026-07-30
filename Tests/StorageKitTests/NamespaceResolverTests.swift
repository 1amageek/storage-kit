import Testing
@testable import StorageKit

@Suite("Namespace Resolver Tests")
struct NamespaceResolverTests {
    private func withDeterministicResolver<T: Sendable>(
        _ operation: @escaping @Sendable (
            DeterministicNamespaceResolver,
            any TransactionAccess
        ) async throws -> T
    ) async throws -> T {
        let engine = InMemoryEngine()
        return try await engine.withTransaction { transaction in
            try await operation(
                DeterministicNamespaceResolver(),
                transaction
            )
        }
    }

    @Test func resolveOrCreateReturnsSubspaceFromPath() async throws {
        let subspace = try await withDeterministicResolver { resolver, transaction in
            try await resolver.resolveOrCreate(
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

    @Test func resolveOrCreateHandlesSingleElementAndEmptyPath() async throws {
        try await withDeterministicResolver { resolver, transaction in
            let single = try await resolver.resolveOrCreate(
                path: ["orders"],
                transaction: transaction
            )
            #expect(single == Subspace(Tuple("orders" as any TupleElement)))

            let empty = try await resolver.resolveOrCreate(
                path: [],
                transaction: transaction
            )
            #expect(empty.prefix == Tuple([]).pack())
        }
    }

    @Test func namespaceMappingIsDeterministicAndDistinct() async throws {
        try await withDeterministicResolver { resolver, transaction in
            let first = try await resolver.resolveOrCreate(
                path: ["foo", "bar"],
                transaction: transaction
            )
            let second = try await resolver.resolveExisting(
                path: ["foo", "bar"],
                transaction: transaction
            )
            let distinct = try await resolver.resolveOrCreate(
                path: ["other"],
                transaction: transaction
            )
            #expect(first == second)
            #expect(first != distinct)
        }
    }

    @Test func deterministicNamespacesAlwaysExist() async throws {
        try await withDeterministicResolver { resolver, transaction in
            let namedPathExists = try await resolver.namespaceExists(
                path: ["anything"],
                transaction: transaction
            )
            let rootExists = try await resolver.namespaceExists(
                path: [],
                transaction: transaction
            )
            #expect(namedPathExists)
            #expect(rootExists)
        }
    }

    @Test func inMemoryEngineDoesNotExposeNamespaceCatalog() {
        let engine = InMemoryEngine()
        #expect(engine.namespaceCatalog == nil)
    }

    @Test func catalogConveniencesReportUnsupportedOperation() async throws {
        let engine = InMemoryEngine()
        do {
            _ = try await engine.listNamespaces(path: ["anything"])
            Issue.record("Expected namespace listing to be unsupported")
        } catch let error as StorageError {
            #expect(error.code == .unsupportedOperation)
            #expect(error.operation == .read)
        }
        do {
            try await engine.removeNamespace(path: ["anything"])
            Issue.record("Expected namespace removal to be unsupported")
        } catch let error as StorageError {
            #expect(error.code == .unsupportedOperation)
            #expect(error.operation == .delete)
        }
    }

    @Test func oneShotNamespaceConvenienceSharesEngineTransaction() async throws {
        let engine = InMemoryEngine()
        let subspace = try await engine.resolveOrCreateNamespace(path: ["users"])

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
