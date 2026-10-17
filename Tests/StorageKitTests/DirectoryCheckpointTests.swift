import DatabaseTypes
@testable import StorageKit
import Testing

@Suite("Directory checkpoint provenance")
struct DirectoryCheckpointTests {
    private func execute(
        _ engine: InMemoryEngine,
        _ body: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let transaction = try engine.createTransaction()
        try transaction.configureCheckpoints(limits: TransactionCheckpointLimits(
            maximumEntries: 1_000,
            maximumDepth: 16,
            maximumRetainedBytes: 1_000_000,
            maximumWorkUnits: 100_000,
            maximumReadPageRows: 32
        ))
        try await TransactionLifecycleOwner(transaction: transaction).execute(body)
    }

    @Test(.timeLimit(.minutes(1)))
    func discardedRootDoesNotReviveWhenInitializationReusesItsPrefix() async throws {
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        try await execute(engine) { transaction in
            let checkpoints = try #require(transaction.checkpoints)
            let marker = try await checkpoints.createCheckpoint()
            let discarded = try await catalog.openOrInitializeRoot(transaction: transaction)
            try await checkpoints.rollback(to: marker)
            #expect(try await catalog.openRoot(transaction: transaction) == nil)
            let current = try await catalog.openOrInitializeRoot(transaction: transaction)
            #expect(discarded.keyspacePrefix == current.keyspacePrefix)
            await expectStorageError(.staleLease) {
                _ = try await catalog.openOrCreateDirectory("bad", in: discarded, transaction: transaction)
            }
            _ = try await catalog.openOrCreateDirectory("kept", in: current, transaction: transaction)
        }
        try await engine.withTransaction { transaction in
            let root = try #require(try await catalog.openRoot(transaction: transaction))
            #expect(try await catalog.openDirectory("bad", in: root, transaction: transaction) == nil)
            #expect(try await catalog.openDirectory("kept", in: root, transaction: transaction) != nil)
        }
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func rolledBackMoveInvalidatesReadDescendantsButPreservesOriginalGeneration() async throws {
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        let original = try await engine.withTransaction { transaction in
            let root = try await catalog.openOrInitializeRoot(transaction: transaction)
            let parent = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
            _ = try await catalog.openOrCreateDirectory("child", in: parent, transaction: transaction)
            return parent
        }
        try await execute(engine) { transaction in
            let root = try #require(try await catalog.openRoot(transaction: transaction))
            let checkpoints = try #require(transaction.checkpoints)
            let marker = try await checkpoints.createCheckpoint()
            let moved = try await catalog.move("a", in: root, to: "b", in: root, transaction: transaction)
            let descendant = try #require(try await catalog.openDirectory("child", in: moved, transaction: transaction))
            try await checkpoints.rollback(to: marker)
            await expectStorageError(.staleLease) {
                _ = try await catalog.listChildren(in: moved, after: nil, limit: 10, transaction: transaction)
            }
            await expectStorageError(.staleLease) {
                _ = try await catalog.openDirectory("missing", in: descendant, transaction: transaction)
            }
            #expect(try await catalog.openDirectory("child", in: original, transaction: transaction) != nil)
            #expect(try await catalog.openDirectory("b", in: root, transaction: transaction) == nil)
        }
        try await engine.withTransaction { transaction in
            let root = try #require(try await catalog.openRoot(transaction: transaction))
            #expect(try await catalog.openDirectory("a", in: root, transaction: transaction) != nil)
            #expect(try await catalog.openDirectory("b", in: root, transaction: transaction) == nil)
        }
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func readOfCommittedNodeSurvivesRollbackAndPendingGenerationSurvivesCommit() async throws {
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        try await engine.withTransaction { transaction in
            let root = try await catalog.openOrInitializeRoot(transaction: transaction)
            _ = try await catalog.openOrCreateDirectory("committed", in: root, transaction: transaction)
        }
        let publication = DirectoryPublication()
        try await execute(engine) { transaction in
            let root = try #require(try await catalog.openRoot(transaction: transaction))
            let pending = try await catalog.openOrCreateDirectory("pending", in: root, transaction: transaction)
            let checkpoints = try #require(transaction.checkpoints)
            let marker = try await checkpoints.createCheckpoint()
            let readAfterMarker = try #require(try await catalog.openDirectory("committed", in: root, transaction: transaction))
            _ = try await catalog.openOrCreateDirectory("discarded", in: root, transaction: transaction)
            try await checkpoints.rollback(to: marker)
            _ = try await catalog.listChildren(in: readAfterMarker, after: nil, limit: 10, transaction: transaction)
            _ = try await catalog.openOrCreateDirectory("child", in: pending, transaction: transaction)
            await publication.publish(pending)
        }
        let pending = try #require(await publication.value)
        try await engine.withTransaction { transaction in
            #expect(try await catalog.openDirectory("child", in: pending, transaction: transaction) != nil)
            let root = try #require(try await catalog.openRoot(transaction: transaction))
            #expect(try await catalog.openDirectory("discarded", in: root, transaction: transaction) == nil)
        }
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func invalidWitnessCannotBePublishedAndValueEqualityDoesNotGrantAuthority() throws {
        let witness = DirectoryProvenance()
        let domain = StorageTransactionDomain()
        let discarded = Directory(
            domain: domain,
            address: .root,
            layer: .default,
            keyspacePrefix: [0x01],
            layerRoot: [],
            provenance: [witness]
        )
        let ordinary = Directory(domain: domain, address: .root, layer: .default, keyspacePrefix: [0x01], layerRoot: [])
        #expect(discarded == ordinary)
        #expect(discarded.hashValue == ordinary.hashValue)
        witness.invalidate()
        witness.publish()
        #expect(throws: StorageError.self) {
            try discarded.checkProvenance(operation: .open, backend: .inMemory)
        }
        try ordinary.checkProvenance(operation: .open, backend: .inMemory)
        let published = DirectoryProvenance()
        published.publish()
        published.invalidate()
        try published.requireValid(operation: .open, backend: .inMemory)
    }
}

private actor DirectoryPublication {
    private(set) var value: Directory?
    func publish(_ value: Directory) { self.value = value }
}
