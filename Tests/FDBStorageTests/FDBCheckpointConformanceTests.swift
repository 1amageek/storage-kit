import DatabaseTypes
import FDBStorage
import Foundation
import FoundationDB
import StorageKit
import StorageKitConformance
import Testing

@Suite("FoundationDB shared checkpoint conformance", .serialized)
struct FDBCheckpointConformanceTests {
    @Test(.timeLimit(.minutes(1))) func tokens() async throws { try await withCase { try await $0.verifyTokens() } }
    @Test(.timeLimit(.minutes(1))) func overlay() async throws { try await withCase { try await $0.verifyOverlay() } }
    @Test(.timeLimit(.minutes(1))) func directoryRollback() async throws { try await withCase { try await $0.verifyDirectoryRollback() } }
    @Test(.timeLimit(.minutes(1))) func conflicts() async throws { try await withCase { try await $0.verifyConflicts() } }
    @Test(.timeLimit(.minutes(1))) func lifecycle() async throws { try await withCase { try await $0.verifyLifecycle() } }
    @Test(.timeLimit(.minutes(1))) func budgets() async throws { try await withCase { try await $0.verifyBudgets() } }

    private func withCase(_ body: (TransactionCheckpointConformanceCase<FDBStorageEngine>) async throws -> Void) async throws {
        let cluster = try FDBCheckpointTestEnvironment.clusterFilePath()
        let path = ["storage-kit-checkpoint-conformance", UUID().uuidString.lowercased()]
        let engine = try await FDBStorageEngine(configuration: .init(clusterFilePath: cluster, rootPath: path))
        let database = try FDBClient.openDatabase(clusterFilePath: cluster)
        // These exact raw keys belong to the shared fixture, which deliberately
        // exercises Layer 0 independently of Directory-allocated content prefixes.
        let keys: [ByteString] = [[1], [2], [3], [4], [0x10], [0x11], [0x20], [0x30], [0x50]]
        try await clear(keys, database: database)
        let fixture = TransactionCheckpointConformanceCase { engine }
        let result: Result<Void, any Error>
        do { try await body(fixture); result = .success(()) }
        catch { result = .failure(error) }
        await engine.shutdown()
        do {
            try await clear(keys, database: database)
            let layer = DirectoryLayer(database: database)
            if try await layer.exists(path: path) { try await layer.remove(path: path) }
        } catch {
            if case .failure(let operationError) = result {
                throw StorageTransactionCleanupError(operationError: operationError, cancellationError: error)
            }
            throw error
        }
        try result.get()
    }

    private func clear(_ keys: [ByteString], database: any DatabaseProtocol) async throws {
        let transaction = try database.createTransaction()
        for key in keys { try transaction.clear(key: key) }
        try await transaction.commit()
    }
}
