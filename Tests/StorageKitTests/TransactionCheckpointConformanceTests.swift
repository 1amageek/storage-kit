import StorageKit
import StorageKitConformance
import Testing

@Suite("InMemory checkpoint conformance")
struct TransactionCheckpointConformanceTests {
    private let cases = TransactionCheckpointConformanceCase { InMemoryEngine() }
    @Test(.timeLimit(.minutes(1))) func tokens() async throws { try await cases.verifyTokens() }
    @Test(.timeLimit(.minutes(1))) func overlay() async throws { try await cases.verifyOverlay() }
    @Test(.timeLimit(.minutes(1))) func directoryRollback() async throws { try await cases.verifyDirectoryRollback() }
    @Test(.timeLimit(.minutes(1))) func conflicts() async throws { try await cases.verifyConflicts() }
    @Test(.timeLimit(.minutes(1))) func lifecycle() async throws { try await cases.verifyLifecycle() }
    @Test(.timeLimit(.minutes(1))) func budgets() async throws { try await cases.verifyBudgets() }
}
