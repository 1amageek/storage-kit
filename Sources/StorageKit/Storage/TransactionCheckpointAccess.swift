/// Attenuated authority to restore the view of one transaction attempt.
public protocol TransactionCheckpointAccess: Sendable {
    func createCheckpoint() async throws -> TransactionCheckpoint
    func rollback(to checkpoint: TransactionCheckpoint) async throws
    func release(_ checkpoint: TransactionCheckpoint) async throws
}
