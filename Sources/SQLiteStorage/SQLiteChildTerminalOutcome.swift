import StorageKit

struct SQLiteChildTerminalOutcome: Sendable {
    let error: StorageError?
    let parentResumed: Bool
}
