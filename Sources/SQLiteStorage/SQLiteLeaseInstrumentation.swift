struct SQLiteLeaseInstrumentation: Sendable, Equatable {
    let hasActiveRoot: Bool
    let waitingRootCount: Int
    let savepointBeginCount: UInt64
    let savepointReleaseCount: UInt64
    let savepointRollbackCount: UInt64
}
