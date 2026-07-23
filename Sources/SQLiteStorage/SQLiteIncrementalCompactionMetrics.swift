/// SQLite page counts observed around one incremental vacuum invocation.
struct SQLiteIncrementalCompactionMetrics: Sendable {
    let freePagesBefore: UInt64
    let freePagesAfter: UInt64
}
