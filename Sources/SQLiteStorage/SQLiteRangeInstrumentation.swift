struct SQLiteRangeInstrumentation: Sendable, Equatable {
    let prepareCount: UInt64
    let stepCount: UInt64
    let payloadCopyCount: UInt64
    let finalizeCount: UInt64
    let openCursorCount: Int
}
