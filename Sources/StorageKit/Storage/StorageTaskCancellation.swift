package func ensureStorageTaskIsActive() throws {
    guard !Task<Never, Never>.isCancelled else {
        throw CancellationError()
    }
}
