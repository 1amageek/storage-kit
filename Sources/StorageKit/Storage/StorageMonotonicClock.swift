/// Monotonic time and cancellable waiting used by storage-backed operations.
public protocol StorageMonotonicClock: Sendable {
    var now: StorageInstant { get }

    func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError)

    /// Suspends for a relative monotonic duration.
    func sleep(
        for duration: Duration
    ) async throws(StorageClockError)
}

extension StorageMonotonicClock {
    public func sleep(
        for duration: Duration
    ) async throws(StorageClockError) {
        precondition(duration >= .zero, "Storage clock delay must not be negative")
        try await sleep(until: now.advanced(by: duration))
    }
}
