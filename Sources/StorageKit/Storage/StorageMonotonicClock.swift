/// Monotonic time and cancellable waiting used by storage-backed operations.
public protocol StorageMonotonicClock: Sendable {
    var now: ContinuousClock.Instant { get }

    func sleep(until deadline: ContinuousClock.Instant) async throws
}

extension StorageMonotonicClock {
    public func sleep(for duration: Duration) async throws {
        precondition(duration >= .zero, "Storage clock delay must not be negative")
        try await sleep(until: now.advanced(by: duration))
    }
}
