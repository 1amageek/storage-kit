/// Monotonic time and cancellable waiting used by storage-backed operations.
public protocol StorageMonotonicClock: Sendable {
    var now: StorageInstant { get }

    func sleep(until deadline: StorageInstant) async throws
}

extension StorageMonotonicClock {
    public func sleep(for duration: Duration) async throws {
        precondition(duration >= .zero, "Storage clock delay must not be negative")
        try await sleep(until: now.advanced(by: duration))
    }
}
