/// Process-local monotonic clock used for storage deadlines.
public struct SystemStorageClock: StorageMonotonicClock {
    public init() {}

    public var now: ContinuousClock.Instant {
        ContinuousClock().now
    }

    public func sleep(
        until deadline: ContinuousClock.Instant
    ) async throws {
        try await ContinuousClock().sleep(until: deadline)
    }
}
