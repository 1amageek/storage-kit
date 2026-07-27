import StorageKit

/// Process-local monotonic clock used for storage deadlines.
public struct SystemStorageClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    public init() {}

    public var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    public func sleep(until deadline: StorageInstant) async throws {
        let remaining = now.duration(to: deadline)
        guard remaining > .zero else { return }
        try await Self.clock.sleep(for: remaining)
    }
}
