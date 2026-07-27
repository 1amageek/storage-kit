/// Monotonic point in time measured from a platform-defined reference.
public struct StorageInstant: Sendable, Equatable, Comparable {
    package let durationSinceReference: Duration

    public init(durationSinceReference: Duration) {
        self.durationSinceReference = durationSinceReference
    }

    public func advanced(by duration: Duration) -> StorageInstant {
        StorageInstant(
            durationSinceReference: durationSinceReference + duration
        )
    }

    public func duration(to other: StorageInstant) -> Duration {
        other.durationSinceReference - durationSinceReference
    }

    public static func < (lhs: StorageInstant, rhs: StorageInstant) -> Bool {
        lhs.durationSinceReference < rhs.durationSinceReference
    }
}
