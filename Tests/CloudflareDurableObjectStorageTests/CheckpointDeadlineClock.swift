import StorageKit
import Synchronization

final class CheckpointDeadlineClock: StorageMonotonicClock {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))

    var now: StorageInstant { instant.withLock { $0 } }

    func advance(by duration: Duration) {
        instant.withLock { $0 = $0.advanced(by: duration) }
    }

    func sleep(until deadline: StorageInstant) async throws(StorageClockError) {
        throw .unavailable
    }
}
