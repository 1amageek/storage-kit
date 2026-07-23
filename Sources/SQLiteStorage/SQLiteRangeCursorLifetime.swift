import Synchronization

/// Ensures an abandoned iterator finalizes its SQLite statement immediately.
/// The connection handle serializes this synchronous terminal operation with
/// any in-flight step, so no unstructured cleanup task is required.
final class SQLiteRangeCursorLifetime: Sendable {
    struct Payload: Sendable {
        let transaction: SQLiteStorageTransaction
        let registrationIdentifier: UInt64
        let cursorIdentifier: UInt64
    }

    private let payload = Mutex<Payload?>(nil)

    func install(_ payload: Payload) {
        self.payload.withLock { current in
            precondition(current == nil, "SQLite range cursor installed twice")
            current = payload
        }
    }

    func current() -> Payload? {
        payload.withLock { $0 }
    }

    func disarm() {
        payload.withLock { $0 = nil }
    }

    deinit {
        let abandoned = payload.withLock { current -> Payload? in
            let value = current
            current = nil
            return value
        }
        if let abandoned {
            abandoned.transaction.abandonRange(
                registrationIdentifier: abandoned.registrationIdentifier,
                cursorIdentifier: abandoned.cursorIdentifier
            )
        }
    }
}
