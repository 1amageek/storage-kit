import DatabaseTypes
import NIOCore
import StorageKit

/// Retains PostgreSQL result bytes and lends their readable region without copying.
struct PostgreSQLResultBytesOwner<Observation: Sendable>:
    ByteStringOwner {
    let buffer: ByteBuffer
    private let observation: Observation

    init(
        buffer: ByteBuffer,
        observation: Observation
    ) {
        self.buffer = buffer
        self.observation = observation
    }

    var count: Int {
        buffer.readableBytes
    }

    /// A readable region can share a larger PostgreSQL/NIO result allocation.
    var retainedByteCount: Int? { nil }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try buffer.withUnsafeReadableBytes(body)
    }
}

/// Zero-size production specialization that keeps the owner inside the
/// existential's three-word inline buffer.
struct PostgreSQLResultBytesNoObservation: Sendable {}

/// Test-only observation specialization of the production owner implementation.
struct PostgreSQLResultBytesObservation: Sendable {
    private let token: PostgreSQLResultBytesLifecycleToken

    init(
        buffer: ByteBuffer,
        observer: any PostgreSQLResultBytesLifecycleObserver
    ) {
        let baseAddress = buffer.withUnsafeReadableBytes { bytes in
            bytes.baseAddress.map { UInt(bitPattern: $0) }
        }
        self.token = PostgreSQLResultBytesLifecycleToken(
            observer: observer,
            identifier: observer.resultBytesOwnerCreated(
                readableByteCount: buffer.readableBytes,
                baseAddress: baseAddress
            )
        )
    }
}

private final class PostgreSQLResultBytesLifecycleToken: Sendable {
    private let observer: any PostgreSQLResultBytesLifecycleObserver
    private let identifier: UInt64

    init(
        observer: any PostgreSQLResultBytesLifecycleObserver,
        identifier: UInt64
    ) {
        self.observer = observer
        self.identifier = identifier
    }

    deinit {
        observer.resultBytesOwnerReleased(identifier: identifier)
    }
}
