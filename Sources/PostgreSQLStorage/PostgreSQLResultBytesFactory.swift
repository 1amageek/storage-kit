import DatabaseTypes
import NIOCore

/// Constructs the result owner selected by an engine's immutable composition.
struct PostgreSQLResultBytesFactory: Sendable {
    static let production = PostgreSQLResultBytesFactory(
        lifecycleObserver: nil
    )

    private let lifecycleObserver:
        (any PostgreSQLResultBytesLifecycleObserver)?

    init(
        lifecycleObserver:
            (any PostgreSQLResultBytesLifecycleObserver)?
    ) {
        self.lifecycleObserver = lifecycleObserver
    }

    func makeByteString(retaining buffer: ByteBuffer) -> ByteString {
        guard let lifecycleObserver else {
            return ByteString(
                retaining: PostgreSQLResultBytesOwner(
                    buffer: buffer,
                    observation: PostgreSQLResultBytesNoObservation()
                )
            )
        }
        return ByteString(
            retaining: PostgreSQLResultBytesOwner(
                buffer: buffer,
                observation: PostgreSQLResultBytesObservation(
                    buffer: buffer,
                    observer: lifecycleObserver
                )
            )
        )
    }
}
