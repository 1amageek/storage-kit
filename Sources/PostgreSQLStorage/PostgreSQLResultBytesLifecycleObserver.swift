/// Package-internal evidence sink for PostgreSQL result-owner lifetimes.
///
/// The observer is injected per engine. Production engines do not install one,
/// so result construction retains the decoded buffer without an additional
/// allocation or payload copy.
protocol PostgreSQLResultBytesLifecycleObserver: Sendable {
    func resultBytesOwnerCreated(
        readableByteCount: Int,
        baseAddress: UInt?
    ) -> UInt64

    func resultBytesOwnerReleased(identifier: UInt64)
}
