import Testing
@testable import PostgreSQLStorage

extension SerializedPostgreSQLStorageTests {
@Suite(
    "PostgreSQL Client Lifecycle Tests",
    .serialized,
    .enabled(if: PostgreSQLTestEnvironment.isConfigured)
)
struct PostgreSQLClientLifecycleTests {
    @available(
        macOS 26.0,
        iOS 26.0,
        tvOS 26.0,
        watchOS 26.0,
        visionOS 26.0,
        *
    )
    @Test("Client run loop starts before the first connection lease")
    func runLoopStartsBeforeFirstLease() async throws {
        let recorder = PostgreSQLClientLifecycleLogRecorder()
        let configuration = try PostgreSQLTestEnvironment.makeConfiguration(
            backgroundLogger: recorder.makeLogger()
        )
        let engine = try await PostgreSQLStorageEngine(configuration: configuration)
        defer { await engine.waitUntilShutdown() }

        #expect(
            !recorder.messages.contains(
                "Trying to lease connection from `PostgresClient`, but "
                    + "`PostgresClient.run()` hasn't been called yet."
            )
        )
    }
}
}
