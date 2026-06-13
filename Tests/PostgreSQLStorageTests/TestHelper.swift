import Testing
import Foundation
@testable import PostgreSQLStorage
@testable import StorageKit

/// Parent suite that serializes all PostgreSQL test suites.
///
/// Both `PostgreSQLStorageTests` and `TransactionRunnerPatternTests` are nested
/// inside this parent with `.serialized`, ensuring they never run concurrently.
/// This prevents connection pool contention on the shared PostgreSQL instance.
@Suite("All PostgreSQL Tests", .serialized)
enum AllPostgreSQLTests {}

/// Shared engine factory for PostgreSQL tests.
enum PostgreSQLTestHelper {

    /// Whether a PostgreSQL test host is configured in the environment.
    ///
    /// DB-requiring suites gate on this via `.enabled(if:)` so they are cleanly
    /// skipped — not failed — when `POSTGRES_TEST_HOST` is unset. Swift Testing
    /// treats a thrown error as a failure, so a skip cannot be expressed by
    /// throwing; it must be a condition trait evaluated before the test runs.
    static var isAvailable: Bool {
        ProcessInfo.processInfo.environment["POSTGRES_TEST_HOST"] != nil
    }

    /// Create a fresh engine. Each call creates a new engine and connection pool.
    ///
    /// NOTE: Callers MUST call `engine.shutdown()` when done to release the pool.
    static func makeEngine() async throws -> PostgreSQLStorageEngine {
        guard let host = ProcessInfo.processInfo.environment["POSTGRES_TEST_HOST"] else {
            throw PostgreSQLTestSkipError()
        }
        let port = ProcessInfo.processInfo.environment["POSTGRES_TEST_PORT"]
            .flatMap(Int.init) ?? 5432
        let user = ProcessInfo.processInfo.environment["POSTGRES_TEST_USER"] ?? "postgres"
        let password = ProcessInfo.processInfo.environment["POSTGRES_TEST_PASSWORD"] ?? ""
        let database = ProcessInfo.processInfo.environment["POSTGRES_TEST_DB"] ?? "storage_kit_test"

        let config = PostgreSQLConfiguration(
            host: host,
            port: port,
            username: user,
            password: password,
            database: database
        )
        return try await PostgreSQLStorageEngine(configuration: config)
    }
}

/// Error thrown to skip tests when PostgreSQL is not available.
struct PostgreSQLTestSkipError: Error {}
