import Foundation
import Logging
@testable import PostgreSQLStorage
@testable import StorageKit
import Testing

/// Parent suite that serializes all PostgreSQL test suites.
///
/// Both `PostgreSQLStorageTests` and `DatabaseFrameworkTransactionContractTests` are nested
/// inside this parent with `.serialized`, ensuring they never run concurrently.
/// This prevents connection pool contention on the shared PostgreSQL instance.
@Suite("Serialized PostgreSQL Storage Tests", .serialized)
enum SerializedPostgreSQLStorageTests {}

/// Configuration and engine creation for PostgreSQL integration tests.
enum PostgreSQLTestEnvironment {

    /// Whether a non-empty PostgreSQL test host is configured.
    ///
    /// Behavioral suites use this condition so ordinary package discovery does
    /// not attempt network access. The always-enabled environment contract test
    /// prevents an explicitly selected PostgreSQL test target from succeeding
    /// when every behavioral suite is skipped.
    static var isConfigured: Bool {
        isConfigured(environment: ProcessInfo.processInfo.environment)
    }

    static func isConfigured(environment: [String: String]) -> Bool {
        guard let host = environment["POSTGRES_TEST_HOST"] else {
            return false
        }
        return !host.isEmpty
    }

    /// Create a fresh engine. Each call creates a new engine and connection pool.
    ///
    /// Callers must await `engine.waitUntilShutdown()` before the test ends so
    /// a later test never overlaps this engine's connection-pool cleanup.
    static func makeEngine() async throws -> PostgreSQLStorageEngine {
        let configuration = try makeConfiguration()
        return try await PostgreSQLStorageEngine(configuration: configuration)
    }

    static func makeConfiguration(
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage")
    ) throws -> PostgreSQLConfiguration {
        try makeConfiguration(
            environment: ProcessInfo.processInfo.environment,
            backgroundLogger: backgroundLogger
        )
    }

    static func makeConfiguration(
        environment: [String: String],
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage")
    ) throws -> PostgreSQLConfiguration {
        guard let host = environment["POSTGRES_TEST_HOST"],
              !host.isEmpty else {
            throw PostgreSQLTestEnvironmentError.hostNotConfigured
        }
        let port: Int
        if let portValue = environment["POSTGRES_TEST_PORT"] {
            guard let parsedPort = Int(portValue), (1...65_535).contains(parsedPort) else {
                throw PostgreSQLTestEnvironmentError.invalidPort(portValue)
            }
            port = parsedPort
        } else {
            port = 5_432
        }
        let user = environment["POSTGRES_TEST_USER"] ?? "postgres"
        let password = environment["POSTGRES_TEST_PASSWORD"] ?? ""
        let database = environment["POSTGRES_TEST_DB"] ?? "storage_kit_test"

        return PostgreSQLConfiguration(
            host: host,
            port: port,
            username: user,
            password: password,
            database: database,
            backgroundLogger: backgroundLogger
        )
    }
}

enum PostgreSQLTestEnvironmentError: Error, Equatable {
    case hostNotConfigured
    case invalidPort(String)
}
