import PostgresNIO
import Logging

/// Transaction isolation level for PostgreSQL.
///
/// FoundationDB provides serializable isolation by default.
/// To maintain consistency when using PostgreSQL as a backend for
/// database-framework, the default is `.serializable`.
public enum PostgreSQLIsolationLevel: String, Sendable {
    /// READ COMMITTED — weakest level; phantom reads possible.
    /// Only use for read-heavy workloads where consistency is not critical.
    case readCommitted = "BEGIN ISOLATION LEVEL READ COMMITTED"

    /// REPEATABLE READ — no phantom reads within a transaction.
    case repeatableRead = "BEGIN ISOLATION LEVEL REPEATABLE READ"

    /// SERIALIZABLE — strongest level; matches FoundationDB semantics.
    /// Concurrent conflicting transactions will be retried automatically.
    case serializable = "BEGIN ISOLATION LEVEL SERIALIZABLE"
}

/// Configuration for PostgreSQLStorageEngine.
///
/// Wraps `PostgresClient.Configuration` and adds storage-engine-specific options.
///
/// ## Usage
/// ```swift
/// let config = PostgreSQLConfiguration(
///     host: "localhost",
///     username: "postgres",
///     password: "secret",
///     database: "mydb"
/// )
/// let engine = try await PostgreSQLStorageEngine(configuration: config)
/// ```
///
/// ## Isolation Level
///
/// Default is `.serializable` to match FoundationDB's transaction guarantees.
/// This means concurrent conflicting transactions will receive serialization
/// failures (40001), which the engine retries automatically.
///
/// Only lower the isolation level if you understand the consistency trade-offs:
/// ```swift
/// let config = PostgreSQLConfiguration(
///     host: "localhost",
///     username: "postgres",
///     password: "secret",
///     database: "mydb",
///     isolationLevel: .readCommitted  // weaker but fewer conflicts
/// )
/// ```
public struct PostgreSQLConfiguration: Sendable {

    /// PostgresNIO client configuration (connection pool settings).
    public let clientConfiguration: PostgresClient.Configuration

    /// Logger used for background pool operations and query logging.
    public var backgroundLogger: Logger

    /// Transaction isolation level.
    ///
    /// Default: `.serializable` (matches FoundationDB semantics).
    public var isolationLevel: PostgreSQLIsolationLevel

    /// SQL statement used to begin a transaction.
    /// Derived from `isolationLevel`.
    var beginStatement: String { isolationLevel.rawValue }

    /// Maximum retry attempts for transient errors in `withTransaction`.
    ///
    /// Note: When used via database-framework's `TransactionRunner`,
    /// `TransactionConfiguration.retryLimit` takes precedence for the
    /// outer retry loop. This value controls the engine-level retry
    /// in `withTransaction()` only.
    public var maxRetries: Int

    /// TCP connection configuration.
    public init(
        host: String,
        port: Int = 5432,
        username: String,
        password: String? = nil,
        database: String? = nil,
        tls: PostgresClient.Configuration.TLS = .disable,
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage"),
        isolationLevel: PostgreSQLIsolationLevel = .serializable,
        maxRetries: Int = 5
    ) {
        self.clientConfiguration = PostgresClient.Configuration(
            host: host,
            port: port,
            username: username,
            password: password,
            database: database,
            tls: tls
        )
        self.backgroundLogger = backgroundLogger
        self.isolationLevel = isolationLevel
        self.maxRetries = maxRetries
    }
}
