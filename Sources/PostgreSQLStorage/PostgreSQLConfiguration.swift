import PostgresNIO
import Logging

/// Transaction isolation level for PostgreSQL.
///
/// FoundationDB provides serializable isolation by default.
/// To maintain consistency when using PostgreSQL as a backend for
/// database-framework, the default is `.serializable`.
public enum PostgreSQLIsolationLevel: Sendable, Hashable {
    /// READ COMMITTED — weakest level; phantom reads possible.
    /// Only use for read-heavy workloads where consistency is not critical.
    case readCommitted

    /// REPEATABLE READ — no phantom reads within a transaction.
    case repeatableRead

    /// SERIALIZABLE — strongest level; matches FoundationDB semantics.
    /// Concurrent conflicting transactions will be retried automatically.
    case serializable

    /// SQL name used in `SET TRANSACTION ISOLATION LEVEL` / `BEGIN`.
    public var sqlName: String {
        switch self {
        case .readCommitted: return "READ COMMITTED"
        case .repeatableRead: return "REPEATABLE READ"
        case .serializable: return "SERIALIZABLE"
        }
    }
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
/// failures (40001), which higher-level transaction runners can retry.
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

    /// How the engine manages the KV table schema at initialization.
    public enum SchemaManagement: Sendable, Hashable {
        /// Issue `CREATE TABLE IF NOT EXISTS` at engine initialization.
        case createIfNeeded
        /// Assume the table already exists. Use for pre-provisioned databases
        /// where the connecting role lacks DDL privileges (e.g. IAM-managed
        /// Cloud SQL users restricted to DML).
        case assumeExists
    }

    /// PostgresNIO client configuration (connection pool settings).
    public let clientConfiguration: PostgresClient.Configuration

    /// Logger used for background pool operations and query logging.
    public var backgroundLogger: Logger

    /// Transaction isolation level.
    ///
    /// Default: `.serializable` (matches FoundationDB semantics).
    public var isolationLevel: PostgreSQLIsolationLevel

    /// Name of the KV table.
    ///
    /// Must be a bare SQL identifier (letters, digits, underscores; not
    /// starting with a digit; at most 63 bytes). Validated at engine
    /// initialization because the name is interpolated into SQL text.
    public var tableName: String

    /// Schema management strategy. Default: `.createIfNeeded`.
    public var schemaManagement: SchemaManagement

    /// SQL statement used to begin a transaction.
    /// Derived from `isolationLevel`.
    var beginStatement: String {
        "BEGIN ISOLATION LEVEL \(isolationLevel.sqlName)"
    }

    /// Create a configuration from a fully customized PostgresNIO client configuration.
    ///
    /// Use this when deployment-specific settings such as connection pool size,
    /// startup parameters, or TLS server names need to be controlled by the app.
    public init(
        clientConfiguration: PostgresClient.Configuration,
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage"),
        isolationLevel: PostgreSQLIsolationLevel = .serializable,
        tableName: String = "kv_store",
        schemaManagement: SchemaManagement = .createIfNeeded
    ) {
        self.clientConfiguration = clientConfiguration
        self.backgroundLogger = backgroundLogger
        self.isolationLevel = isolationLevel
        self.tableName = tableName
        self.schemaManagement = schemaManagement
    }

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
        tableName: String = "kv_store",
        schemaManagement: SchemaManagement = .createIfNeeded
    ) {
        self.init(
            clientConfiguration: PostgresClient.Configuration(
                host: host,
                port: port,
                username: username,
                password: password,
                database: database,
                tls: tls
            ),
            backgroundLogger: backgroundLogger,
            isolationLevel: isolationLevel,
            tableName: tableName,
            schemaManagement: schemaManagement
        )
    }

    /// Unix domain socket connection configuration.
    public init(
        unixSocketPath: String,
        username: String,
        password: String? = nil,
        database: String? = nil,
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage"),
        isolationLevel: PostgreSQLIsolationLevel = .serializable,
        tableName: String = "kv_store",
        schemaManagement: SchemaManagement = .createIfNeeded
    ) {
        self.init(
            clientConfiguration: PostgresClient.Configuration(
                unixSocketPath: unixSocketPath,
                username: username,
                password: password,
                database: database
            ),
            backgroundLogger: backgroundLogger,
            isolationLevel: isolationLevel,
            tableName: tableName,
            schemaManagement: schemaManagement
        )
    }

    /// Cloud SQL Unix socket connection configuration.
    ///
    /// Cloud Run mounts Cloud SQL PostgreSQL sockets under:
    /// `/cloudsql/PROJECT:REGION:INSTANCE/.s.PGSQL.5432`.
    public init(
        cloudSQLInstanceConnectionName: String,
        username: String,
        password: String? = nil,
        database: String? = nil,
        port: Int = 5432,
        socketDirectory: String = "/cloudsql",
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage"),
        isolationLevel: PostgreSQLIsolationLevel = .serializable,
        tableName: String = "kv_store",
        schemaManagement: SchemaManagement = .createIfNeeded
    ) {
        self.init(
            unixSocketPath: Self.cloudSQLUnixSocketPath(
                instanceConnectionName: cloudSQLInstanceConnectionName,
                port: port,
                socketDirectory: socketDirectory
            ),
            username: username,
            password: password,
            database: database,
            backgroundLogger: backgroundLogger,
            isolationLevel: isolationLevel,
            tableName: tableName,
            schemaManagement: schemaManagement
        )
    }

    /// Build the Unix socket path used by Cloud SQL PostgreSQL on Cloud Run.
    public static func cloudSQLUnixSocketPath(
        instanceConnectionName: String,
        port: Int = 5432,
        socketDirectory: String = "/cloudsql"
    ) -> String {
        let directory = socketDirectory.hasSuffix("/")
            ? String(socketDirectory.dropLast())
            : socketDirectory
        return "\(directory)/\(instanceConnectionName)/.s.PGSQL.\(port)"
    }
}
