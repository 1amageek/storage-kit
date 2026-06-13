import Foundation
import PostgresNIO
import Logging
import StorageKit

extension PostgreSQLConfiguration {
    /// Environment variable names used by `cloudRunProduction(environment:)`.
    public enum ProductionEnvironmentKey {
        public static let cloudSQLInstanceConnectionName = "STORAGE_KIT_POSTGRES_CLOUD_SQL_CONNECTION_NAME"
        public static let username = "STORAGE_KIT_POSTGRES_USER"
        public static let password = "STORAGE_KIT_POSTGRES_PASSWORD"
        public static let database = "STORAGE_KIT_POSTGRES_DATABASE"
        public static let tableName = "STORAGE_KIT_POSTGRES_TABLE"
        public static let schemaManagement = "STORAGE_KIT_POSTGRES_SCHEMA_MANAGEMENT"
        public static let poolMaximumConnections = "STORAGE_KIT_POSTGRES_POOL_MAX_CONNECTIONS"
        public static let poolMinimumConnections = "STORAGE_KIT_POSTGRES_POOL_MIN_CONNECTIONS"
        public static let connectTimeoutSeconds = "STORAGE_KIT_POSTGRES_CONNECT_TIMEOUT_SECONDS"
        public static let cloudRunMaxInstances = "STORAGE_KIT_CLOUD_RUN_MAX_INSTANCES"
        public static let cloudSQLMaxConnections = "STORAGE_KIT_CLOUD_SQL_MAX_CONNECTIONS"
        public static let cloudSQLReservedConnections = "STORAGE_KIT_CLOUD_SQL_RESERVED_CONNECTIONS"
    }

    /// Build a Cloud Run + Cloud SQL production configuration from explicit values.
    public static func cloudRunProduction(
        cloudSQLInstanceConnectionName: String,
        username: String,
        password: String? = nil,
        database: String,
        connectionBudget: PostgreSQLConnectionBudget,
        poolMinimumConnections: Int = 0,
        connectTimeout: Duration = .seconds(10),
        socketDirectory: String = "/cloudsql",
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage"),
        isolationLevel: PostgreSQLIsolationLevel = .serializable,
        tableName: String = "kv_store",
        schemaManagement: SchemaManagement = .assumeExists
    ) throws -> PostgreSQLConfiguration {
        try connectionBudget.validate()
        try validateCloudSQLInstanceConnectionName(cloudSQLInstanceConnectionName)
        try validateNonBlank(username, name: "username")
        try validateNonBlank(database, name: "database")
        try validateSocketDirectory(socketDirectory)
        try PostgreSQLStorageEngine.validateTableName(tableName)
        guard poolMinimumConnections >= 0 else {
            throw invalidProductionConfiguration("poolMinimumConnections must not be negative")
        }
        guard poolMinimumConnections <= connectionBudget.connectionsPerInstance else {
            throw invalidProductionConfiguration(
                "poolMinimumConnections must not exceed connectionsPerInstance"
            )
        }

        var clientConfiguration = PostgresClient.Configuration(
            unixSocketPath: cloudSQLUnixSocketPath(
                instanceConnectionName: cloudSQLInstanceConnectionName,
                socketDirectory: socketDirectory
            ),
            username: username,
            password: password,
            database: database
        )
        clientConfiguration.options.maximumConnections = connectionBudget.connectionsPerInstance
        clientConfiguration.options.minimumConnections = poolMinimumConnections
        clientConfiguration.options.connectTimeout = connectTimeout

        return PostgreSQLConfiguration(
            clientConfiguration: clientConfiguration,
            backgroundLogger: backgroundLogger,
            isolationLevel: isolationLevel,
            tableName: tableName,
            schemaManagement: schemaManagement
        )
    }

    /// Build a Cloud Run + Cloud SQL production configuration from environment values.
    public static func cloudRunProduction(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        backgroundLogger: Logger = Logger(label: "PostgreSQLStorage")
    ) throws -> PostgreSQLConfiguration {
        let keys = ProductionEnvironmentKey.self
        let poolMaximumConnections = try positiveInt(
            environment[keys.poolMaximumConnections],
            key: keys.poolMaximumConnections
        )
        let cloudRunMaxInstances = try positiveInt(
            environment[keys.cloudRunMaxInstances],
            key: keys.cloudRunMaxInstances
        )
        let cloudSQLMaxConnections = try positiveInt(
            environment[keys.cloudSQLMaxConnections],
            key: keys.cloudSQLMaxConnections
        )
        let poolMinimumConnections = try nonNegativeInt(
            environment[keys.poolMinimumConnections],
            key: keys.poolMinimumConnections,
            defaultValue: 0
        )
        let reservedConnections = try nonNegativeInt(
            environment[keys.cloudSQLReservedConnections],
            key: keys.cloudSQLReservedConnections,
            defaultValue: 10
        )
        let connectTimeoutSeconds = try positiveInt(
            environment[keys.connectTimeoutSeconds],
            key: keys.connectTimeoutSeconds,
            defaultValue: 10
        )
        let schemaManagement = try parseSchemaManagement(
            environment[keys.schemaManagement] ?? "assumeExists",
            key: keys.schemaManagement
        )
        let tableName = try optionalNonBlank(
            environment[keys.tableName],
            key: keys.tableName,
            defaultValue: "kv_store"
        )

        return try cloudRunProduction(
            cloudSQLInstanceConnectionName: required(
                environment[keys.cloudSQLInstanceConnectionName],
                key: keys.cloudSQLInstanceConnectionName
            ),
            username: required(environment[keys.username], key: keys.username),
            password: emptyToNil(environment[keys.password]),
            database: required(environment[keys.database], key: keys.database),
            connectionBudget: PostgreSQLConnectionBudget(
                cloudRunMaxInstances: cloudRunMaxInstances,
                connectionsPerInstance: poolMaximumConnections,
                cloudSQLMaxConnections: cloudSQLMaxConnections,
                reservedConnections: reservedConnections
            ),
            poolMinimumConnections: poolMinimumConnections,
            connectTimeout: .seconds(connectTimeoutSeconds),
            backgroundLogger: backgroundLogger,
            tableName: tableName,
            schemaManagement: schemaManagement
        )
    }

    private static func required(_ value: String?, key: String) throws -> String {
        guard let value else {
            throw invalidProductionConfiguration("missing required environment value \(key)")
        }
        try validateNonBlank(value, name: key)
        return value
    }

    private static func emptyToNil(_ value: String?) -> String? {
        guard let value, !isBlank(value) else { return nil }
        return value
    }

    private static func positiveInt(
        _ value: String?,
        key: String,
        defaultValue: Int? = nil
    ) throws -> Int {
        guard let value else {
            if let defaultValue { return defaultValue }
            throw invalidProductionConfiguration("missing required environment value \(key)")
        }
        guard !isBlank(value) else {
            throw invalidProductionConfiguration("\(key) must be a positive integer")
        }
        guard let parsed = Int(value), parsed > 0 else {
            throw invalidProductionConfiguration("\(key) must be a positive integer")
        }
        return parsed
    }

    private static func nonNegativeInt(
        _ value: String?,
        key: String,
        defaultValue: Int
    ) throws -> Int {
        guard let value else { return defaultValue }
        guard !isBlank(value) else {
            throw invalidProductionConfiguration("\(key) must be a non-negative integer")
        }
        guard let parsed = Int(value), parsed >= 0 else {
            throw invalidProductionConfiguration("\(key) must be a non-negative integer")
        }
        return parsed
    }

    private static func optionalNonBlank(
        _ value: String?,
        key: String,
        defaultValue: String
    ) throws -> String {
        guard let value else { return defaultValue }
        try validateNonBlank(value, name: key)
        return value
    }

    private static func parseSchemaManagement(
        _ value: String,
        key: String
    ) throws -> SchemaManagement {
        switch value {
        case "createIfNeeded":
            return .createIfNeeded
        case "assumeExists":
            return .assumeExists
        default:
            throw invalidProductionConfiguration(
                "\(key) must be either createIfNeeded or assumeExists"
            )
        }
    }

    private static func invalidProductionConfiguration(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .initialize,
            backend: .postgreSQL,
            message: "Invalid PostgreSQL production configuration: \(message)"
        )
    }

    private static func validateCloudSQLInstanceConnectionName(_ value: String) throws {
        try validateNonBlank(value, name: "cloudSQLInstanceConnectionName")
        guard !value.contains("/") else {
            throw invalidProductionConfiguration("cloudSQLInstanceConnectionName must not contain '/'")
        }
        guard value.rangeOfCharacter(from: .whitespacesAndNewlines) == nil else {
            throw invalidProductionConfiguration("cloudSQLInstanceConnectionName must not contain whitespace")
        }
    }

    private static func validateSocketDirectory(_ value: String) throws {
        try validateNonBlank(value, name: "socketDirectory")
        guard value.hasPrefix("/") else {
            throw invalidProductionConfiguration("socketDirectory must be an absolute path")
        }
    }

    private static func validateNonBlank(_ value: String, name: String) throws {
        guard !isBlank(value) else {
            throw invalidProductionConfiguration("\(name) must not be empty")
        }
    }

    private static func isBlank(_ value: String) -> Bool {
        value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }
}
