/// Successful PostgreSQL readiness check result.
public struct PostgreSQLReadinessReport: Sendable, Hashable {
    public let tableName: String
    public let schemaManagement: PostgreSQLConfiguration.SchemaManagement

    public init(
        tableName: String,
        schemaManagement: PostgreSQLConfiguration.SchemaManagement
    ) {
        self.tableName = tableName
        self.schemaManagement = schemaManagement
    }
}
