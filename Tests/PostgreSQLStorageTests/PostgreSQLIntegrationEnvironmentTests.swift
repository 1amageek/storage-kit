import Testing

@Suite("PostgreSQL Integration Environment")
struct PostgreSQLIntegrationEnvironmentTests {
    @Test("PostgreSQL integration endpoint is explicitly configured")
    func endpointIsConfigured() {
        #expect(
            PostgreSQLTestEnvironment.isConfigured,
            "Set POSTGRES_TEST_HOST before running PostgreSQLStorageTests."
        )
    }

    @Test("Empty hosts are not configured")
    func emptyHostIsNotConfigured() {
        #expect(
            !PostgreSQLTestEnvironment.isConfigured(
                environment: ["POSTGRES_TEST_HOST": ""]
            )
        )
    }

    @Test(
        "Malformed PostgreSQL ports fail explicitly",
        arguments: ["", "0", "65536", "not-a-port"]
    )
    func malformedPortFailsExplicitly(_ port: String) {
        #expect(throws: PostgreSQLTestEnvironmentError.invalidPort(port)) {
            try PostgreSQLTestEnvironment.makeConfiguration(
                environment: [
                    "POSTGRES_TEST_HOST": "database.test",
                    "POSTGRES_TEST_PORT": port,
                ]
            )
        }
    }

    @Test("Configuration applies documented defaults")
    func configurationAppliesDefaults() throws {
        let configuration = try PostgreSQLTestEnvironment.makeConfiguration(
            environment: ["POSTGRES_TEST_HOST": "database.test"]
        )
        let clientConfiguration = configuration.clientConfiguration

        #expect(clientConfiguration.host == "database.test")
        #expect(clientConfiguration.port == 5_432)
        #expect(clientConfiguration.username == "postgres")
        #expect(clientConfiguration.password == "")
        #expect(clientConfiguration.database == "storage_kit_test")
    }
}
