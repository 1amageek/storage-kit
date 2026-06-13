import Testing
@testable import PostgreSQLStorage
@testable import StorageKit

extension AllPostgreSQLTests {
    @Suite("PostgreSQL Configuration Tests")
    struct PostgreSQLConfigurationTests {
        @Test func cloudSQLUnixSocketPathUsesCloudRunLayout() {
            let path = PostgreSQLConfiguration.cloudSQLUnixSocketPath(
                instanceConnectionName: "project:region:instance"
            )

            #expect(path == "/cloudsql/project:region:instance/.s.PGSQL.5432")
        }

        @Test func cloudSQLUnixSocketPathNormalizesTrailingSlash() {
            let path = PostgreSQLConfiguration.cloudSQLUnixSocketPath(
                instanceConnectionName: "project:region:instance",
                socketDirectory: "/cloudsql/"
            )

            #expect(path == "/cloudsql/project:region:instance/.s.PGSQL.5432")
        }

        @Test func cloudSQLUnixSocketPathUsesCustomPort() {
            let path = PostgreSQLConfiguration.cloudSQLUnixSocketPath(
                instanceConnectionName: "project:region:instance",
                port: 5433
            )

            #expect(path == "/cloudsql/project:region:instance/.s.PGSQL.5433")
        }

        @Test func cloudSQLInitializerBuildsUnixSocketConfiguration() {
            let configuration = PostgreSQLConfiguration(
                cloudSQLInstanceConnectionName: "project:region:instance",
                username: "app_user",
                password: "secret",
                database: "app_db"
            )

            #expect(configuration.clientConfiguration.unixSocketPath == "/cloudsql/project:region:instance/.s.PGSQL.5432")
            #expect(configuration.clientConfiguration.username == "app_user")
            #expect(configuration.clientConfiguration.password == "secret")
            #expect(configuration.clientConfiguration.database == "app_db")
        }

        @Test func cloudRunProductionBuildsBoundedPoolConfiguration() throws {
            let configuration = try PostgreSQLConfiguration.cloudRunProduction(
                cloudSQLInstanceConnectionName: "project:region:instance",
                username: "app_user",
                password: "secret",
                database: "app_db",
                connectionBudget: PostgreSQLConnectionBudget(
                    cloudRunMaxInstances: 10,
                    connectionsPerInstance: 4,
                    cloudSQLMaxConnections: 60,
                    reservedConnections: 10
                ),
                poolMinimumConnections: 1,
                tableName: "prod_kv",
                schemaManagement: .assumeExists
            )

            #expect(configuration.clientConfiguration.unixSocketPath == "/cloudsql/project:region:instance/.s.PGSQL.5432")
            #expect(configuration.clientConfiguration.options.maximumConnections == 4)
            #expect(configuration.clientConfiguration.options.minimumConnections == 1)
            #expect(configuration.tableName == "prod_kv")
            #expect(configuration.schemaManagement == .assumeExists)
        }

        @Test func cloudRunProductionRejectsConnectionBudgetOverflow() {
            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(
                    cloudSQLInstanceConnectionName: "project:region:instance",
                    username: "app_user",
                    database: "app_db",
                    connectionBudget: PostgreSQLConnectionBudget(
                        cloudRunMaxInstances: 20,
                        connectionsPerInstance: 10,
                        cloudSQLMaxConnections: 100,
                        reservedConnections: 20
                    )
                )
                Issue.record("Expected connection budget overflow to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionRejectsInvalidPoolMinimum() {
            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(
                    cloudSQLInstanceConnectionName: "project:region:instance",
                    username: "app_user",
                    database: "app_db",
                    connectionBudget: PostgreSQLConnectionBudget(
                        cloudRunMaxInstances: 2,
                        connectionsPerInstance: 4,
                        cloudSQLMaxConnections: 40
                    ),
                    poolMinimumConnections: 5
                )
                Issue.record("Expected invalid pool minimum to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionAcceptsPoolMinimumEqualToMaximum() throws {
            let configuration = try PostgreSQLConfiguration.cloudRunProduction(
                cloudSQLInstanceConnectionName: "project:region:instance",
                username: "app_user",
                database: "app_db",
                connectionBudget: PostgreSQLConnectionBudget(
                    cloudRunMaxInstances: 2,
                    connectionsPerInstance: 4,
                    cloudSQLMaxConnections: 20
                ),
                poolMinimumConnections: 4
            )

            #expect(configuration.clientConfiguration.options.minimumConnections == 4)
            #expect(configuration.clientConfiguration.options.maximumConnections == 4)
        }

        @Test func cloudRunProductionRejectsBlankExplicitValues() {
            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(
                    cloudSQLInstanceConnectionName: "   ",
                    username: "app_user",
                    database: "app_db",
                    connectionBudget: PostgreSQLConnectionBudget(
                        cloudRunMaxInstances: 1,
                        connectionsPerInstance: 1,
                        cloudSQLMaxConnections: 20
                    )
                )
                Issue.record("Expected blank Cloud SQL connection name to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionRejectsInvalidTableNameBeforeEngineInit() {
            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(
                    cloudSQLInstanceConnectionName: "project:region:instance",
                    username: "app_user",
                    database: "app_db",
                    connectionBudget: PostgreSQLConnectionBudget(
                        cloudRunMaxInstances: 1,
                        connectionsPerInstance: 1,
                        cloudSQLMaxConnections: 20
                    ),
                    tableName: "kv-store"
                )
                Issue.record("Expected invalid table name to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func connectionBudgetRejectsOverflowWithoutTrapping() {
            let budget = PostgreSQLConnectionBudget(
                cloudRunMaxInstances: Int.max,
                connectionsPerInstance: 2,
                cloudSQLMaxConnections: Int.max,
                reservedConnections: 0
            )

            #expect(budget.maximumApplicationConnections == Int.max)
            do {
                try budget.validate()
                Issue.record("Expected overflow to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionFromEnvironmentBuildsConfiguration() throws {
            let keys = PostgreSQLConfiguration.ProductionEnvironmentKey.self
            let environment = [
                keys.cloudSQLInstanceConnectionName: "project:region:instance",
                keys.username: "app_user",
                keys.password: "secret",
                keys.database: "app_db",
                keys.tableName: "prod_kv",
                keys.schemaManagement: "assumeExists",
                keys.poolMaximumConnections: "4",
                keys.poolMinimumConnections: "1",
                keys.connectTimeoutSeconds: "7",
                keys.cloudRunMaxInstances: "10",
                keys.cloudSQLMaxConnections: "60",
                keys.cloudSQLReservedConnections: "10"
            ]

            let configuration = try PostgreSQLConfiguration.cloudRunProduction(
                environment: environment
            )

            #expect(configuration.clientConfiguration.unixSocketPath == "/cloudsql/project:region:instance/.s.PGSQL.5432")
            #expect(configuration.clientConfiguration.username == "app_user")
            #expect(configuration.clientConfiguration.password == "secret")
            #expect(configuration.clientConfiguration.database == "app_db")
            #expect(configuration.clientConfiguration.options.maximumConnections == 4)
            #expect(configuration.clientConfiguration.options.minimumConnections == 1)
            #expect(configuration.tableName == "prod_kv")
            #expect(configuration.schemaManagement == .assumeExists)
        }

        @Test func cloudRunProductionFromEnvironmentRejectsMissingRequiredValue() {
            let keys = PostgreSQLConfiguration.ProductionEnvironmentKey.self
            let environment = [
                keys.username: "app_user",
                keys.database: "app_db",
                keys.poolMaximumConnections: "4",
                keys.cloudRunMaxInstances: "10",
                keys.cloudSQLMaxConnections: "60"
            ]

            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(environment: environment)
                Issue.record("Expected missing environment value to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionFromEnvironmentRejectsBlankOptionalInteger() {
            let keys = PostgreSQLConfiguration.ProductionEnvironmentKey.self
            let environment = [
                keys.cloudSQLInstanceConnectionName: "project:region:instance",
                keys.username: "app_user",
                keys.database: "app_db",
                keys.poolMaximumConnections: "4",
                keys.poolMinimumConnections: " ",
                keys.cloudRunMaxInstances: "10",
                keys.cloudSQLMaxConnections: "60"
            ]

            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(environment: environment)
                Issue.record("Expected blank integer environment value to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionFromEnvironmentRejectsBlankTableName() {
            let keys = PostgreSQLConfiguration.ProductionEnvironmentKey.self
            let environment = [
                keys.cloudSQLInstanceConnectionName: "project:region:instance",
                keys.username: "app_user",
                keys.database: "app_db",
                keys.tableName: " ",
                keys.poolMaximumConnections: "4",
                keys.cloudRunMaxInstances: "10",
                keys.cloudSQLMaxConnections: "60"
            ]

            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(environment: environment)
                Issue.record("Expected blank table name to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func cloudRunProductionFromEnvironmentRejectsInvalidSchemaManagement() {
            let keys = PostgreSQLConfiguration.ProductionEnvironmentKey.self
            let environment = [
                keys.cloudSQLInstanceConnectionName: "project:region:instance",
                keys.username: "app_user",
                keys.database: "app_db",
                keys.schemaManagement: "migrateAtBoot",
                keys.poolMaximumConnections: "4",
                keys.cloudRunMaxInstances: "10",
                keys.cloudSQLMaxConnections: "60"
            ]

            do {
                _ = try PostgreSQLConfiguration.cloudRunProduction(environment: environment)
                Issue.record("Expected invalid schema management value to throw")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .initialize)
                #expect(error.backend == .postgreSQL)
            } catch {
                Issue.record("Expected StorageError")
            }
        }

        @Test func validateTableName_acceptsSafeBareIdentifiers() throws {
            let names = [
                "kv_store",
                "_kv1",
                String(repeating: "a", count: 63)
            ]

            for name in names {
                try PostgreSQLStorageEngine.validateTableName(name)
            }
        }

        @Test func validateTableName_rejectsUnsafeIdentifiers() throws {
            let names = [
                "",
                "1table",
                "table-name",
                "table name",
                "table;drop",
                String(repeating: "a", count: 64),
                "é"
            ]

            for name in names {
                do {
                    try PostgreSQLStorageEngine.validateTableName(name)
                    Issue.record("Expected invalid table name to throw")
                } catch let error as StorageError {
                    #expect(error.code == .invalidOperation)
                    #expect(error.operation == .initialize)
                    #expect(error.backend == .postgreSQL)
                }
            }
        }

        @Test func mapSQLState_connectionClassIsRetryableConnectionFailure() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "08006",
                serverMessage: "connection failure",
                fallbackDescription: "fallback",
                operation: .read
            )

            #expect(mapped.code == .connectionFailure)
            #expect(mapped.operation == .read)
            #expect(mapped.backend == .postgreSQL)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_commitConnectionClassIsCommitUnknown() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "08003",
                serverMessage: "connection does not exist",
                fallbackDescription: "fallback",
                operation: .commit
            )

            #expect(mapped.code == .commitUnknownResult)
            #expect(mapped.operation == .commit)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_serverShutdownIsRetryableConnectionFailure() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "57P01",
                serverMessage: "admin shutdown",
                fallbackDescription: "fallback",
                operation: .write
            )

            #expect(mapped.code == .connectionFailure)
            #expect(mapped.operation == .write)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_commitServerShutdownIsCommitUnknown() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "57P01",
                serverMessage: "admin shutdown",
                fallbackDescription: "fallback",
                operation: .commit
            )

            #expect(mapped.code == .commitUnknownResult)
            #expect(mapped.operation == .commit)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_serializationFailureIsRetryableConflict() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "40001",
                serverMessage: "could not serialize access due to concurrent update",
                fallbackDescription: "fallback",
                operation: .commit
            )

            #expect(mapped.code == .transactionConflict)
            #expect(mapped.operation == .commit)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_uniqueViolationIsRetryableConflict() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "23505",
                serverMessage: "duplicate key value violates unique constraint",
                fallbackDescription: "fallback",
                operation: .write
            )

            #expect(mapped.code == .transactionConflict)
            #expect(mapped.operation == .write)
            #expect(mapped.isRetryable)
        }

        @Test func mapSQLState_programLimitIsBackendFailure() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "54000",
                serverMessage: "index row size exceeds btree version 4 maximum",
                fallbackDescription: "fallback",
                operation: .write
            )

            #expect(mapped.code == .backendFailure)
            #expect(mapped.operation == .write)
            #expect(!mapped.isRetryable)
            #expect(mapped.underlyingDescription?.contains("sqlState=54000") == true)
        }

        @Test func mapSQLState_unknownStatePreservesSQLState() {
            let mapped = PostgreSQLStorageEngine.mapSQLState(
                "XX000",
                serverMessage: "internal error",
                fallbackDescription: "fallback",
                operation: .execute
            )

            #expect(mapped.code == .backendFailure)
            #expect(mapped.operation == .execute)
            #expect(!mapped.isRetryable)
            #expect(mapped.underlyingDescription?.contains("sqlState=XX000") == true)
        }

        @Test func advisoryLockID_isDeterministicForSameKey() {
            let first = PostgreSQLStorageTransaction.advisoryLockID(for: [0x01, 0x02, 0x03])
            let second = PostgreSQLStorageTransaction.advisoryLockID(for: [0x01, 0x02, 0x03])

            #expect(first == second)
        }

        @Test func advisoryLockID_distinguishesDifferentKeys() {
            let first = PostgreSQLStorageTransaction.advisoryLockID(for: [0x01, 0x02, 0x03])
            let second = PostgreSQLStorageTransaction.advisoryLockID(for: [0x01, 0x02, 0x04])

            #expect(first != second)
        }

        @Test func advisoryLockID_emptyKeyIsStable() {
            let first = PostgreSQLStorageTransaction.advisoryLockID(for: [])
            let second = PostgreSQLStorageTransaction.advisoryLockID(for: [])

            #expect(first == second)
        }
    }
}
