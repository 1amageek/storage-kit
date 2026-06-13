import StorageKit

/// Connection budget for serverless PostgreSQL deployments.
///
/// Cloud Run can scale horizontally, so the effective database connection demand
/// is `cloudRunMaxInstances * connectionsPerInstance`. Validate this before
/// building a production configuration so overload is caught at boot/deploy time
/// instead of at peak traffic.
public struct PostgreSQLConnectionBudget: Sendable, Hashable {
    public var cloudRunMaxInstances: Int
    public var connectionsPerInstance: Int
    public var cloudSQLMaxConnections: Int
    public var reservedConnections: Int

    public init(
        cloudRunMaxInstances: Int,
        connectionsPerInstance: Int,
        cloudSQLMaxConnections: Int,
        reservedConnections: Int = 10
    ) {
        self.cloudRunMaxInstances = cloudRunMaxInstances
        self.connectionsPerInstance = connectionsPerInstance
        self.cloudSQLMaxConnections = cloudSQLMaxConnections
        self.reservedConnections = reservedConnections
    }

    public var maximumApplicationConnections: Int {
        let result = cloudRunMaxInstances.multipliedReportingOverflow(by: connectionsPerInstance)
        return result.overflow ? Int.max : result.partialValue
    }

    public var usableCloudSQLConnections: Int {
        let result = cloudSQLMaxConnections.subtractingReportingOverflow(reservedConnections)
        guard result.overflow else { return result.partialValue }
        return reservedConnections < 0 ? Int.max : Int.min
    }

    public func validate() throws {
        guard cloudRunMaxInstances > 0 else {
            throw Self.invalid("cloudRunMaxInstances must be greater than zero")
        }
        guard connectionsPerInstance > 0 else {
            throw Self.invalid("connectionsPerInstance must be greater than zero")
        }
        let product = cloudRunMaxInstances.multipliedReportingOverflow(by: connectionsPerInstance)
        guard !product.overflow else {
            throw Self.invalid("maximum application connections overflow Int")
        }
        guard cloudSQLMaxConnections > 0 else {
            throw Self.invalid("cloudSQLMaxConnections must be greater than zero")
        }
        guard reservedConnections >= 0 else {
            throw Self.invalid("reservedConnections must not be negative")
        }
        guard usableCloudSQLConnections > 0 else {
            throw Self.invalid("reservedConnections must be lower than cloudSQLMaxConnections")
        }
        guard maximumApplicationConnections <= usableCloudSQLConnections else {
            throw Self.invalid(
                "maximum application connections (\(maximumApplicationConnections)) exceed "
                    + "usable Cloud SQL connections (\(usableCloudSQLConnections))"
            )
        }
    }

    private static func invalid(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .initialize,
            backend: .postgreSQL,
            message: "Invalid PostgreSQL connection budget: \(message)"
        )
    }
}
