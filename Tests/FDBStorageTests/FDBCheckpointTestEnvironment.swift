import Foundation
import StorageKit

/// Refuses fixed-key fixtures outside the run-owned isolated FoundationDB cluster.
enum FDBCheckpointTestEnvironment {
    static func clusterFilePath() throws -> String {
        let environment = ProcessInfo.processInfo.environment
        guard let cluster = environment["FDB_CLUSTER_FILE"], !cluster.isEmpty,
              let token = environment["DATABASE_FRAMEWORK_FDB_HARNESS_TOKEN"], !token.isEmpty,
              let identityPath = environment["DATABASE_FRAMEWORK_FDB_HARNESS_IDENTITY"], !identityPath.isEmpty else {
            throw failure("Checkpoint integration requires the isolated FoundationDB harness environment")
        }
        let identity = try String(contentsOfFile: identityPath, encoding: .utf8)
        let fields = identity.split(separator: "\n").reduce(into: [String: String]()) { result, line in
            let parts = line.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            if parts.count == 2 { result[String(parts[0])] = String(parts[1]) }
        }
        guard fields["format"] == "database-framework-docker-foundationdb-v1",
              fields["token"] == token, fields["cluster_file"] == cluster else {
            throw failure("Checkpoint integration cluster does not match its isolated harness identity")
        }
        return cluster
    }

    private static func failure(_ message: String) -> StorageError {
        StorageError(code: .invalidOperation, operation: .open, backend: .foundationDB, message: message)
    }
}
