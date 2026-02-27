import StorageKit
import FoundationDB

/// Directory service using FDB DirectoryLayer.
///
/// FDB's DirectoryLayer uses the High Contention Allocator (HCA)
/// to dynamically assign short prefixes.
public final class FDBDirectoryService: DirectoryService, @unchecked Sendable {

    private let database: any DatabaseProtocol

    public init(database: any DatabaseProtocol) {
        self.database = database
    }

    public func createOrOpen(path: [String]) async throws -> StorageKit.Subspace {
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path)
        return StorageKit.Subspace(prefix: dirSubspace.subspace.prefix)
    }

    public func list(path: [String]) async throws -> [String] {
        let directoryLayer = DirectoryLayer(database: database)
        return try await directoryLayer.list(path: path)
    }

    public func remove(path: [String]) async throws {
        let directoryLayer = DirectoryLayer(database: database)
        try await directoryLayer.remove(path: path)
    }

    public func exists(path: [String]) async throws -> Bool {
        let directoryLayer = DirectoryLayer(database: database)
        return try await directoryLayer.exists(path: path)
    }
}
