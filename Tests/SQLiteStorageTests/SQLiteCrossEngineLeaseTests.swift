import DatabaseTypes
import Foundation
import Testing
@testable import SQLiteStorage
@testable import StorageKit

/// Verifies that a `PartitionLease` is answered by the store rather than by the
/// engine instance that issued it.
///
/// This is the property a process-local registration could not have: the
/// removal below is committed by a second engine over the same database file,
/// which the issuing engine never observes in memory. The lease still fails,
/// because every bind re-resolves the Partition in the caller's transaction and
/// the prefix allocator never reissues a number.
@Suite("SQLite cross-engine lease")
struct SQLiteCrossEngineLeaseTests {

    @Test(.timeLimit(.minutes(1)))
    func aPartitionReplacedByAnotherEngineInvalidatesTheLease() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }

        let holder = try SQLiteStorageEngine(configuration: .file(path))
        let remover = try SQLiteStorageEngine(configuration: .file(path))

        let tenant = try await holder.withTransaction { transaction -> Partition in
            let root = try await holder.directoryAccess.openOrInitializeRoot(transaction: transaction)
            return try await holder.directoryAccess.openOrCreatePartition(
                "tenant",
                in: root,
                transaction: transaction
            )
        }

        let issuing = try holder.createOwnedTransaction()
        let lease = try await holder.leasePartition(tenant, transaction: issuing)
        try await lease.withWriteAccess(issuing) { access in
            try access.setValue([0x01], for: ByteString(Array(tenant.root.root.prefix) + [0x6B]))
        }
        try await issuing.commit()

        // A second engine over the same file replaces the Partition. The
        // holder's process learns nothing about it.
        let recreated = try await remover.withTransaction { transaction -> Partition in
            guard let root = try await remover.directoryAccess.openRoot(transaction: transaction) else {
                Issue.record("root must exist")
                throw CancellationError()
            }
            try await remover.directoryAccess.remove("tenant", in: root, transaction: transaction)
            return try await remover.directoryAccess.openOrCreatePartition(
                "tenant",
                in: root,
                transaction: transaction
            )
        }
        #expect(recreated.keyspacePrefix != tenant.keyspacePrefix)

        let binding = try holder.createOwnedTransaction()
        do {
            try await lease.withWriteAccess(binding) { access in
                try access.setValue([0x02], for: ByteString(Array(tenant.root.root.prefix) + [0x6B]))
            }
            Issue.record("binding a lease whose Partition another engine replaced must fail")
        } catch let error as StorageError {
            #expect(error.code == .staleLease, "got \(error.code): \(error.message)")
        }
        try await binding.cancel()
        lease.release()

        // The write the stale lease attempted never reached the new keyspace.
        let carried = try await holder.withTransaction { transaction in
            try await transaction.getValue(for: ByteString(Array(recreated.root.root.prefix) + [0x6B]))
        }
        #expect(carried == nil)

        await holder.shutdown()
        await remover.shutdown()
    }

    private func temporaryDatabasePath() -> String {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("storage-kit-cross-engine-lease-\(UUID().uuidString).sqlite")
            .path
    }

    private func removeTemporaryDatabase(_ path: String) {
        let paths = [path, "\(path)-wal", "\(path)-shm"]
        for candidate in paths where FileManager.default.fileExists(atPath: candidate) {
            do {
                try FileManager.default.removeItem(atPath: candidate)
            } catch {
                Issue.record("failed to remove \(candidate): \(error)")
            }
        }
    }
}
