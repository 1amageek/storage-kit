import DatabaseTypes
@testable import StorageKit
import Synchronization
import Testing

@Suite("Partition lease")
struct PartitionLeaseTests {
    private let partitionName = "p"

    private func makePartition(_ engine: InMemoryEngine) async throws -> Partition {
        try await engine.withTransaction { transaction in
            let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            return try await engine.directoryAccess.openOrCreatePartition(
                partitionName,
                in: root,
                transaction: transaction
            )
        }
    }

    private func key(in partition: Partition, _ suffix: [UInt8]) -> ByteString {
        ByteString(Array(partition.root.root.prefix) + suffix)
    }

    @Test(.timeLimit(.minutes(1)))
    func cursorFailsAfterBindingScopeEnds() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        try await engine.withTransaction { transaction in
            let lease = try await engine.leasePartition(partition, transaction: transaction)
            var cursor = try await lease.withReadAccess(transaction) { access in
                try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
            }
            let error = await expectStorageError(.staleLease) { _ = try await cursor.next() }
            #expect(error?.message.contains("scope") == true)
            lease.release()
        }
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func cursorFailsAfterLeaseRelease() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        try await engine.withTransaction { transaction in
            let lease = try await engine.leasePartition(partition, transaction: transaction)
            var cursor = try await lease.withReadAccess(transaction) { access in
                try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
            }
            lease.release()
            let error = await expectStorageError(.staleLease) { _ = try await cursor.next() }
            #expect(error?.message.contains("released") == true)
        }
        await engine.shutdown()
    }

    /// The bound region covers the Partition's content and stops below its
    /// nested Directory Layer node subspace.
    @Test(.timeLimit(.minutes(1)))
    func boundRegionCoversContentAndExcludesNestedMetadata() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        #expect(bounds.prefix == partition.keyspacePrefix)
        #expect(bounds.end == partition.keyspacePrefix.appending(Directory.nodeSubspaceByte))
        // The Partition's own data root and every allocated descendant prefix
        // are inside; the nested allocator key is not.
        #expect(bounds.contains(partition.root.root.prefix))
        #expect(
            !bounds.contains(
                KeyValueDirectoryCatalog.Layout.allocatorKey(layerRoot: partition.keyspacePrefix)
            )
        )
        let inner = try await engine.withTransaction { transaction in
            try await engine.directoryAccess.openOrCreateDirectory(
                "inner",
                in: partition.root,
                transaction: transaction
            )
        }
        #expect(bounds.contains(inner.root.prefix))
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func boundAccessRejectsKeysAndSelectorsOutsidePartition() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = PartitionKeyBounds(partition: partition, backend: .inMemory)
        let inside = key(in: partition, [0x01])
        try await engine.withTransaction { transaction in
            let lease = try await engine.leasePartition(partition, transaction: transaction)
            try await lease.withWriteAccess(transaction) { access in
                try access.setValue([0x01], for: inside)
                await expectStorageError(.invalidOperation) { try access.setValue([0x01], for: [0x00]) }
                await expectStorageError(.invalidOperation) { try access.clear(key: [0xFD]) }
                await expectStorageError(.invalidOperation) {
                    try access.clearRange(beginKey: bounds.prefix, endKey: [0xFF])
                }
                await expectStorageError(.invalidOperation) {
                    _ = try access.rangeCursor(from: .firstGreaterOrEqual([0x00]), to: .firstGreaterOrEqual(bounds.end))
                }
                await expectStorageError(.invalidOperation) {
                    _ = try access.rangeCursor(from: .firstGreaterOrEqual(bounds.prefix), to: .firstGreaterOrEqual([0xFF]))
                }
                await expectStorageError(.invalidOperation) { _ = try await access.getValue(for: [0x00]) }
                // The nested Directory Layer is catalog metadata, not content.
                await expectStorageError(.invalidOperation) {
                    try access.clear(
                        key: KeyValueDirectoryCatalog.Layout.allocatorKey(
                            layerRoot: partition.keyspacePrefix
                        )
                    )
                }

                let first = try await access.getKey(selector: .firstGreaterOrEqual(bounds.prefix))
                #expect(first == inside)
                let beyond = try await access.getKey(selector: .firstGreaterOrEqual(key(in: partition, [0x02])))
                #expect(beyond == nil)

                var cursor = try access.rangeCursor(
                    from: .firstGreaterOrEqual(bounds.prefix),
                    to: .firstGreaterOrEqual(bounds.end)
                )
                var rows: [(ByteString, ByteString)] = []
                while let row = try await cursor.next() {
                    rows.append(row)
                }
                #expect(rows.count == 1)
                #expect(rows.first?.0 == inside)

                try access.clearRange(beginKey: bounds.prefix, endKey: bounds.end)
                let cleared = try await access.getValue(for: inside)
                #expect(cleared == nil)
            }
            lease.release()
        }
        // Clearing the whole bound region leaves the nested layer usable.
        let recreated = try await engine.withTransaction { transaction in
            try await engine.directoryAccess.openOrCreateDirectory(
                "inner",
                in: partition.root,
                transaction: transaction
            )
        }
        #expect(recreated.keyspacePrefix.starts(with: partition.keyspacePrefix))
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func removingALeasedPartitionIsAdmittedAndTheLeaseGoesStale() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let catalog = engine.directoryAccess
        try await engine.withTransaction { transaction in
            guard let root = try await catalog.openRoot(transaction: transaction) else {
                Issue.record("root must exist")
                return
            }
            let lease = try await engine.leasePartition(partition, transaction: transaction)
            let isActive = lease.isActive
            #expect(isActive)

            // A lease is not an exclusion: the removal is admitted while it is
            // held. Whether it should have been is a decision above StorageKit.
            try await catalog.remove(partitionName, in: root, transaction: transaction)

            // The lease still holds its registration; what changed is the
            // store, and the binding is where that is observed.
            let stillActive = lease.isActive
            #expect(stillActive)
            await expectStorageError(.staleLease) {
                try await lease.withReadAccess(transaction) { _ in }
            }
            lease.release()
        }
        let remaining = try await engine.withTransaction { transaction -> Partition? in
            guard let root = try await catalog.openRoot(transaction: transaction) else { return nil }
            return try await catalog.openPartition(partitionName, in: root, transaction: transaction)
        }
        #expect(remaining == nil)
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func releaseIsIdempotentAndShutdownClosesIssuance() async throws {
        let registration = try LeaseRegistration(address: StorageAddress(["a", partitionName]))
        #expect(registration.release())
        #expect(!registration.release())
        await expectStorageError(.staleLease) {
            try registration.requireActive(operation: .read, backend: .inMemory)
        }

        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let admitted = try engine.createOwnedTransaction()

        // Shutdown closes issuance only. A transaction that was already
        // admitted keeps working, so cleanup finishes deterministically.
        engine.transactionDomain.requestShutdown()
        await expectStorageError(.resourceUnavailable) {
            _ = try await engine.leasePartition(partition, transaction: admitted)
        }
        try await admitted.cancel()
        await engine.shutdown()
    }

    /// A binding reports a caller error the engine already knows about before
    /// it asks the backend whether the binding is admissible at all.
    ///
    /// The double refuses every admission, so reaching admission is visible as
    /// its own failure code and as a non-zero `admitCalls`.
    @Test(.timeLimit(.minutes(1)))
    func bindingRejectsAForeignTransactionBeforeAdmissionAndGenerationIO() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let access = RejectingDirectoryAccess(transactionDomain: engine.transactionDomain)
        let other = InMemoryEngine()
        try await other.withTransaction { foreign in
            let lease = PartitionLease(
                partition: partition,
                directoryAccess: access,
                registration: LeaseRegistration(address: partition.root.address),
                bounds: PartitionKeyBounds(partition: partition, backend: .inMemory)
            )
            await expectStorageError(.storageDomainMismatch) {
                try await lease.withReadAccess(foreign) { _ in }
            }
            await expectStorageError(.storageDomainMismatch) {
                try await lease.withWriteAccess(foreign) { _ in }
            }
            lease.release()
        }
        #expect(access.admitCalls == 0)
        #expect(access.openRootCalls == 0)
        await other.shutdown()
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func bindingRejectsAReleasedLeaseBeforeAdmissionAndGenerationIO() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let access = RejectingDirectoryAccess(transactionDomain: engine.transactionDomain)
        try await engine.withTransaction { transaction in
            let registration = LeaseRegistration(address: partition.root.address)
            let lease = PartitionLease(
                partition: partition,
                directoryAccess: access,
                registration: registration,
                bounds: PartitionKeyBounds(partition: partition, backend: .inMemory)
            )
            // Released out of band: `release()` consumes the lease, and a
            // consumed lease could not be bound again to observe the order.
            #expect(registration.release())
            await expectStorageError(.staleLease) {
                try await lease.withReadAccess(transaction) { _ in }
            }
            await expectStorageError(.staleLease) {
                try await lease.withWriteAccess(transaction) { _ in }
            }
        }
        #expect(access.admitCalls == 0)
        #expect(access.openRootCalls == 0)
        await engine.shutdown()
    }

    /// The control for the two tests above: with the same double and no caller
    /// error, admission is reached, its refusal is what the caller sees, and
    /// the generation walk still costs no round trip.
    @Test(.timeLimit(.minutes(1)))
    func bindingReportsBackendRefusalBeforeAnyGenerationIO() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let access = RejectingDirectoryAccess(transactionDomain: engine.transactionDomain)
        try await engine.withTransaction { transaction in
            let lease = PartitionLease(
                partition: partition,
                directoryAccess: access,
                registration: LeaseRegistration(address: partition.root.address),
                bounds: PartitionKeyBounds(partition: partition, backend: .inMemory)
            )
            let read = await expectStorageError(.unsupportedOperation) {
                try await lease.withReadAccess(transaction) { _ in }
            }
            #expect(read?.operation == .read)
            #expect(access.admitCalls == 1)
            #expect(access.openRootCalls == 0)

            let write = await expectStorageError(.unsupportedOperation) {
                try await lease.withWriteAccess(transaction) { _ in }
            }
            #expect(write?.operation == .write)
            #expect(access.admitCalls == 2)
            #expect(access.openRootCalls == 0)
            lease.release()
        }
        await engine.shutdown()
    }
}

/// A `DirectoryAccess` that refuses every admission and performs no I/O.
///
/// It makes the binding order observable from outside `PartitionLease`:
/// `admit` is the first member a binding may reach after the checks this
/// process settles alone, and `openRoot` is the first read of the generation
/// walk that follows. Every other member fails instead of returning a value,
/// so a binding that reached one is reported rather than absorbed.
private final class RejectingDirectoryAccess: DirectoryAccess {
    let transactionDomain: StorageTransactionDomain
    let backend: StorageBackend = .inMemory
    private let admitted = Mutex(0)
    private let rootReads = Mutex(0)

    init(transactionDomain: StorageTransactionDomain) {
        self.transactionDomain = transactionDomain
    }

    var admitCalls: Int { admitted.withLock { $0 } }
    var openRootCalls: Int { rootReads.withLock { $0 } }

    func admit(_ operation: StorageOperation) throws {
        admitted.withLock { $0 += 1 }
        throw StorageError.unsupportedOperation(
            "Refused by RejectingDirectoryAccess",
            operation: operation,
            backend: backend
        )
    }

    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        rootReads.withLock { $0 += 1 }
        throw Self.unreachable("openRoot")
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        throw Self.unreachable("openOrInitializeRoot")
    }

    func open(
        _ name: String,
        expecting expected: LayerTag?,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        throw Self.unreachable("open")
    }

    func openOrCreate(
        _ name: String,
        layer: LayerTag,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        throw Self.unreachable("openOrCreate")
    }

    func listChildren(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [DirectoryEntry] {
        throw Self.unreachable("listChildren")
    }

    func move(
        _ name: String,
        in source: Directory,
        to newName: String,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        throw Self.unreachable("move")
    }

    func remove(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        throw Self.unreachable("remove")
    }

    private static func unreachable(_ member: String) -> StorageError {
        StorageError.invalidOperation("RejectingDirectoryAccess.\(member) must not be reached")
    }
}
