import DatabaseTypes
@testable import StorageKit
import Testing

@Suite("Partition lease")
struct PartitionLeaseTests {
    private let partitionID = try! PartitionID(utf8: "p")

    private func makePartition(_ engine: InMemoryEngine) async throws -> Partition {
        try await engine.withTransaction { transaction in
            let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            return try await engine.directoryAccess.openOrCreatePartition(partitionID, in: root, transaction: transaction)
        }
    }

    private func key(in partition: Partition, _ suffix: [UInt8]) -> ByteString {
        ByteString(Array(partition.root.root.prefix) + suffix)
    }

    @Test(.timeLimit(.minutes(1)))
    func cursorFailsAfterBindingScopeEnds() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = try PartitionKeyBounds(partition: partition, backend: .inMemory)
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
        let bounds = try PartitionKeyBounds(partition: partition, backend: .inMemory)
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

    @Test(.timeLimit(.minutes(1)))
    func boundAccessRejectsKeysAndSelectorsOutsidePartition() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let bounds = try PartitionKeyBounds(partition: partition, backend: .inMemory)
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
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func leaseDeinitReleasesRegistration() async throws {
        let engine = InMemoryEngine()
        let partition = try await makePartition(engine)
        let catalog = engine.directoryAccess
        try await engine.withTransaction { transaction in
            guard let root = try await catalog.openRoot(transaction: transaction) else {
                Issue.record("root must exist")
                return
            }
            do {
                let lease = try await engine.leasePartition(partition, transaction: transaction)
                let isActive = lease.isActive
                #expect(isActive)
                #expect(engine.transactionDomain.leases.isLeased(within: partition.root.address))
                await expectStorageError(.directoryLeased) {
                    try await catalog.removeChild(.partition(partitionID), in: root, transaction: transaction)
                }
            }
            #expect(!engine.transactionDomain.leases.isLeased(within: partition.root.address))
            try await catalog.removeChild(.partition(partitionID), in: root, transaction: transaction)
        }
        let remaining = try await engine.withTransaction { transaction -> Partition? in
            guard let root = try await catalog.openRoot(transaction: transaction) else { return nil }
            return try await catalog.openPartition(partitionID, in: root, transaction: transaction)
        }
        #expect(remaining == nil)
        await engine.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func releaseIsIdempotentAndRegistryRejectsCoveredIntents() async throws {
        let registry = PartitionLeaseRegistry()
        let address = try StorageAddress([.directory("a"), .partition(partitionID)])
        let registration = try registry.reserve(address, backend: .inMemory)
        #expect(registry.isLeased(within: address))
        try #expect(registry.isLeased(within: StorageAddress([.directory("a")])))
        try #expect(!registry.isLeased(within: StorageAddress([.directory("b")])))
        #expect(registration.release())
        #expect(!registration.release())
        #expect(!registry.isLeased(within: address))
        await expectStorageError(.staleLease) { try registration.requireActive(operation: .read, backend: .inMemory) }

        let engine = InMemoryEngine()
        let transaction = try engine.createOwnedTransaction()
        try registry.registerIntent(covering: StorageAddress([.directory("a")]), transaction: transaction, operation: .write, backend: .inMemory)
        await expectStorageError(.staleLease) { _ = try registry.reserve(address, backend: .inMemory) }
        registry.releaseIntents(for: transaction as AnyObject)
        let second = try registry.reserve(address, backend: .inMemory)
        let secondIsActive = second.isActive
        #expect(secondIsActive)
        await expectStorageError(.directoryLeased) {
            try registry.registerIntent(covering: StorageAddress([.directory("a")]), transaction: transaction, operation: .write, backend: .inMemory)
        }
        #expect(second.release())
        try await transaction.cancel()

        registry.requestShutdown()
        await expectStorageError(.resourceUnavailable) { _ = try registry.reserve(address, backend: .inMemory) }
        await engine.shutdown()
    }
}
