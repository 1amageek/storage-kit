import DatabaseTypes
@testable import StorageKit
import Testing

@Suite("Directory values")
struct DirectoryValueTests {
    @Test func directoryPathValidation() throws {
        #expect(throws: DirectoryAddressError.emptyPath) { try DirectoryPath([]) }
        #expect(throws: DirectoryAddressError.emptyComponent) { try DirectoryPath("a", "") }
        let long = String(repeating: "x", count: DirectoryLimits.maximumComponentByteCount + 1)
        #expect(throws: DirectoryAddressError.componentTooLong(byteCount: DirectoryLimits.maximumComponentByteCount + 1)) {
            try DirectoryPath(long)
        }
        #expect(throws: DirectoryAddressError.depthExceeded(depth: DirectoryLimits.maximumDepth + 1)) {
            try DirectoryPath(Array(repeating: "d", count: DirectoryLimits.maximumDepth + 1))
        }
        let deepest = try DirectoryPath(Array(repeating: "d", count: DirectoryLimits.maximumDepth))
        #expect(deepest.depth == DirectoryLimits.maximumDepth)
    }

    @Test func partitionIDValidation() throws {
        #expect(throws: DirectoryAddressError.emptyPartitionID) { try PartitionID([]) }
        let overflow = DirectoryLimits.maximumPartitionIDByteCount + 1
        #expect(throws: DirectoryAddressError.partitionIDTooLong(byteCount: overflow)) {
            try PartitionID(ByteString(Array(repeating: 0x01, count: overflow)))
        }
        try #expect(PartitionID(utf8: "a") < PartitionID(utf8: "b"))
        try #expect(PartitionID(utf8: "a") == PartitionID([0x61]))
    }

    @Test func storageAddressRelations() throws {
        let partitionID = try PartitionID(utf8: "p")
        let a = try StorageAddress([.directory("a")])
        let ap = try a.appending(.partition(partitionID))
        #expect(StorageAddress.root.isRoot)
        #expect(StorageAddress.root.parent == nil)
        #expect(ap.parent == a)
        #expect(ap.lastStep == .partition(partitionID))
        #expect(ap.depth == 2)
        #expect(a.isAncestorOrSelf(of: ap))
        #expect(a.isAncestorOrSelf(of: a))
        #expect(!ap.isAncestorOrSelf(of: a))
        #expect(StorageAddress.root.isAncestorOrSelf(of: ap))
        #expect(throws: DirectoryAddressError.emptyComponent) { try a.appending(.directory("")) }
        #expect(throws: DirectoryAddressError.depthExceeded(depth: DirectoryLimits.maximumDepth + 1)) {
            var address = StorageAddress.root
            for _ in 0...DirectoryLimits.maximumDepth {
                address = try address.appending(.directory("d"))
            }
        }
    }

    @Test func partitionAddressConsistency() async throws {
        let engine = InMemoryEngine()
        let id = try PartitionID(utf8: "p")
        let other = try PartitionID(utf8: "q")
        let partition = try await engine.withTransaction { transaction in
            let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            return try await engine.directoryAccess.openOrCreatePartition(id, in: root, transaction: transaction)
        }
        #expect(partition.hasConsistentAddress)
        #expect(!Partition(id: other, root: partition.root).hasConsistentAddress)
        #expect(partition.domain === engine.transactionDomain)
        await engine.shutdown()
    }

    @Test func layoutMarkerStateMachine() async throws {
        let engine = InMemoryEngine()
        func inspect() async throws -> StorageLayoutMarker.Inspection {
            try await engine.withTransaction { transaction in
                try await StorageLayoutMarker.inspect(transaction: transaction)
            }
        }
        let fresh = try await inspect()
        #expect(fresh == .uninitialized)

        try await engine.withTransaction { transaction in try transaction.setValue([0x01], for: [0x61]) }
        let strayKey = try await inspect()
        #expect(strayKey == .rejected(.markerAbsentKeyspaceNonempty))

        try await engine.withTransaction { transaction in try transaction.setValue([0xAA], for: StorageLayoutMarker.key) }
        let unknown = try await inspect()
        #expect(unknown == .rejected(.unknownMarker([0xAA])))

        try await engine.withTransaction { transaction in try transaction.setValue(StorageLayoutMarker.v1, for: StorageLayoutMarker.key) }
        let open = try await inspect()
        #expect(open == .openV1)
        await engine.shutdown()
    }

    @Test func keyValueLayoutV1() async throws {
        typealias Layout = KeyValueDirectoryCatalog.Layout
        #expect(Layout.rootPrefix(Layout.rootNumber) == [0x14])
        #expect(Layout.rootPrefix(1) == [0x15, 0x01])
        #expect(Layout.allocatorKey == [0xFE, 0x61])
        #expect(Layout.nodePrefix == [0xFE, 0x6E])
        let expectedNodeKey = ByteString(
            [0xFE, 0x6E] + Array(Tuple([0x14] as ByteString, Int64(0), "a").pack())
        )
        #expect(Layout.nodeKey(parentPrefix: [0x14], kind: .directory, name: "a") == expectedNodeKey)

        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        let id = try PartitionID(utf8: "p")
        let created = try await engine.withTransaction { transaction -> (Directory, Partition) in
            let root = try await catalog.openOrInitializeRoot(transaction: transaction)
            let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
            let partition = try await catalog.openOrCreatePartition(id, in: a, transaction: transaction)
            return (a, partition)
        }
        let (a, partition) = created
        #expect(a.root.prefix == Layout.rootPrefix(1))
        #expect(partition.root.root.prefix == Layout.rootPrefix(2))

        let stored = try await engine.withTransaction { transaction -> (ByteString?, ByteString?, ByteString?, ByteString?) in
            let marker = try await transaction.getValue(for: StorageLayoutMarker.key)
            let allocator = try await transaction.getValue(for: Layout.allocatorKey)
            let directoryNode = try await transaction.getValue(
                for: Layout.nodeKey(parentPrefix: Layout.rootPrefix(0), kind: .directory, name: "a")
            )
            let partitionNode = try await transaction.getValue(
                for: Layout.nodeKey(parentPrefix: a.root.prefix, kind: .partition, name: id.bytes)
            )
            return (marker, allocator, directoryNode, partitionNode)
        }
        #expect(stored.0 == StorageLayoutMarker.v1)
        #expect(stored.1 == Tuple(Int64(3)).pack())
        #expect(stored.2 == Tuple(Int64(1)).pack())
        #expect(stored.3 == Tuple(Int64(2)).pack())
        await engine.shutdown()
    }
}
