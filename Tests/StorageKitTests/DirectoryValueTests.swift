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

    @Test func layerTagValidation() throws {
        #expect(LayerTag.default.isDefault)
        #expect(!LayerTag.default.isPartition)
        #expect(LayerTag.partition.isPartition)
        #expect(!LayerTag.partition.isDefault)
        #expect(LayerTag.partition.bytes == ByteString(utf8: "partition"))
        try #expect(LayerTag(utf8: "partition") == LayerTag.partition)
        try #expect(LayerTag(ByteString()) == LayerTag.default)
        try #expect(LayerTag(utf8: "index").isDefault == false)
        let overflow = DirectoryLimits.maximumLayerTagByteCount + 1
        #expect(throws: DirectoryAddressError.layerTagTooLong(byteCount: overflow)) {
            try LayerTag(ByteString(Array(repeating: 0x01, count: overflow)))
        }
    }

    @Test func storageAddressRelations() throws {
        let a = try StorageAddress(["a"])
        let ap = try a.appending("p")
        #expect(StorageAddress.root.isRoot)
        #expect(StorageAddress.root.parent == nil)
        #expect(ap.parent == a)
        #expect(ap.lastComponent == "p")
        #expect(ap.depth == 2)
        #expect(a.isAncestorOrSelf(of: ap))
        #expect(a.isAncestorOrSelf(of: a))
        #expect(!ap.isAncestorOrSelf(of: a))
        #expect(StorageAddress.root.isAncestorOrSelf(of: ap))
        #expect(throws: DirectoryAddressError.emptyComponent) { try a.appending("") }
        #expect(throws: DirectoryAddressError.depthExceeded(depth: DirectoryLimits.maximumDepth + 1)) {
            var address = StorageAddress.root
            for _ in 0...DirectoryLimits.maximumDepth {
                address = try address.appending("d")
            }
        }
    }

    @Test func partitionWrapsOnlyPartitionNodes() async throws {
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        let nodes = try await engine.withTransaction { transaction -> (Directory, Directory) in
            let root = try await catalog.openOrInitializeRoot(transaction: transaction)
            let plain = try await catalog.openOrCreateDirectory("d", in: root, transaction: transaction)
            let node = try await catalog.openOrCreate(
                "p",
                layer: .partition,
                in: root,
                transaction: transaction
            )
            return (plain, node)
        }
        let (plain, node) = nodes
        #expect(Partition(plain) == nil)
        let partition = try #require(Partition(node))
        #expect(partition.name == "p")
        #expect(partition.address == node.address)
        #expect(partition.keyspacePrefix == node.keyspacePrefix)
        #expect(partition.domain === engine.transactionDomain)
        // A plain node's data root is its own prefix; a Partition's is offset
        // below its nested node subspace.
        #expect(plain.root.prefix == plain.keyspacePrefix)
        #expect(partition.root.root.prefix == node.keyspacePrefix.appending(Directory.partitionDataByte))
        await engine.shutdown()
    }

    /// The root layer allocator is the only witness that the root exists, and
    /// initialization records nothing else. A second witness of the same fact
    /// could disagree with this one, and neither root operation could then
    /// decide without fabricating a root or writing over data.
    @Test func theRootAllocatorIsTheOnlyBootstrapWitness() async throws {
        typealias Layout = KeyValueDirectoryCatalog.Layout
        let allocatorKey = Layout.allocatorKey(layerRoot: ByteString())
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess

        let fresh = try await engine.withTransaction { transaction -> (Directory?, ByteString?) in
            let root = try await catalog.openRoot(transaction: transaction)
            return (root, try await transaction.getValue(for: allocatorKey))
        }
        #expect(fresh.0 == nil)
        #expect(fresh.1 == nil)

        try await engine.withTransaction { transaction in
            _ = try await catalog.openOrInitializeRoot(transaction: transaction)
        }
        let written = try await engine.withTransaction { transaction in
            try await transaction.collectRange(begin: [], end: [0xFF, 0xFF])
        }
        #expect(written.count == 1)
        #expect(written.first?.0 == allocatorKey)
        #expect(written.first?.1 == Tuple(Layout.firstNumber).pack())
        await engine.shutdown()
    }

    /// A key at or above `[0xFF]` is data exactly like any other key, so the
    /// foreign-data probe of an uninitialized root has no upper bound. A
    /// bounded probe would report such a root as empty and adopt it.
    @Test func theForeignDataProbeHasNoUpperBound() async throws {
        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        try await engine.withTransaction { transaction in
            try transaction.setValue([0x01], for: [0xFF, 0x01])
        }
        await expectStorageError(.incompatibleStorageLayout) {
            _ = try await engine.withTransaction { transaction in
                try await catalog.openRoot(transaction: transaction)
            }
        }
        await expectStorageError(.incompatibleStorageLayout) {
            _ = try await engine.withTransaction { transaction in
                try await catalog.openOrInitializeRoot(transaction: transaction)
            }
        }
        await engine.shutdown()
    }

    /// No content prefix a layer allocates starts with the reserved catalog
    /// byte, so catalog metadata and node content can never overlap.
    @Test func allocatedContentPrefixesStayOutsideTheReservedRegion() async throws {
        typealias Layout = KeyValueDirectoryCatalog.Layout
        let reservedPrefix: ByteString = [Layout.reservedByte]
        let numbers: [Int64] = [
            Layout.rootNumber,
            Layout.firstNumber,
            -1,
            Int64.min,
            127,
            1 << 20,
            Int64.max
        ]
        for number in numbers {
            let prefix = Layout.contentPrefix(layerRoot: ByteString(), number: number)
            #expect(!prefix.starts(with: reservedPrefix))
        }
    }

    @Test func keyValueLayoutV1() async throws {
        typealias Layout = KeyValueDirectoryCatalog.Layout
        let domainRoot = ByteString()
        #expect(Layout.nodeSubspacePrefix(layerRoot: domainRoot) == [0xFE])
        #expect(Layout.allocatorKey(layerRoot: domainRoot) == [0xFE, 0x61])
        #expect(Layout.contentPrefix(layerRoot: domainRoot, number: Layout.rootNumber) == [0x14])
        #expect(Layout.contentPrefix(layerRoot: domainRoot, number: 1) == [0x15, 0x01])
        #expect(Layout.contentPrefix(layerRoot: [0x15, 0x02], number: 1) == [0x15, 0x02, 0x15, 0x01])
        let expectedEdgeKey = ByteString(
            [0xFE] + Array(Tuple([0x14] as ByteString, Int64(0), "a").pack())
        )
        #expect(
            Layout.edgeKey(layerRoot: domainRoot, parentPrefix: [0x14], name: "a") == expectedEdgeKey
        )
        #expect(
            expectedEdgeKey.starts(
                with: Layout.edgeListPrefix(layerRoot: domainRoot, parentPrefix: [0x14])
            )
        )
        #expect(
            Layout.edgeValue(prefix: [0x15, 0x01], layer: .default)
                == Tuple([0x15, 0x01] as ByteString, ByteString()).pack()
        )

        let engine = InMemoryEngine()
        let catalog = engine.directoryAccess
        let created = try await engine.withTransaction { transaction -> (Directory, Partition, Directory) in
            let root = try await catalog.openOrInitializeRoot(transaction: transaction)
            let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
            let partition = try await catalog.openOrCreatePartition("p", in: a, transaction: transaction)
            let inner = try await catalog.openOrCreateDirectory(
                "inner",
                in: partition.root,
                transaction: transaction
            )
            return (a, partition, inner)
        }
        let (a, partition, inner) = created
        // Siblings of the domain root layer receive consecutive numbers.
        #expect(a.keyspacePrefix == [0x15, 0x01])
        #expect(a.root.prefix == [0x15, 0x01])
        #expect(partition.keyspacePrefix == [0x15, 0x02])
        #expect(partition.root.root.prefix == [0x15, 0x02, 0xFD])
        // The nested layer allocates inside the Partition, from 1 again.
        #expect(inner.keyspacePrefix == [0x15, 0x02, 0x15, 0x01])
        #expect(inner.root.prefix == inner.keyspacePrefix)
        #expect(inner.keyspacePrefix.starts(with: partition.keyspacePrefix))

        let stored = try await engine.withTransaction { transaction -> [ByteString?] in
            var values: [ByteString?] = []
            values.append(
                try await transaction.getValue(for: Layout.allocatorKey(layerRoot: domainRoot))
            )
            values.append(
                try await transaction.getValue(
                    for: Layout.allocatorKey(layerRoot: partition.keyspacePrefix)
                )
            )
            values.append(
                try await transaction.getValue(
                    for: Layout.edgeKey(
                        layerRoot: domainRoot,
                        parentPrefix: Layout.contentPrefix(layerRoot: domainRoot, number: Layout.rootNumber),
                        name: "a"
                    )
                )
            )
            values.append(
                try await transaction.getValue(
                    for: Layout.edgeKey(
                        layerRoot: domainRoot,
                        parentPrefix: a.keyspacePrefix,
                        name: "p"
                    )
                )
            )
            values.append(
                try await transaction.getValue(
                    for: Layout.edgeKey(
                        layerRoot: partition.keyspacePrefix,
                        parentPrefix: partition.keyspacePrefix,
                        name: "inner"
                    )
                )
            )
            return values
        }
        // Root layer allocated 1 and 2; the next number is 3.
        #expect(stored[0] == Tuple(Int64(3)).pack())
        // The nested layer allocated 1; the next number is 2.
        #expect(stored[1] == Tuple(Int64(2)).pack())
        #expect(stored[2] == Tuple([0x15, 0x01] as ByteString, ByteString()).pack())
        #expect(stored[3] == Tuple([0x15, 0x02] as ByteString, ByteString(utf8: "partition")).pack())
        #expect(stored[4] == Tuple([0x15, 0x02, 0x15, 0x01] as ByteString, ByteString()).pack())
        await engine.shutdown()
    }
}
