import DatabaseTypes

/// Transactional Directory catalog for key-value backends.
///
/// One instance is bound to one engine domain and is that engine's sole
/// existence authority. All catalog reads and writes go through the caller's
/// transaction, so creation, move, and removal are atomic with it.
///
/// Every Directory Layer has a content base: the empty prefix for the domain
/// root layer, and the Partition's own prefix for the layer nested inside a
/// Partition. A layer keeps its allocator and its child edges under
/// `base + 0xFE`, and allocates child content prefixes as `base + Tuple(n)`, so
/// every descendant of a Partition lies inside that Partition's prefix range.
/// The layout is fixed by `DESIGN.md`; changing it is a layout change.
public final class KeyValueDirectoryCatalog: DirectoryAccess, Sendable {
    /// Backend-owned admission check run before the catalog writes anything.
    ///
    /// A backend whose configured isolation cannot detect the read-then-write
    /// conflicts the catalog relies on (PostgreSQL `readCommitted`) rejects the
    /// mutation here with a typed failure instead of committing an unsafe write.
    public typealias MutationAdmission = @Sendable (StorageOperation) throws -> Void

    public let transactionDomain: StorageTransactionDomain
    public let backend: StorageBackend
    private let mutationAdmission: MutationAdmission?

    public init(
        transactionDomain: StorageTransactionDomain,
        backend: StorageBackend,
        mutationAdmission: MutationAdmission? = nil
    ) {
        self.transactionDomain = transactionDomain
        self.backend = backend
        self.mutationAdmission = mutationAdmission
    }

    // MARK: - Layout

    package enum Layout {
        /// First byte of every layer's node subspace; the same reserved byte
        /// that bounds a Partition's leasable content region.
        package static let reservedByte: UInt8 = Directory.nodeSubspaceByte
        /// Allocator key suffix inside a layer's node subspace.
        package static let allocatorByte: UInt8 = 0x61
        /// Edge-key discriminator, matching the FoundationDB Directory Layer.
        package static let childEdgeMarker: Int64 = 0
        package static let rootNumber: Int64 = 0
        package static let firstNumber: Int64 = 1

        package static func nodeSubspacePrefix(layerRoot: ByteString) -> ByteString {
            layerRoot.appending(reservedByte)
        }

        package static func allocatorKey(layerRoot: ByteString) -> ByteString {
            nodeSubspacePrefix(layerRoot: layerRoot).appending(allocatorByte)
        }

        package static func contentPrefix(
            layerRoot: ByteString,
            number: Int64
        ) -> ByteString {
            layerRoot.appending(contentsOf: Tuple(number).pack())
        }

        package static func edgeKey(
            layerRoot: ByteString,
            parentPrefix: ByteString,
            name: String
        ) -> ByteString {
            nodeSubspacePrefix(layerRoot: layerRoot)
                .appending(contentsOf: Tuple(parentPrefix, childEdgeMarker, name).pack())
        }

        package static func edgeListPrefix(
            layerRoot: ByteString,
            parentPrefix: ByteString
        ) -> ByteString {
            nodeSubspacePrefix(layerRoot: layerRoot)
                .appending(contentsOf: Tuple(parentPrefix, childEdgeMarker).pack())
        }

        package static func edgeValue(prefix: ByteString, layer: LayerTag) -> ByteString {
            Tuple(prefix, layer.bytes).pack()
        }
    }

    /// One resolved child edge: the node's content prefix and its layer tag.
    private struct Edge {
        let prefix: ByteString
        let layer: LayerTag
    }

    private var rootDirectory: Directory {
        Directory(
            domain: transactionDomain,
            address: .root,
            layer: .default,
            keyspacePrefix: Layout.contentPrefix(
                layerRoot: ByteString(),
                number: Layout.rootNumber
            ),
            layerRoot: ByteString()
        )
    }

    // MARK: - Root

    /// The root layer's allocator: the sole witness that this root is
    /// initialized (SPEC §8.7).
    ///
    /// Every content prefix of the root layer is handed out by this key, so a
    /// catalog that owns the root owns it, and nothing else writes it. Nothing
    /// else records the same fact: a second witness can disagree with this one,
    /// and a disagreement is a state neither `openRoot` nor
    /// `openOrInitializeRoot` can resolve without either fabricating a root or
    /// writing over data.
    private var rootAllocatorKey: ByteString {
        Layout.allocatorKey(layerRoot: ByteString())
    }

    public func openRoot(
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try requireDomain(of: transaction, operation: .open)
        guard try await transaction.getValue(for: rootAllocatorKey) != nil else {
            try await requireRootHoldsNoForeignData(transaction: transaction, operation: .open)
            return nil
        }
        return rootDirectory
    }

    public func openOrInitializeRoot(
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, operation: .initialize)
        if try await transaction.getValue(for: rootAllocatorKey) != nil {
            return rootDirectory
        }
        try await requireRootHoldsNoForeignData(transaction: transaction, operation: .initialize)
        try admitMutation(operation: .initialize)
        try transaction.setValue(Tuple(Layout.firstNumber).pack(), for: rootAllocatorKey)
        return rootDirectory
    }

    /// Rejects an uninitialized root that already holds data.
    ///
    /// The root layer allocates content prefixes from `Tuple(1)` upward inside
    /// the same flat keyspace the store hands to whoever wrote first, so a
    /// Directory created here could land on top of existing keys. The probe
    /// has no upper bound: a key at or above `[0xFF]` is data exactly like any
    /// other key, and a bounded probe would report such a root as empty and
    /// adopt a foreign layout.
    private func requireRootHoldsNoForeignData(
        transaction: any TransactionReadAccess,
        operation: StorageOperation
    ) async throws {
        guard try await transaction.getKey(selector: .firstGreaterOrEqual([])) == nil else {
            throw StorageError.incompatibleStorageLayout(
                "the storage root holds data that no Directory catalog wrote",
                operation: operation,
                backend: backend
            )
        }
    }

    // MARK: - Operation 1

    public func open(
        _ name: String,
        expecting expected: LayerTag?,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try requireDomain(of: transaction, parent: parent, operation: .open)
        let address = try childAddress(of: parent, name, operation: .open)
        let layerRoot = parent.childLayerRoot
        guard let edge = try await readEdge(
            layerRoot: layerRoot,
            parentPrefix: parent.keyspacePrefix,
            name: name,
            transaction: transaction
        ) else {
            return nil
        }
        try requireLayer(edge.layer, expected: expected, name: name, operation: .open)
        return directory(at: address, edge: edge, layerRoot: layerRoot)
    }

    // MARK: - Operation 2

    public func openOrCreate(
        _ name: String,
        layer: LayerTag,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, parent: parent, operation: .write)
        let address = try childAddress(of: parent, name, operation: .write)
        let layerRoot = parent.childLayerRoot
        if let edge = try await readEdge(
            layerRoot: layerRoot,
            parentPrefix: parent.keyspacePrefix,
            name: name,
            transaction: transaction
        ) {
            try requireLayer(edge.layer, expected: layer, name: name, operation: .write)
            return directory(at: address, edge: edge, layerRoot: layerRoot)
        }
        try admitMutation(operation: .write)
        let number = try await allocateNumber(layerRoot: layerRoot, transaction: transaction)
        let prefix = Layout.contentPrefix(layerRoot: layerRoot, number: number)
        try transaction.setValue(
            Layout.edgeValue(prefix: prefix, layer: layer),
            for: Layout.edgeKey(
                layerRoot: layerRoot,
                parentPrefix: parent.keyspacePrefix,
                name: name
            )
        )
        if layer.isPartition {
            // The nested layer allocates inside the Partition from its own
            // allocator, so a missing allocator always means corruption.
            try transaction.setValue(
                Tuple(Layout.firstNumber).pack(),
                for: Layout.allocatorKey(layerRoot: prefix)
            )
        }
        return directory(
            at: address,
            edge: Edge(prefix: prefix, layer: layer),
            layerRoot: layerRoot
        )
    }

    // MARK: - Operation 3

    public func listChildren(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [DirectoryEntry] {
        try requireDomain(of: transaction, parent: parent, operation: .rangeRead)
        try requireListLimit(limit)
        if let after {
            try validate(component: after, operation: .rangeRead)
        }
        let layerRoot = parent.childLayerRoot
        let listPrefix = Layout.edgeListPrefix(
            layerRoot: layerRoot,
            parentPrefix: parent.keyspacePrefix
        )
        let begin: KeySelector
        if let after {
            begin = .firstGreaterThan(
                Layout.edgeKey(
                    layerRoot: layerRoot,
                    parentPrefix: parent.keyspacePrefix,
                    name: after
                )
            )
        } else {
            begin = .firstGreaterOrEqual(listPrefix)
        }
        let end = KeySelector.firstGreaterOrEqual(try increment(listPrefix))
        let rows = try await transaction.collectRange(from: begin, to: end, limit: limit)
        return try rows.map { row in
            let name = try decodeName(from: row.0, listPrefix: listPrefix)
            let edge = try decodeEdge(row.1)
            return DirectoryEntry(name: name, layer: edge.layer)
        }
    }

    // MARK: - Operation 4

    public func move(
        _ name: String,
        in source: Directory,
        to newName: String,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, parent: source, operation: .write)
        guard destination.domain === transactionDomain else {
            throw domainMismatch(operation: .write, subject: "Destination Directory")
        }
        let movedAddress = try childAddress(of: source, name, operation: .write)
        let targetAddress = try childAddress(of: destination, newName, operation: .write)
        let layerRoot = source.childLayerRoot
        guard layerRoot == destination.childLayerRoot else {
            throw StorageError.partitionBoundaryViolation(
                "A node cannot move into or out of a Partition",
                operation: .write,
                backend: backend
            )
        }
        guard let edge = try await readEdge(
            layerRoot: layerRoot,
            parentPrefix: source.keyspacePrefix,
            name: name,
            transaction: transaction
        ) else {
            throw notFound(
                "Node '\(name)' does not exist in the source Directory",
                operation: .write
            )
        }
        if movedAddress.isAncestorOrSelf(of: destination.address)
            || destination.keyspacePrefix == edge.prefix {
            throw StorageError.invalidDirectoryAddress(
                .targetInsideMovedSubtree,
                operation: .write,
                backend: backend
            )
        }
        if try await readEdge(
            layerRoot: layerRoot,
            parentPrefix: destination.keyspacePrefix,
            name: newName,
            transaction: transaction
        ) != nil {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: backend,
                message: "Node '\(newName)' already exists in the destination Directory"
            )
        }
        try admitMutation(operation: .write)
        try transactionDomain.leases.registerIntent(
            covering: movedAddress,
            transaction: transaction,
            operation: .write,
            backend: backend
        )
        try transaction.clear(
            key: Layout.edgeKey(
                layerRoot: layerRoot,
                parentPrefix: source.keyspacePrefix,
                name: name
            )
        )
        try transaction.setValue(
            Layout.edgeValue(prefix: edge.prefix, layer: edge.layer),
            for: Layout.edgeKey(
                layerRoot: layerRoot,
                parentPrefix: destination.keyspacePrefix,
                name: newName
            )
        )
        return directory(at: targetAddress, edge: edge, layerRoot: layerRoot)
    }

    // MARK: - Operation 5

    public func remove(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        try requireDomain(of: transaction, parent: parent, operation: .delete)
        let address = try childAddress(of: parent, name, operation: .delete)
        let layerRoot = parent.childLayerRoot
        guard let edge = try await readEdge(
            layerRoot: layerRoot,
            parentPrefix: parent.keyspacePrefix,
            name: name,
            transaction: transaction
        ) else {
            throw notFound(
                "Node '\(name)' does not exist in the parent Directory",
                operation: .delete
            )
        }
        try admitMutation(operation: .delete)
        try transactionDomain.leases.registerIntent(
            covering: address,
            transaction: transaction,
            operation: .delete,
            backend: backend
        )
        try await clearSubtree(of: edge, layerRoot: layerRoot, transaction: transaction)
        try transaction.clear(
            key: Layout.edgeKey(
                layerRoot: layerRoot,
                parentPrefix: parent.keyspacePrefix,
                name: name
            )
        )
    }

    /// Clears a node's descendants and its own data, but not its parent edge.
    ///
    /// A Partition owns one contiguous range that also holds its nested layer,
    /// so one range clear removes its whole subtree. A plain Directory's
    /// children are allocated from the layer that contains it, so its subtree
    /// is walked edge by edge inside the caller's transaction.
    private func clearSubtree(
        of edge: Edge,
        layerRoot: ByteString,
        transaction: any TransactionAccess
    ) async throws {
        if edge.layer.isPartition {
            try clearRange(prefix: edge.prefix, transaction: transaction)
            return
        }
        var pending: [ByteString] = [edge.prefix]
        var visited: Set<ByteString> = []
        while let nodePrefix = pending.popLast() {
            guard visited.insert(nodePrefix).inserted else {
                throw dataCorruption("Directory catalog contains a node prefix cycle")
            }
            let listPrefix = Layout.edgeListPrefix(
                layerRoot: layerRoot,
                parentPrefix: nodePrefix
            )
            let listEnd = try increment(listPrefix)
            // Read-your-writes hides the edges cleared in this transaction, so
            // repeatedly draining the first page terminates.
            while true {
                let rows = try await transaction.collectRange(
                    begin: listPrefix,
                    end: listEnd,
                    limit: DirectoryLimits.maximumListLimit
                )
                if rows.isEmpty {
                    break
                }
                for row in rows {
                    let child = try decodeEdge(row.1)
                    if child.layer.isPartition {
                        try clearRange(prefix: child.prefix, transaction: transaction)
                    } else {
                        pending.append(child.prefix)
                    }
                    try transaction.clear(key: row.0)
                }
            }
            try clearRange(prefix: nodePrefix, transaction: transaction)
        }
    }

    // MARK: - Node access

    private func readEdge(
        layerRoot: ByteString,
        parentPrefix: ByteString,
        name: String,
        transaction: any TransactionReadAccess
    ) async throws -> Edge? {
        let key = Layout.edgeKey(
            layerRoot: layerRoot,
            parentPrefix: parentPrefix,
            name: name
        )
        guard let value = try await transaction.getValue(for: key) else {
            return nil
        }
        return try decodeEdge(value)
    }

    /// Read-modify-write on the layer allocator key inside the caller's
    /// transaction, so concurrent creators conflict through the backend's own
    /// detection.
    private func allocateNumber(
        layerRoot: ByteString,
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        let key = Layout.allocatorKey(layerRoot: layerRoot)
        guard let raw = try await transaction.getValue(for: key) else {
            throw dataCorruption(
                "Directory layer allocator key is missing from an initialized catalog"
            )
        }
        let next = try decodeNumber(raw, context: "Directory allocator")
        guard next >= Layout.firstNumber, next < Int64.max else {
            throw dataCorruption("Directory allocator holds an invalid next number \(next)")
        }
        try transaction.setValue(Tuple(next + 1).pack(), for: key)
        return next
    }

    private func decodeEdge(_ value: ByteString) throws -> Edge {
        let elements: [any TupleElement]
        do {
            elements = try Tuple.unpack(from: value)
        } catch {
            throw dataCorruption("Directory edge value is not a Tuple: \(error)")
        }
        guard elements.count == 2,
              let prefix = elements[0] as? ByteString,
              let tag = elements[1] as? ByteString,
              !prefix.isEmpty
        else {
            throw dataCorruption("Directory edge value is not a prefix and a layer tag")
        }
        do {
            return Edge(prefix: prefix, layer: try LayerTag(tag))
        } catch {
            throw dataCorruption("Directory node layer tag is invalid: \(error)")
        }
    }

    private func decodeNumber(
        _ value: ByteString,
        context: String
    ) throws -> Int64 {
        let elements: [any TupleElement]
        do {
            elements = try Tuple.unpack(from: value)
        } catch {
            throw dataCorruption("\(context) value is not a Tuple: \(error)")
        }
        guard elements.count == 1,
              let number = elements[0] as? Int64,
              number >= 0
        else {
            throw dataCorruption("\(context) value is not a single nonnegative number")
        }
        return number
    }

    private func decodeName(
        from key: ByteString,
        listPrefix: ByteString
    ) throws -> String {
        guard key.count > listPrefix.count, key.starts(with: listPrefix) else {
            throw dataCorruption("Directory edge key lies outside its listing prefix")
        }
        let suffix = key[listPrefix.count..<key.count]
        let elements: [any TupleElement]
        do {
            elements = try Tuple.unpack(from: suffix)
        } catch {
            throw dataCorruption("Directory node name is not a Tuple: \(error)")
        }
        guard elements.count == 1, let name = elements[0] as? String else {
            throw dataCorruption("Directory node name has an unexpected Tuple shape")
        }
        return name
    }

    private func directory(
        at address: StorageAddress,
        edge: Edge,
        layerRoot: ByteString
    ) -> Directory {
        Directory(
            domain: transactionDomain,
            address: address,
            layer: edge.layer,
            keyspacePrefix: edge.prefix,
            layerRoot: layerRoot
        )
    }

    // MARK: - Validation

    private func requireDomain(
        of transaction: any TransactionReadAccess,
        operation: StorageOperation
    ) throws {
        guard transaction.transactionDomain === transactionDomain else {
            throw domainMismatch(operation: operation, subject: "Transaction")
        }
    }

    private func requireDomain(
        of transaction: any TransactionReadAccess,
        parent: Directory,
        operation: StorageOperation
    ) throws {
        try requireDomain(of: transaction, operation: operation)
        guard parent.domain === transactionDomain else {
            throw domainMismatch(operation: operation, subject: "Directory")
        }
    }

    private func requireLayer(
        _ stored: LayerTag,
        expected: LayerTag?,
        name: String,
        operation: StorageOperation
    ) throws {
        guard let expected, stored != expected else {
            return
        }
        throw StorageError.directoryLayerMismatch(
            "Node '\(name)' carries a different layer tag than the caller expected",
            operation: operation,
            backend: backend
        )
    }

    private func admitMutation(operation: StorageOperation) throws {
        guard let mutationAdmission else { return }
        try mutationAdmission(operation)
    }

    private func requireListLimit(_ limit: Int) throws {
        guard limit >= 1, limit <= DirectoryLimits.maximumListLimit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: backend,
                message: "Directory listing limit \(limit) must be between 1 and \(DirectoryLimits.maximumListLimit)"
            )
        }
    }

    private func validate(
        component: String,
        operation: StorageOperation
    ) throws {
        do {
            try StorageAddress.validateComponent(component)
        } catch {
            throw StorageError.invalidDirectoryAddress(error, operation: operation, backend: backend)
        }
    }

    private func childAddress(
        of parent: Directory,
        _ name: String,
        operation: StorageOperation
    ) throws -> StorageAddress {
        do {
            return try parent.address.appending(name)
        } catch {
            throw StorageError.invalidDirectoryAddress(error, operation: operation, backend: backend)
        }
    }

    private func clearRange(
        prefix: ByteString,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clearRange(beginKey: prefix, endKey: try increment(prefix))
    }

    private func increment(_ prefix: ByteString) throws -> ByteString {
        do {
            return try strinc(prefix)
        } catch {
            throw contractViolation("Catalog key prefix cannot be incremented: \(error)")
        }
    }

    // MARK: - Errors

    private func domainMismatch(
        operation: StorageOperation,
        subject: String
    ) -> StorageError {
        StorageError.storageDomainMismatch(
            "\(subject) belongs to a different storage engine than this Directory catalog",
            operation: operation,
            backend: backend
        )
    }

    private func notFound(_ message: String, operation: StorageOperation) -> StorageError {
        StorageError(code: .keyNotFound, operation: operation, backend: backend, message: message)
    }

    private func dataCorruption(_ message: String) -> StorageError {
        StorageError(code: .dataCorruption, operation: .read, backend: backend, message: message)
    }

    private func contractViolation(_ message: String) -> StorageError {
        StorageError(
            code: .backendContractViolation,
            operation: .read,
            backend: backend,
            message: message
        )
    }
}
