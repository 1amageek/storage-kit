import DatabaseTypes
import FoundationDB
import StorageKit

/// FoundationDB realization of `DirectoryAccess` over the native Directory
/// Layer (SPEC §7.3).
///
/// Node existence, prefixes, and layer tags live only in the native layer
/// metadata below the configured root path, so the native layer is the sole
/// existence authority for this backend. A Partition is a native `partition`
/// node, which is a nested Directory Layer over one contiguous prefix, so every
/// descendant of a Partition is allocated inside it and one range covers the
/// whole Partition. A plain Directory allocates its children as prefix-free
/// siblings of the layer that owns it, which contiguity does not require.
///
/// This type adds what the shared Directory contract requires on top of the
/// native layer: root bootstrap, domain checks, address validation, layer-tag
/// verification on every open, Partition boundary rejection, own-subtree
/// rejection, and lease intents. Path and layer mapping is owned by
/// `FDBDirectoryLayout`.
///
/// Every native call runs inside `FDBStorageTransaction.withDirectoryOperation`,
/// which keeps the Directory operation exclusive with the transaction's own
/// reads and writes and marks the transaction as mutated for mutating
/// operations.
final class FDBDirectoryAccess: DirectoryAccess, Sendable {
    let transactionDomain: StorageTransactionDomain
    let backend: StorageBackend = .foundationDB
    let rootPath: [String]

    init(transactionDomain: StorageTransactionDomain, rootPath: [String]) {
        self.transactionDomain = transactionDomain
        self.rootPath = rootPath
    }

    // MARK: - Root

    /// The witness the native layer holds in its own slot on the node at
    /// `rootPath`: the sole evidence that this root is initialized
    /// (SPEC §8.7, FD-1).
    ///
    /// The native Directory Layer reserves one slot per node beside that node's
    /// layer value and child edges, owns its physical coordinate, and resolves
    /// it from a path through nested Partition layers included. This adapter
    /// therefore never computes, reads, or writes a metadata key; it owns only
    /// what the recorded identifier means.
    ///
    /// Existence of the node is not the evidence: the native layer creates the
    /// ancestors of a path as ordinary untyped Directories, so opening a root
    /// at `["a", "b"]` brings `["a"]` into existence as a side effect. A node
    /// at `rootPath` whose slot is absent, or holds different bytes, is refused
    /// rather than adopted or overwritten.
    ///
    /// The slot is disjoint from every content key and range this adapter can
    /// derive, so clearing the root's own content prefix leaves the root
    /// openable. A raw Layer 0 write can still reach the bytes, which is why
    /// the identifier is compared exactly rather than by presence.
    ///
    /// The witness is not a layer value. SPEC §4 gives every tag other than
    /// `partition` to the application, so reading a root out of one would make
    /// a caller's own tag either forge a storage root or be refused.
    ///
    /// One cluster hosts one storage root per root path, so existence is asked
    /// of that node and never of the cluster. Another root's nodes and the
    /// native allocator counters are not this root's data.
    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        let operation = StorageOperation.open
        let storage = try resolve(transaction, operation: operation)
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            // Observation resolves the path and reads one slot without touching
            // the layer version key, so an uninitialized root is observed
            // without any write.
            let observation = try await layer.inspectWitness(
                try Self.rootWitness(),
                at: rootPath,
                transaction: native
            )
            guard let node = observation.node else {
                try await requireNoAncestorRoot(layer, transaction: native, operation: operation)
                return nil
            }
            return try requireRoot(
                node,
                witness: observation.witness,
                operation: operation
            )
        }
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        let operation = StorageOperation.initialize
        let storage = try resolve(transaction, operation: operation)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            let witness = try Self.rootWitness()
            // The node is observed before it is created, so an existing node is
            // adjudicated by `requireRoot`: an untyped node this catalog never
            // witnessed is reported as an incompatible layout rather than
            // adopted, and one carrying a layer value keeps the failure that
            // names what is actually there.
            let observation = try await layer.inspectWitness(
                witness,
                at: rootPath,
                transaction: native
            )
            if let node = observation.node {
                return try requireRoot(
                    node,
                    witness: observation.witness,
                    operation: operation
                )
            }
            // Creation brings the proper ancestors of `rootPath` into existence
            // as untyped nodes, so they are adjudicated before that happens.
            try await requireNoAncestorRoot(layer, transaction: native, operation: operation)
            // Creation is strict rather than create-or-open, so the node and
            // its witness commit or roll back together in the caller's
            // transaction and a root is never observed created but unwitnessed.
            let node = try await layer.create(
                path: rootPath,
                type: nil,
                recording: witness,
                transaction: native
            )
            return try requireRootShape(node, operation: operation)
        }
    }

    // MARK: - Children

    func open(
        _ name: String,
        expecting expected: LayerTag?,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        let operation = StorageOperation.open
        let storage = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, name, operation: operation)
        let layerRoot = parent.childLayerRoot
        let path = nativePath(of: address)
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            guard let node = try await openIfExists(layer, path: path, transaction: native) else {
                return nil
            }
            let child = try directory(
                at: address,
                node: node,
                layerRoot: layerRoot,
                operation: operation
            )
            // The native layer verifies nothing when the caller states no type,
            // so the expected tag is verified here for every open.
            try requireLayer(child.layer, expected: expected, name: name, operation: operation)
            return child
        }
    }

    func openOrCreate(
        _ name: String,
        layer tag: LayerTag,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let operation = StorageOperation.write
        let storage = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, name, operation: operation)
        let type = FDBDirectoryLayout.nativeType(for: tag)
        // The native create recreates every missing ancestor as an untyped
        // node, so a removed parent would otherwise be rebuilt into a tree that
        // no operation created and no invariant describes. Re-resolving refuses
        // that first, and it is what D-13 requires of operation 2 in any case:
        // the child is created under, and reported inside, the node the address
        // names now.
        let living = try await requireLiving(
            parent,
            "Parent Directory of '\(name)' does not exist",
            operation: operation,
            transaction: transaction
        )
        let layerRoot = living.childLayerRoot
        let path = nativePath(of: address)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            // Resolving the child first keeps an existing node at one descent.
            // The tag is verified here rather than by the native layer, matching
            // `open`: a stored tag that differs is never adopted.
            if let existing = try await openIfExists(layer, path: path, transaction: native) {
                let child = try directory(
                    at: address,
                    node: existing,
                    layerRoot: layerRoot,
                    operation: operation
                )
                try requireLayer(child.layer, expected: tag, name: name, operation: operation)
                return child
            }
            let node = try await layer.createOrOpen(
                path: path,
                type: type,
                transaction: native
            )
            let child = try directory(
                at: address,
                node: node,
                layerRoot: layerRoot,
                operation: operation
            )
            try requireLayer(child.layer, expected: tag, name: name, operation: operation)
            return child
        }
    }

    func listChildren(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [DirectoryEntry] {
        let operation = StorageOperation.rangeRead
        let storage = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        try validate(limit: limit, operation: operation)
        if let after {
            try validate(component: after, operation: operation)
        }
        let layerRoot = parent.childLayerRoot
        let path = nativePath(of: parent.address)
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            // Both bounds go to the native subdirectory range read, so the page
            // is produced by the store in the store's own key order. Tuple
            // string encoding preserves UTF-8 byte order, which is the order
            // D-8 states and the order `after` resumes from.
            let names: [String]
            do {
                names = try await layer.list(
                    path: path,
                    after: after,
                    limit: limit,
                    transaction: native
                )
            } catch DirectoryError.directoryNotFound {
                // A stale parent has no children, matching the KeyValue catalog,
                // which range-reads the parent's edges without resolving it.
                return []
            }
            var entries: [DirectoryEntry] = []
            entries.reserveCapacity(names.count)
            for name in names {
                let address = try childAddress(of: parent, name, operation: operation)
                // The subdirectory entry and the node it names are written
                // together, and this read sees one snapshot of both. A listed
                // child that does not resolve is corrupted metadata: dropping
                // it would shorten the page, and a short page is how this
                // contract reports the end of the enumeration.
                guard let node = try await openIfExists(
                    layer,
                    path: nativePath(of: address),
                    transaction: native
                ) else {
                    throw StorageError(
                        code: .dataCorruption,
                        operation: operation,
                        backend: backend,
                        message: "Directory '\(name)' is listed by its parent but has no node"
                    )
                }
                let resolved = try directory(
                    at: address,
                    node: node,
                    layerRoot: layerRoot,
                    operation: operation
                )
                entries.append(DirectoryEntry(name: name, layer: resolved.layer))
            }
            return entries
        }
    }

    func move(
        _ name: String,
        in source: Directory,
        to newName: String,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let operation = StorageOperation.write
        let storage = try resolve(transaction, operation: operation)
        try requireDomain(of: source, operation: operation)
        guard destination.domain === transactionDomain else {
            throw StorageError.storageDomainMismatch(
                "Destination Directory belongs to a different storage engine",
                operation: operation,
                backend: backend
            )
        }
        let movedAddress = try childAddress(of: source, name, operation: operation)
        let targetAddress = try childAddress(of: destination, newName, operation: operation)
        // D-13: one operation reads both endpoints through the same authority,
        // so the source is re-resolved as well as the destination. The native
        // move creates a missing destination parent, which this refuses before
        // any write, and the comparison below then holds two live layer bases
        // rather than one the caller has been carrying.
        let livingSource = try await requireLiving(
            source,
            "Source Directory does not exist",
            operation: operation,
            transaction: transaction
        )
        let livingDestination = try await requireLiving(
            destination,
            "Destination Directory does not exist",
            operation: operation,
            transaction: transaction
        )
        let layerRoot = livingSource.childLayerRoot
        // The two nodes share a Directory Layer exactly when their contents are
        // allocated from the same content base, so this is the whole Partition
        // boundary rule: a Partition node itself moves within its own layer,
        // and nothing moves into or out of a Partition.
        guard layerRoot == livingDestination.childLayerRoot else {
            throw StorageError.partitionBoundaryViolation(
                "A node cannot move into or out of a Partition",
                operation: operation,
                backend: backend
            )
        }
        let oldPath = nativePath(of: movedAddress)
        let newPath = nativePath(of: targetAddress)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            guard let node = try await openIfExists(layer, path: oldPath, transaction: native) else {
                throw notFound(
                    "Node '\(name)' does not exist in the source Directory",
                    operation: operation
                )
            }
            if movedAddress.isAncestorOrSelf(of: livingDestination.address)
                || livingDestination.keyspacePrefix == ByteString(node.prefix) {
                throw StorageError.invalidDirectoryAddress(
                    .targetInsideMovedSubtree,
                    operation: operation,
                    backend: backend
                )
            }
            // Rejected here rather than by the native move so no lease intent is
            // registered for a move that cannot happen.
            guard try await layer.exists(path: newPath, transaction: native) == false else {
                throw StorageError(
                    code: .invalidOperation,
                    operation: operation,
                    backend: backend,
                    message: "Node '\(newName)' already exists in the destination Directory"
                )
            }
            let moved = try await layer.move(
                oldPath: oldPath,
                newPath: newPath,
                transaction: native
            )
            return try directory(
                at: targetAddress,
                node: moved,
                layerRoot: layerRoot,
                operation: operation
            )
        }
    }

    func remove(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        let operation = StorageOperation.delete
        let storage = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, name, operation: operation)
        let path = nativePath(of: address)
        try await withLayer(storage, writes: true, operation: operation) { layer, native in
            guard try await layer.exists(path: path, transaction: native) else {
                throw notFound(
                    "Node '\(name)' does not exist in the parent Directory",
                    operation: operation
                )
            }
            // Removal is recursive and has no emptiness precondition: the
            // native layer clears every descendant node and the whole content
            // range of the removed node, which for a Partition is one range.
            try await layer.remove(path: path, transaction: native)
        }
    }

    // MARK: - Resolution

    /// Resolves what a caller-held `Directory` names now, inside the caller's
    /// own transaction, and fails when nothing does (D-13).
    ///
    /// The native layer positions by path, so a write already lands in whatever
    /// the address resolves to. What the caller holds beyond the address is the
    /// physical context of an earlier resolution: a content prefix and the
    /// content base of the Directory Layer that contained the node then. A
    /// value built from those would pair a live node with a superseded
    /// Partition boundary, and the next move wholly inside the live Partition
    /// would read that base and reject itself. The walk resolves from the root,
    /// so every value it produces carries the live prefix and the live base.
    ///
    /// It runs outside `withDirectoryOperation`: each of its steps takes that
    /// same exclusion, which is not reentrant. It costs at most
    /// `DirectoryLimits.maximumDepth` native opens, and every node it reads
    /// joins the transaction's read set, so a concurrent removal of any
    /// ancestor conflicts with the operation that called it.
    private func requireLiving(
        _ directory: Directory,
        _ message: @autoclosure () -> String,
        operation: StorageOperation,
        transaction: any TransactionReadAccess
    ) async throws -> Directory {
        guard let living = try await openDirectory(
            at: directory.address,
            transaction: transaction
        ) else {
            throw notFound(message(), operation: operation)
        }
        return living
    }

    // MARK: - Native layer

    private func withLayer<T: Sendable>(
        _ transaction: FDBStorageTransaction,
        writes: Bool,
        operation: StorageOperation,
        _ body: (DirectoryLayer, any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let database = try transaction.retainedDatabaseForDirectoryOperation(operation: operation)
        let layer = DirectoryLayer(database: database)
        do {
            return try await transaction.withDirectoryOperation(
                transactionDomain: transactionDomain,
                writes: writes,
                operation: operation
            ) { native in
                try await body(layer, native)
            }
        } catch is CancellationError {
            // Cancellation is a Swift task signal, not a backend failure. The
            // transaction paths already surface it unchanged, so the Directory
            // path must not convert it into `backendFailure`.
            throw CancellationError()
        } catch {
            throw Self.convert(error, operation: operation)
        }
    }

    private func openIfExists(
        _ layer: DirectoryLayer,
        path: [String],
        transaction: any TransactionProtocol
    ) async throws -> DirectorySubspace? {
        do {
            return try await layer.open(path: path, transaction: transaction)
        } catch DirectoryError.directoryNotFound {
            return nil
        }
    }

    private func nativePath(of address: StorageAddress) -> [String] {
        FDBDirectoryLayout.nativePath(rootPath: rootPath, address: address)
    }

    private func directory(
        at address: StorageAddress,
        node: DirectorySubspace,
        layerRoot: ByteString,
        operation: StorageOperation
    ) throws -> Directory {
        let tag = try FDBDirectoryLayout.layerTag(
            for: node.type,
            operation: operation,
            backend: backend
        )
        return Directory(
            domain: transactionDomain,
            address: address,
            layer: tag,
            keyspacePrefix: ByteString(node.prefix),
            layerRoot: layerRoot
        )
    }

    /// The catalog root is the witnessed node at `rootPath`, and its children
    /// are allocated from the native root layer's content base.
    ///
    /// A node this catalog initialized is untyped, so any layer value refuses
    /// the node before its witness is considered: a native partition names the
    /// shape mismatch, and any other type is a node someone else placed at this
    /// path. A witness on such a node would claim a keyspace this adapter does
    /// not own.
    private func requireRoot(
        _ node: DirectorySubspace,
        witness: DirectoryNodeObservation.Witness,
        operation: StorageOperation
    ) throws -> Directory {
        let root = try requireRootShape(node, operation: operation)
        switch witness {
        case .matching:
            return root
        case .absent:
            throw StorageError.incompatibleStorageLayout(
                "the node at the configured root path was not initialized as a storage root",
                operation: operation,
                backend: backend
            )
        case .conflicting:
            throw StorageError.incompatibleStorageLayout(
                "the storage root witness holds a value no Directory catalog wrote",
                operation: operation,
                backend: backend
            )
        }
    }

    /// The root `Directory` of an untyped node, refusing every typed one.
    ///
    /// The root is returned with `LayerTag.default` instead of being mapped
    /// through `FDBDirectoryLayout.layerTag`, so both backend classes expose
    /// the same root value.
    private func requireRootShape(
        _ node: DirectorySubspace,
        operation: StorageOperation
    ) throws -> Directory {
        if let type = node.type {
            guard type != DirectoryType.partition else {
                throw StorageError.directoryLayerMismatch(
                    "The configured root path is not a plain Directory",
                    operation: operation,
                    backend: backend
                )
            }
            throw StorageError.incompatibleStorageLayout(
                "the node at the configured root path carries a layer value this catalog never wrote",
                operation: operation,
                backend: backend
            )
        }
        return Directory(
            domain: transactionDomain,
            address: .root,
            layer: .default,
            keyspacePrefix: ByteString(node.prefix),
            layerRoot: ByteString()
        )
    }

    /// The witness this catalog records on, and expects to find on, its own
    /// root node.
    ///
    /// The identifier is a fixed constant well inside the bound the bindings
    /// admit, so a rejection here is this adapter breaking the bindings'
    /// contract rather than a fact about the store, and `convert` reports it as
    /// `backendContractViolation`.
    private static func rootWitness() throws -> DirectoryNodeWitness {
        try DirectoryNodeWitness(
            identifier: Array(FDBDirectoryLayout.rootWitnessIdentifier)
        )
    }

    /// Refuses a root path that lies inside another storage root (FD-1a).
    ///
    /// One observation per proper ancestor of `rootPath` is read in the
    /// caller's transaction. The default root path has a single component and
    /// therefore no proper ancestor, so this walk reads nothing there. The
    /// reverse order of creation is already refused by `requireRoot`, because
    /// the outer path then holds the unwitnessed node the inner root's own
    /// creation left behind.
    ///
    /// Any recorded witness refuses the path, whatever the node's layer value
    /// is: the slot lives in the node's own metadata, so a typed node can carry
    /// one too. A matching witness is another storage root, and a conflicting
    /// one is corruption in a node this adapter would otherwise write straight
    /// through. An absent node holds no witness, so a missing ancestor and a
    /// clean one are the same answer.
    private func requireNoAncestorRoot(
        _ layer: DirectoryLayer,
        transaction native: any TransactionProtocol,
        operation: StorageOperation
    ) async throws {
        guard rootPath.count > 1 else {
            return
        }
        let witness = try Self.rootWitness()
        for end in 1..<rootPath.count {
            let ancestor = Array(rootPath[0..<end])
            let observation = try await layer.inspectWitness(
                witness,
                at: ancestor,
                transaction: native
            )
            switch observation.witness {
            case .absent:
                continue
            case .matching:
                throw StorageError.incompatibleStorageLayout(
                    """
                    the configured root path lies inside the storage root at \
                    '\(ancestor.joined(separator: "/"))'
                    """,
                    operation: operation,
                    backend: backend
                )
            case .conflicting:
                throw StorageError.incompatibleStorageLayout(
                    """
                    the node at '\(ancestor.joined(separator: "/"))' on the \
                    configured root path holds a storage root witness no \
                    Directory catalog wrote
                    """,
                    operation: operation,
                    backend: backend
                )
            }
        }
    }

    // MARK: - Validation

    private func resolve(
        _ transaction: any TransactionReadAccess,
        operation: StorageOperation
    ) throws -> FDBStorageTransaction {
        guard let transaction = transaction as? FDBStorageTransaction,
              transaction.transactionDomain === transactionDomain
        else {
            throw StorageError.storageDomainMismatch(
                "Transaction belongs to a different storage engine",
                operation: operation,
                backend: backend
            )
        }
        return transaction
    }

    private func requireDomain(of directory: Directory, operation: StorageOperation) throws {
        guard directory.domain === transactionDomain else {
            throw StorageError.storageDomainMismatch(
                "Directory belongs to a different storage engine",
                operation: operation,
                backend: backend
            )
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

    private func validate(component: String, operation: StorageOperation) throws {
        do {
            try StorageAddress.validateComponent(component)
        } catch {
            throw StorageError.invalidDirectoryAddress(error, operation: operation, backend: backend)
        }
    }

    private func validate(limit: Int, operation: StorageOperation) throws {
        guard limit >= 1, limit <= DirectoryLimits.maximumListLimit else {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: backend,
                message: "Directory listing limit \(limit) must be between 1 and \(DirectoryLimits.maximumListLimit)"
            )
        }
    }

    private func notFound(_ message: String, operation: StorageOperation) -> StorageError {
        StorageError(code: .keyNotFound, operation: operation, backend: backend, message: message)
    }

    // MARK: - Error conversion

    private static func convert(_ error: any Error, operation: StorageOperation) -> StorageError {
        switch error {
        case let error as StorageError:
            return error
        case let error as DirectoryError:
            return convert(error, operation: operation)
        case let error as FDBError:
            return FDBStorageTransaction.convertFDBError(error, operation: operation)
        default:
            return FDBStorageTransaction.convertBackendError(error, operation: operation)
        }
    }

    private static func convert(_ error: DirectoryError, operation: StorageOperation) -> StorageError {
        let backend = StorageBackend.foundationDB
        switch error {
        case .directoryNotFound(let path):
            return StorageError(
                code: .keyNotFound,
                operation: operation,
                backend: backend,
                message: "Directory node does not exist: \(path)"
            )
        case .directoryAlreadyExists(let path):
            return StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: backend,
                message: "Directory node already exists: \(path)"
            )
        case .invalidPath(_, let reason):
            return StorageError(
                code: .invalidDirectoryAddress,
                operation: operation,
                backend: backend,
                message: "Native Directory path rejected: \(reason)"
            )
        case .layerMismatch(let expected, let actual):
            return StorageError.directoryLayerMismatch(
                "Node carries layer \(actual?.rawValue ?? []) where \(expected?.rawValue ?? []) was expected",
                operation: operation,
                backend: backend
            )
        case .cannotMoveAcrossPartitions:
            return StorageError.partitionBoundaryViolation(
                "A node cannot move into or out of a Partition",
                operation: operation,
                backend: backend
            )
        case .cannotCreatePartitionInPartition:
            // Nested Partition creation is permitted; the native layer no longer
            // raises this case.
            return StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: backend,
                message: "Native Directory Layer rejected nested Partition creation: \(error)"
            )
        case .incompatibleVersion, .invalidVersion, .invalidMetadata, .directoryLayerNotInitialized:
            return StorageError(
                code: .incompatibleStorageLayout,
                operation: operation,
                backend: backend,
                message: "Native Directory Layer metadata is not usable: \(error)"
            )
        case .invalidWitness(let reason):
            // The identifier is this adapter's own constant, so a rejection is
            // this adapter breaking the bindings' contract rather than a layout
            // fact about the store.
            return StorageError(
                code: .backendContractViolation,
                operation: operation,
                backend: backend,
                message: "Native Directory Layer rejected the node witness: \(reason)"
            )
        case .prefixInUse(let prefix), .prefixInMetadataSpace(let prefix):
            return StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: backend,
                message: "Native Directory prefix cannot be allocated: \(prefix)"
            )
        }
    }
}
