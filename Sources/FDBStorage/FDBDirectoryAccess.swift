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

    /// The root marker on the native node at `rootPath`: the sole witness that
    /// this root is initialized (SPEC §8.7, FD-1).
    ///
    /// This adapter owns no key of its own. The native Directory Layer never
    /// allocates a prefix that is already in use, so no StorageKit write can
    /// land on bytes written outside it, and the whole bootstrap question is
    /// which node this root owns. Existence of the node does not answer it:
    /// the native layer creates the ancestors of a path as ordinary
    /// empty-layer Directories, so opening a root at `["a", "b"]` brings
    /// `["a"]` into existence as a side effect. The witness is therefore
    /// `FDBDirectoryLayout.rootLayer`, which only initialization writes, and
    /// an unmarked node at `rootPath` is refused rather than adopted.
    ///
    /// One cluster hosts one storage root per root path, so existence is asked
    /// of that node and never of the cluster. Another root's nodes and the
    /// native allocator counters are not this root's data.
    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        let operation = StorageOperation.open
        let storage = try resolve(transaction, operation: operation)
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            // `exists` resolves the path without touching the layer version
            // key, so an uninitialized root is observed without any write.
            guard try await layer.exists(path: rootPath, transaction: native) else {
                try await requireNoAncestorRoot(layer, transaction: native, operation: operation)
                return nil
            }
            let node = try await layer.open(path: rootPath, transaction: native)
            return try requireRoot(node, operation: operation)
        }
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        let operation = StorageOperation.initialize
        let storage = try resolve(transaction, operation: operation)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            // The node is resolved before it is created, so an existing node
            // is adjudicated by `requireRoot` and reported as an incompatible
            // layout. Asking `createOrOpen` for the marker instead would let
            // the native layer answer the same state with its own layer
            // mismatch, which names a different failure than the one that
            // happened.
            if let node = try await openIfExists(layer, path: rootPath, transaction: native) {
                return try requireRoot(node, operation: operation)
            }
            try await requireNoAncestorRoot(layer, transaction: native, operation: operation)
            let node = try await layer.createOrOpen(
                path: rootPath,
                type: FDBDirectoryLayout.rootLayer,
                transaction: native
            )
            return try requireRoot(node, operation: operation)
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
        let layerRoot = parent.childLayerRoot
        let type = try FDBDirectoryLayout.nativeType(
            for: tag,
            operation: operation,
            backend: backend
        )
        let path = nativePath(of: address)
        let parentPath = nativePath(of: parent.address)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            // Resolving the child first keeps an existing node at one descent
            // and confines the parent check below to an actual creation. The
            // tag is verified here rather than by the native layer, matching
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
            // The native create recreates every missing ancestor as an untyped
            // node, so a removed parent would otherwise be rebuilt into a tree
            // that no operation created and no invariant describes. Operation 2
            // creates the named child only.
            guard try await layer.exists(path: parentPath, transaction: native) else {
                throw notFound(
                    "Parent Directory of '\(name)' does not exist",
                    operation: operation
                )
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
        let layerRoot = source.childLayerRoot
        // The two nodes share a Directory Layer exactly when their contents are
        // allocated from the same content base, so this is the whole Partition
        // boundary rule: a Partition node itself moves within its own layer,
        // and nothing moves into or out of a Partition.
        guard layerRoot == destination.childLayerRoot else {
            throw StorageError.partitionBoundaryViolation(
                "A node cannot move into or out of a Partition",
                operation: operation,
                backend: backend
            )
        }
        let oldPath = nativePath(of: movedAddress)
        let newPath = nativePath(of: targetAddress)
        let destinationPath = nativePath(of: destination.address)
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            guard let node = try await openIfExists(layer, path: oldPath, transaction: native) else {
                throw notFound(
                    "Node '\(name)' does not exist in the source Directory",
                    operation: operation
                )
            }
            if movedAddress.isAncestorOrSelf(of: destination.address)
                || destination.keyspacePrefix == ByteString(node.prefix) {
                throw StorageError.invalidDirectoryAddress(
                    .targetInsideMovedSubtree,
                    operation: operation,
                    backend: backend
                )
            }
            // The native move creates a missing destination parent, so a stale
            // destination would otherwise resurrect a removed path.
            guard try await layer.exists(path: destinationPath, transaction: native) else {
                throw notFound(
                    "Destination Directory does not exist",
                    operation: operation
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

    /// The catalog root is the marked node at `rootPath`, and its children are
    /// allocated from the native root layer's content base.
    ///
    /// The marker is verified here instead of being mapped through
    /// `FDBDirectoryLayout.layerTag`, and the root is returned with
    /// `LayerTag.default`, so both backend classes expose the same root value
    /// and no caller learns the marker.
    private func requireRoot(
        _ node: DirectorySubspace,
        operation: StorageOperation
    ) throws -> Directory {
        guard node.type != DirectoryType.partition else {
            throw StorageError.directoryLayerMismatch(
                "The configured root path is not a plain Directory",
                operation: operation,
                backend: backend
            )
        }
        guard node.type == FDBDirectoryLayout.rootLayer else {
            throw StorageError.incompatibleStorageLayout(
                "the node at the configured root path was not initialized as a storage root",
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

    /// Refuses a root path that lies inside another storage root (FD-1a).
    ///
    /// One node per proper ancestor of `rootPath` is read in the caller's
    /// transaction. The default root path has a single component and therefore
    /// no proper ancestor, so this walk reads nothing there. The reverse order
    /// of creation is already refused by `requireRoot`, because the outer path
    /// then holds the empty-layer node the inner root's own creation left
    /// behind.
    private func requireNoAncestorRoot(
        _ layer: DirectoryLayer,
        transaction native: any TransactionProtocol,
        operation: StorageOperation
    ) async throws {
        guard rootPath.count > 1 else {
            return
        }
        for end in 1..<rootPath.count {
            let ancestor = Array(rootPath[0..<end])
            guard let node = try await openIfExists(layer, path: ancestor, transaction: native),
                  node.type == FDBDirectoryLayout.rootLayer
            else {
                continue
            }
            throw StorageError.incompatibleStorageLayout(
                """
                the configured root path lies inside the storage root at \
                '\(ancestor.joined(separator: "/"))'
                """,
                operation: operation,
                backend: backend
            )
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
