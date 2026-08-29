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
/// native layer: the layout marker state machine, domain checks, address
/// validation, layer-tag verification on every open, Partition boundary
/// rejection, own-subtree rejection, and lease intents. Path and layer mapping
/// is owned by `FDBDirectoryLayout`.
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

    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        let operation = StorageOperation.open
        let storage = try resolve(transaction, operation: operation)
        switch try await StorageLayoutMarker.inspect(transaction: transaction) {
        case .openV1:
            break
        case .uninitialized:
            return nil
        case .rejected(let rejection):
            throw StorageError.incompatibleStorageLayout(rejection, backend: backend)
        }
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            // `exists` resolves the path without touching the layer version
            // key, so a cluster whose marker is set but whose root path was
            // never created is observed without a write.
            guard try await layer.exists(path: rootPath, transaction: native) else {
                return nil
            }
            let node = try await layer.open(path: rootPath, transaction: native)
            return try requireRoot(node, operation: operation)
        }
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        let operation = StorageOperation.initialize
        let storage = try resolve(transaction, operation: operation)
        switch try await StorageLayoutMarker.inspect(transaction: transaction) {
        case .openV1:
            break
        case .uninitialized:
            // The marker and the root node commit in one transaction, so a
            // reader never observes a marked layout without its catalog.
            try transaction.setValue(StorageLayoutMarker.v1, for: StorageLayoutMarker.key)
        case .rejected(let rejection):
            throw StorageError.incompatibleStorageLayout(rejection, backend: backend)
        }
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            let node = try await layer.createOrOpen(
                path: rootPath,
                type: nil,
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
        return try await withLayer(storage, writes: true, operation: operation) { layer, native in
            // One native call opens or creates the node. Resolving the path
            // first to inspect an existing node would descend it twice on
            // every create, and the tag is verified below either way: the
            // native layer rejects a stored tag that differs from a stated
            // type, and states no type for the default tag.
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
        let bound = after.map { ByteString(utf8: $0) }
        let layerRoot = parent.childLayerRoot
        let path = nativePath(of: parent.address)
        return try await withLayer(storage, writes: false, operation: operation) { layer, native in
            let names: [String]
            do {
                names = try await layer.list(path: path, transaction: native)
            } catch DirectoryError.directoryNotFound {
                // A stale parent has no children, matching the KeyValue catalog,
                // which range-reads the parent's edges without resolving it.
                return []
            }
            // The native layer returns names in Swift `String` order, which is
            // not UTF-8 byte order, and paginates nothing.
            let ordered = names
                .map { (name: $0, bytes: ByteString(utf8: $0)) }
                .sorted { $0.bytes < $1.bytes }
            var entries: [DirectoryEntry] = []
            entries.reserveCapacity(min(limit, ordered.count))
            for child in ordered {
                if let bound, child.bytes <= bound {
                    continue
                }
                guard entries.count < limit else {
                    break
                }
                let address = try childAddress(of: parent, child.name, operation: operation)
                guard let node = try await openIfExists(
                    layer,
                    path: nativePath(of: address),
                    transaction: native
                ) else {
                    continue
                }
                let resolved = try directory(
                    at: address,
                    node: node,
                    layerRoot: layerRoot,
                    operation: operation
                )
                entries.append(DirectoryEntry(name: child.name, layer: resolved.layer))
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
            try transactionDomain.leases.registerIntent(
                covering: movedAddress,
                transaction: storage,
                operation: operation,
                backend: backend
            )
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
            try transactionDomain.leases.registerIntent(
                covering: address,
                transaction: storage,
                operation: operation,
                backend: backend
            )
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

    /// The catalog root is a plain Directory in the native root layer, so its
    /// children are allocated from that layer's content base.
    private func requireRoot(
        _ node: DirectorySubspace,
        operation: StorageOperation
    ) throws -> Directory {
        let root = try directory(
            at: .root,
            node: node,
            layerRoot: ByteString(),
            operation: operation
        )
        guard root.layer.isDefault else {
            throw StorageError.directoryLayerMismatch(
                "The configured root path is not a plain Directory",
                operation: operation,
                backend: backend
            )
        }
        return root
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
            try DirectoryPath.validateComponent(component)
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
