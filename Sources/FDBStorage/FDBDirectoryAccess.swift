import DatabaseTypes
import FoundationDB
import StorageKit

/// FoundationDB realization of `DirectoryAccess` over the native Directory
/// Layer (SPEC §7.3).
///
/// Node existence, prefixes, and kinds live only in the native layer metadata
/// below the configured root path, so the native layer is the sole existence
/// authority for this backend. This type adds what the shared Directory
/// contract requires on top of it: domain checks, address validation, layer
/// type verification on every open, kind-prefixed native names, stale-parent
/// rejection, emptiness checks before removal, and lease intents. Layout rules
/// are owned by `FDBDirectoryLayout`.
///
/// Every native call runs inside `FDBStorageTransaction.withDirectoryOperation`,
/// which keeps the Directory operation exclusive with the transaction's own
/// reads and writes and marks the transaction as mutated for operations 5–8.
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
        let transaction = try resolve(transaction, operation: operation)
        return try await withLayer(transaction, writes: false, operation: operation) { layer, native in
            // `exists` resolves the path without touching the layer version
            // key, so an uninitialized cluster is observed without a write.
            guard try await layer.exists(path: rootPath, transaction: native) else {
                return nil
            }
            let node = try await layer.open(path: rootPath, transaction: native)
            try verify(node, is: FDBDirectoryLayout.directoryType)
            return directory(at: .root, node: node)
        }
    }

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        let operation = StorageOperation.initialize
        let transaction = try resolve(transaction, operation: operation)
        return try await withLayer(transaction, writes: true, operation: operation) { layer, native in
            let node = try await layer.createOrOpen(
                path: rootPath,
                type: FDBDirectoryLayout.directoryType,
                transaction: native
            )
            return directory(at: .root, node: node)
        }
    }

    // MARK: - Children

    func openDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try await openChild(.directory(name), in: parent, transaction: transaction)
    }

    func openOrCreateDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try await openOrCreateChild(.directory(name), in: parent, transaction: transaction)
    }

    func openPartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Partition? {
        guard let root = try await openChild(.partition(id), in: parent, transaction: transaction) else {
            return nil
        }
        return Partition(id: id, root: root)
    }

    func openOrCreatePartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Partition {
        let root = try await openOrCreateChild(.partition(id), in: parent, transaction: transaction)
        return Partition(id: id, root: root)
    }

    private func openChild(
        _ step: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        let operation = StorageOperation.open
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, step: step, operation: operation)
        let path = nativePath(address)
        let expectedType = FDBDirectoryLayout.nativeType(for: step)
        return try await withLayer(transaction, writes: false, operation: operation) { layer, native in
            guard let node = try await openIfExists(layer, path: path, transaction: native) else {
                return nil
            }
            try verify(node, is: expectedType)
            return directory(at: address, node: node)
        }
    }

    private func openOrCreateChild(
        _ step: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let operation = StorageOperation.write
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, step: step, operation: operation)
        let parentPath = nativePath(parent.address)
        let path = nativePath(address)
        let expectedType = FDBDirectoryLayout.nativeType(for: step)
        return try await withLayer(transaction, writes: true, operation: operation) { layer, native in
            // The native layer creates a missing parent as an untyped node;
            // a stale parent value must fail instead of resurrecting it.
            guard try await layer.exists(path: parentPath, transaction: native) else {
                throw notFound("Parent Directory no longer exists", operation: operation)
            }
            let node = try await layer.createOrOpen(path: path, type: expectedType, transaction: native)
            return directory(at: address, node: node)
        }
    }

    // MARK: - Listing

    func listDirectories(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [String] {
        let operation = StorageOperation.rangeRead
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        try validate(limit: limit, operation: operation)
        let path = nativePath(parent.address)
        let afterBytes = after.map { Array($0.utf8) }
        return try await withLayer(transaction, writes: false, operation: operation) { layer, native in
            let components = try await layer.list(path: path, transaction: native)
            var entries: [(bytes: [UInt8], name: String)] = []
            for component in components {
                guard case .directory(let name) = FDBDirectoryLayout.decode(component) else {
                    continue
                }
                let bytes = Array(name.utf8)
                if let afterBytes, !afterBytes.lexicographicallyPrecedes(bytes) {
                    continue
                }
                entries.append((bytes, name))
            }
            entries.sort { $0.bytes.lexicographicallyPrecedes($1.bytes) }
            return entries.prefix(limit).map(\.name)
        }
    }

    func listPartitions(
        in parent: Directory,
        after: PartitionID?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [PartitionID] {
        let operation = StorageOperation.rangeRead
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        try validate(limit: limit, operation: operation)
        let path = nativePath(parent.address)
        let afterBytes = after.map { $0.bytes.copyBytes() }
        return try await withLayer(transaction, writes: false, operation: operation) { layer, native in
            let components = try await layer.list(path: path, transaction: native)
            var entries: [(bytes: [UInt8], id: PartitionID)] = []
            for component in components {
                switch FDBDirectoryLayout.decode(component) {
                case .partition(let id):
                    let bytes = id.bytes.copyBytes()
                    if let afterBytes, !afterBytes.lexicographicallyPrecedes(bytes) {
                        continue
                    }
                    entries.append((bytes, id))
                case .corrupt(let component):
                    throw StorageError(
                        code: .dataCorruption,
                        operation: operation,
                        backend: backend,
                        message: "Partition node name is not a valid identifier encoding: \(component)"
                    )
                case .directory, .foreign:
                    continue
                }
            }
            entries.sort { $0.bytes.lexicographicallyPrecedes($1.bytes) }
            return entries.prefix(limit).map(\.id)
        }
    }

    // MARK: - Move and Remove

    func moveChild(
        _ child: StorageAddressStep,
        in source: Directory,
        to newChild: StorageAddressStep,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let operation = StorageOperation.write
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: source, operation: operation)
        try requireDomain(of: destination, operation: operation)
        guard case .directory = child, case .directory = newChild else {
            throw StorageError.unsupportedOperation(
                "Partitions cannot be moved; only Directories move",
                operation: operation,
                backend: backend
            )
        }
        let sourceAddress = try childAddress(of: source, step: child, operation: operation)
        let targetAddress = try childAddress(of: destination, step: newChild, operation: operation)
        guard !sourceAddress.isAncestorOrSelf(of: targetAddress) else {
            throw StorageError.invalidDirectoryAddress(
                .targetInsideMovedSubtree,
                operation: operation,
                backend: backend
            )
        }
        let sourcePath = nativePath(sourceAddress)
        let destinationPath = nativePath(destination.address)
        let targetPath = nativePath(targetAddress)
        return try await withLayer(transaction, writes: true, operation: operation) { layer, native in
            guard let moved = try await openIfExists(layer, path: sourcePath, transaction: native) else {
                throw notFound("Directory to move does not exist", operation: operation)
            }
            try verify(moved, is: FDBDirectoryLayout.directoryType)
            // The native layer creates a missing destination as an untyped
            // node and would silently accept a stale destination value.
            guard try await layer.exists(path: destinationPath, transaction: native) else {
                throw notFound("Destination Directory no longer exists", operation: operation)
            }
            let targetExists = try await layer.exists(path: targetPath, transaction: native)
            guard !targetExists else {
                throw StorageError(
                    code: .invalidOperation,
                    operation: operation,
                    backend: backend,
                    message: "Move target already exists"
                )
            }
            // The intent is registered only once the move is certain to be
            // attempted, so a rejected move leaves no pending intent behind.
            try transactionDomain.leases.registerIntent(
                covering: sourceAddress,
                transaction: transaction,
                operation: operation,
                backend: backend
            )
            let node = try await layer.move(oldPath: sourcePath, newPath: targetPath, transaction: native)
            return directory(at: targetAddress, node: node)
        }
    }

    func removeChild(
        _ child: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        let operation = StorageOperation.delete
        let transaction = try resolve(transaction, operation: operation)
        try requireDomain(of: parent, operation: operation)
        let address = try childAddress(of: parent, step: child, operation: operation)
        let path = nativePath(address)
        let expectedType = FDBDirectoryLayout.nativeType(for: child)
        try await withLayer(transaction, writes: true, operation: operation) { layer, native in
            guard let node = try await openIfExists(layer, path: path, transaction: native) else {
                throw notFound("Directory to remove does not exist", operation: operation)
            }
            try verify(node, is: expectedType)
            let children = try await layer.list(path: path, transaction: native)
            guard children.isEmpty else {
                throw StorageError.directoryNotEmpty(
                    "Directory still has child Directories or Partitions",
                    backend: backend
                )
            }
            let range = try FoundationDB.Subspace(prefix: node.prefix).prefixRange()
            var hasData = false
            for try await _ in native.getRange(from: range.begin, to: range.end, limit: 1) {
                hasData = true
                break
            }
            guard !hasData else {
                throw StorageError.directoryNotEmpty(
                    "Directory still contains data",
                    backend: backend
                )
            }
            // The intent is registered only once the removal is certain to be
            // attempted, so a rejected removal leaves no pending intent behind.
            try transactionDomain.leases.registerIntent(
                covering: address,
                transaction: transaction,
                operation: operation,
                backend: backend
            )
            try await layer.remove(path: path, transaction: native)
        }
    }

    // MARK: - Native access

    /// Runs `body` against a Directory Layer bound to the transaction's
    /// retained database, inside the transaction's exclusive Directory
    /// operation window, and converts every backend failure to `StorageError`.
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

    private func verify(_ node: DirectorySubspace, is expected: DirectoryType) throws {
        guard node.type == expected else {
            throw StorageError.incompatibleStorageLayout(
                .unknownMarker(ByteString(node.type?.rawValue ?? [])),
                backend: backend
            )
        }
    }

    private func directory(at address: StorageAddress, node: DirectorySubspace) -> Directory {
        Directory(
            domain: transactionDomain,
            address: address,
            root: StorageKit.Subspace(prefix: ByteString(node.prefix))
        )
    }

    private func nativePath(_ address: StorageAddress) -> [String] {
        FDBDirectoryLayout.nativePath(rootPath: rootPath, address: address)
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

    private func childAddress(
        of parent: Directory,
        step: StorageAddressStep,
        operation: StorageOperation
    ) throws -> StorageAddress {
        switch Result(catching: { () throws(DirectoryAddressError) in try parent.address.appending(step) }) {
        case .success(let address):
            return address
        case .failure(let error):
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
        case .layerMismatch(_, let actual):
            return StorageError.incompatibleStorageLayout(
                .unknownMarker(ByteString(actual?.rawValue ?? [])),
                backend: backend
            )
        case .cannotCreatePartitionInPartition, .cannotMoveAcrossPartitions:
            return StorageError.unsupportedOperation(
                "Native FoundationDB partitions are not used by StorageKit: \(error)",
                operation: operation,
                backend: backend
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
