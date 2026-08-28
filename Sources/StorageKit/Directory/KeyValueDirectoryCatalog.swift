import DatabaseTypes

/// Transactional Directory catalog for key-value backends (layout V1).
///
/// One instance is bound to one engine domain and is that engine's sole
/// existence authority. All catalog reads and writes go through the caller's
/// transaction, so creation, move, and removal are atomic with it. The key
/// layout is fixed by `DESIGN.md`; changing it is a layout-version change.
public final class KeyValueDirectoryCatalog: DirectoryAccess, Sendable {
    public let transactionDomain: StorageTransactionDomain
    public let backend: StorageBackend

    public init(
        transactionDomain: StorageTransactionDomain,
        backend: StorageBackend
    ) {
        self.transactionDomain = transactionDomain
        self.backend = backend
    }

    // MARK: - Layout V1

    package enum Layout {
        package static let allocatorKey: ByteString = [0xFE, 0x61]
        package static let nodePrefix: ByteString = [0xFE, 0x6E]
        package static let rootNumber: Int64 = 0
        package static let firstAllocatedRootNumber: Int64 = 1

        package enum Kind: Int64 {
            case directory = 0
            case partition = 1
        }

        package static func rootPrefix(_ number: Int64) -> ByteString {
            Tuple(number).pack()
        }

        package static func nodeKey(
            parentPrefix: ByteString,
            kind: Kind,
            name: any TupleElement
        ) -> ByteString {
            concatenate(nodePrefix, Tuple(parentPrefix, kind.rawValue, name).pack())
        }

        package static func listPrefix(
            parentPrefix: ByteString,
            kind: Kind
        ) -> ByteString {
            concatenate(nodePrefix, Tuple(parentPrefix, kind.rawValue).pack())
        }

        package static func subtreePrefix(rootPrefix: ByteString) -> ByteString {
            concatenate(nodePrefix, Tuple(rootPrefix).pack())
        }

        // Catalog keys are built once per Directory operation, never on a
        // repeated data path, so a materializing concatenation is acceptable.
        private static func concatenate(
            _ head: ByteString,
            _ tail: ByteString
        ) -> ByteString {
            var bytes: [UInt8] = []
            bytes.reserveCapacity(head.count + tail.count)
            bytes.append(contentsOf: head)
            bytes.append(contentsOf: tail)
            return ByteString(bytes)
        }
    }

    private var rootDirectory: Directory {
        Directory(
            domain: transactionDomain,
            address: .root,
            root: Subspace(prefix: Layout.rootPrefix(Layout.rootNumber))
        )
    }

    // MARK: - Root

    public func openRoot(
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try requireDomain(of: transaction, operation: .open)
        switch try await StorageLayoutMarker.inspect(transaction: transaction) {
        case .openV1:
            return rootDirectory
        case .uninitialized:
            return nil
        case .rejected(let rejection):
            throw StorageError.incompatibleStorageLayout(rejection, backend: backend)
        }
    }

    public func openOrInitializeRoot(
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, operation: .initialize)
        switch try await StorageLayoutMarker.inspect(transaction: transaction) {
        case .openV1:
            return rootDirectory
        case .uninitialized:
            try transaction.setValue(StorageLayoutMarker.v1, for: StorageLayoutMarker.key)
            try transaction.setValue(
                Tuple(Layout.firstAllocatedRootNumber).pack(),
                for: Layout.allocatorKey
            )
            return rootDirectory
        case .rejected(let rejection):
            throw StorageError.incompatibleStorageLayout(rejection, backend: backend)
        }
    }

    // MARK: - Operations 1–4

    public func openDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        try requireDomain(of: transaction, parent: parent, operation: .open)
        let address = try childAddress(of: parent, .directory(name), operation: .open)
        guard let number = try await readNode(
            parentPrefix: parent.root.prefix,
            kind: .directory,
            name: name,
            transaction: transaction
        ) else {
            return nil
        }
        return directory(at: address, number: number)
    }

    public func openOrCreateDirectory(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, parent: parent, operation: .write)
        let address = try childAddress(of: parent, .directory(name), operation: .write)
        if let number = try await readNode(
            parentPrefix: parent.root.prefix,
            kind: .directory,
            name: name,
            transaction: transaction
        ) {
            return directory(at: address, number: number)
        }
        let number = try await allocateRootNumber(transaction: transaction)
        try transaction.setValue(
            Tuple(number).pack(),
            for: Layout.nodeKey(parentPrefix: parent.root.prefix, kind: .directory, name: name)
        )
        return directory(at: address, number: number)
    }

    public func openPartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Partition? {
        try requireDomain(of: transaction, parent: parent, operation: .open)
        let address = try childAddress(of: parent, .partition(id), operation: .open)
        guard let number = try await readNode(
            parentPrefix: parent.root.prefix,
            kind: .partition,
            name: id.bytes,
            transaction: transaction
        ) else {
            return nil
        }
        return Partition(id: id, root: directory(at: address, number: number))
    }

    public func openOrCreatePartition(
        _ id: PartitionID,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Partition {
        try requireDomain(of: transaction, parent: parent, operation: .write)
        let address = try childAddress(of: parent, .partition(id), operation: .write)
        if let number = try await readNode(
            parentPrefix: parent.root.prefix,
            kind: .partition,
            name: id.bytes,
            transaction: transaction
        ) {
            return Partition(id: id, root: directory(at: address, number: number))
        }
        let number = try await allocateRootNumber(transaction: transaction)
        try transaction.setValue(
            Tuple(number).pack(),
            for: Layout.nodeKey(parentPrefix: parent.root.prefix, kind: .partition, name: id.bytes)
        )
        return Partition(id: id, root: directory(at: address, number: number))
    }

    // MARK: - Operations 5–6

    public func listDirectories(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [String] {
        try requireDomain(of: transaction, parent: parent, operation: .rangeRead)
        try requireListLimit(limit)
        if let after {
            try validate(.directory(after), operation: .rangeRead)
        }
        let listPrefix = Layout.listPrefix(parentPrefix: parent.root.prefix, kind: .directory)
        let rows = try await listNodes(
            parentPrefix: parent.root.prefix,
            kind: .directory,
            listPrefix: listPrefix,
            after: after,
            limit: limit,
            transaction: transaction
        )
        return try rows.map { row in
            try decodeName(from: row.0, listPrefix: listPrefix, as: String.self)
        }
    }

    public func listPartitions(
        in parent: Directory,
        after: PartitionID?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [PartitionID] {
        try requireDomain(of: transaction, parent: parent, operation: .rangeRead)
        try requireListLimit(limit)
        let listPrefix = Layout.listPrefix(parentPrefix: parent.root.prefix, kind: .partition)
        let rows = try await listNodes(
            parentPrefix: parent.root.prefix,
            kind: .partition,
            listPrefix: listPrefix,
            after: after?.bytes,
            limit: limit,
            transaction: transaction
        )
        return try rows.map { row in
            let bytes = try decodeName(from: row.0, listPrefix: listPrefix, as: ByteString.self)
            do {
                return try PartitionID(bytes)
            } catch {
                throw dataCorruption("Partition node name is invalid: \(error)")
            }
        }
    }

    // MARK: - Operation 7

    public func moveChild(
        _ child: StorageAddressStep,
        in source: Directory,
        to newChild: StorageAddressStep,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        try requireDomain(of: transaction, parent: source, operation: .write)
        guard destination.domain === transactionDomain else {
            throw domainMismatch(operation: .write, subject: "Destination Directory")
        }
        guard case .directory(let name) = child,
              case .directory(let newName) = newChild
        else {
            throw StorageError.unsupportedOperation(
                "Partitions cannot be moved; only Directories support operation 7",
                operation: .write,
                backend: backend
            )
        }
        let movedAddress = try childAddress(of: source, child, operation: .write)
        let targetAddress = try childAddress(of: destination, newChild, operation: .write)
        guard let number = try await readNode(
            parentPrefix: source.root.prefix,
            kind: .directory,
            name: name,
            transaction: transaction
        ) else {
            throw notFound("Directory '\(name)' does not exist in the source Directory", operation: .write)
        }
        let movedPrefix = Layout.rootPrefix(number)
        if movedAddress.isAncestorOrSelf(of: destination.address)
            || destination.root.prefix == movedPrefix {
            throw StorageError.invalidDirectoryAddress(
                .targetInsideMovedSubtree,
                operation: .write,
                backend: backend
            )
        }
        if try await readNode(
            parentPrefix: destination.root.prefix,
            kind: .directory,
            name: newName,
            transaction: transaction
        ) != nil {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: backend,
                message: "Directory '\(newName)' already exists in the destination Directory"
            )
        }
        try transactionDomain.leases.registerIntent(
            covering: movedAddress,
            transaction: transaction,
            operation: .write,
            backend: backend
        )
        try transaction.clear(
            key: Layout.nodeKey(parentPrefix: source.root.prefix, kind: .directory, name: name)
        )
        try transaction.setValue(
            Tuple(number).pack(),
            for: Layout.nodeKey(parentPrefix: destination.root.prefix, kind: .directory, name: newName)
        )
        return directory(at: targetAddress, number: number)
    }

    // MARK: - Operation 8

    public func removeChild(
        _ child: StorageAddressStep,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        try requireDomain(of: transaction, parent: parent, operation: .delete)
        let address = try childAddress(of: parent, child, operation: .delete)
        let kind: Layout.Kind
        let name: any TupleElement
        let label: String
        switch child {
        case .directory(let directoryName):
            kind = .directory
            name = directoryName
            label = "Directory '\(directoryName)'"
        case .partition(let id):
            kind = .partition
            name = id.bytes
            label = "Partition of \(id.bytes.count) identifier bytes"
        }
        guard let number = try await readNode(
            parentPrefix: parent.root.prefix,
            kind: kind,
            name: name,
            transaction: transaction
        ) else {
            throw notFound("\(label) does not exist in the parent Directory", operation: .delete)
        }
        let childPrefix = Layout.rootPrefix(number)
        let subtreePrefix = Layout.subtreePrefix(rootPrefix: childPrefix)
        let childNodes = try await transaction.collectRange(
            begin: subtreePrefix,
            end: try increment(subtreePrefix),
            limit: 1
        )
        guard childNodes.isEmpty else {
            throw StorageError.directoryNotEmpty(
                "\(label) still contains child Directories or Partitions",
                backend: backend
            )
        }
        let dataRange: (begin: ByteString, end: ByteString)
        do {
            dataRange = try Subspace(prefix: childPrefix).prefixRange()
        } catch {
            throw contractViolation("Directory root prefix cannot be bounded: \(error)")
        }
        let data = try await transaction.collectRange(
            begin: dataRange.begin,
            end: dataRange.end,
            limit: 1
        )
        guard data.isEmpty else {
            throw StorageError.directoryNotEmpty(
                "\(label) still contains data keys",
                backend: backend
            )
        }
        try transactionDomain.leases.registerIntent(
            covering: address,
            transaction: transaction,
            operation: .delete,
            backend: backend
        )
        try transaction.clear(
            key: Layout.nodeKey(parentPrefix: parent.root.prefix, kind: kind, name: name)
        )
    }

    // MARK: - Node access

    private func readNode(
        parentPrefix: ByteString,
        kind: Layout.Kind,
        name: any TupleElement,
        transaction: any TransactionReadAccess
    ) async throws -> Int64? {
        let key = Layout.nodeKey(parentPrefix: parentPrefix, kind: kind, name: name)
        guard let value = try await transaction.getValue(for: key) else {
            return nil
        }
        return try decodeRootNumber(value, context: "Directory node")
    }

    /// Read-modify-write on the allocator key inside the caller's transaction,
    /// so concurrent creators conflict through the backend's own detection.
    private func allocateRootNumber(
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        guard let raw = try await transaction.getValue(for: Layout.allocatorKey) else {
            throw dataCorruption("Directory allocator key is missing from an initialized catalog")
        }
        let next = try decodeRootNumber(raw, context: "Directory allocator")
        guard next >= Layout.firstAllocatedRootNumber, next < Int64.max else {
            throw dataCorruption("Directory allocator holds an invalid next root number \(next)")
        }
        try transaction.setValue(Tuple(next + 1).pack(), for: Layout.allocatorKey)
        return next
    }

    private func listNodes(
        parentPrefix: ByteString,
        kind: Layout.Kind,
        listPrefix: ByteString,
        after: (any TupleElement)?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [(ByteString, ByteString)] {
        let begin: KeySelector
        if let after {
            begin = .firstGreaterThan(
                Layout.nodeKey(parentPrefix: parentPrefix, kind: kind, name: after)
            )
        } else {
            begin = .firstGreaterOrEqual(listPrefix)
        }
        let end = KeySelector.firstGreaterOrEqual(try increment(listPrefix))
        return try await transaction.collectRange(from: begin, to: end, limit: limit)
    }

    private func decodeRootNumber(
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
            throw dataCorruption("\(context) value is not a single nonnegative root number")
        }
        return number
    }

    private func decodeName<Name>(
        from key: ByteString,
        listPrefix: ByteString,
        as _: Name.Type
    ) throws -> Name {
        guard key.count > listPrefix.count, key.starts(with: listPrefix) else {
            throw dataCorruption("Directory node key lies outside its listing prefix")
        }
        let suffix = key[listPrefix.count..<key.count]
        let elements: [any TupleElement]
        do {
            elements = try Tuple.unpack(from: suffix)
        } catch {
            throw dataCorruption("Directory node name is not a Tuple: \(error)")
        }
        guard elements.count == 1, let name = elements[0] as? Name else {
            throw dataCorruption("Directory node name has an unexpected Tuple shape")
        }
        return name
    }

    private func directory(at address: StorageAddress, number: Int64) -> Directory {
        Directory(
            domain: transactionDomain,
            address: address,
            root: Subspace(prefix: Layout.rootPrefix(number))
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
        _ step: StorageAddressStep,
        operation: StorageOperation
    ) throws {
        do {
            try step.validate()
        } catch {
            throw StorageError.invalidDirectoryAddress(error, operation: operation, backend: backend)
        }
    }

    private func childAddress(
        of parent: Directory,
        _ step: StorageAddressStep,
        operation: StorageOperation
    ) throws -> StorageAddress {
        do {
            return try parent.address.appending(step)
        } catch {
            throw StorageError.invalidDirectoryAddress(error, operation: operation, backend: backend)
        }
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
