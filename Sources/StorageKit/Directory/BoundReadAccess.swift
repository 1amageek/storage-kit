import DatabaseTypes

/// Read access bound to one leased Partition for the duration of a single
/// `PartitionLease.withReadAccess` closure.
///
/// Every operation validates the lease and the binding scope first, then the
/// key containment rules of `PartitionKeyBounds`, and only then touches the
/// transaction. Cursors carry the same validation into every advance.
public struct BoundReadAccess: ~Copyable, Sendable {
    public let partition: Partition
    let transaction: any TransactionReadAccess
    let bounds: PartitionKeyBounds
    let registration: LeaseRegistration
    let scope: PartitionBindingScope

    init(
        partition: Partition,
        transaction: any TransactionReadAccess,
        bounds: PartitionKeyBounds,
        registration: LeaseRegistration,
        scope: PartitionBindingScope
    ) {
        self.partition = partition
        self.transaction = transaction
        self.bounds = bounds
        self.registration = registration
        self.scope = scope
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try requireLive(operation: .read)
        try bounds.requireKey(key, operation: .read)
        return try await transaction.getValue(for: key, snapshot: snapshot)
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool = false,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        try requireLive(operation: .read)
        try bounds.requireKey(key, operation: .read)
        return try await transaction.getValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    /// Resolves `selector` and returns the key only when it lies inside the
    /// Partition; keys outside resolve to `nil`.
    public func getKey(
        selector: KeySelector,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try requireLive(operation: .read)
        guard let key = try await transaction.getKey(selector: selector, snapshot: snapshot)
        else {
            return nil
        }
        return bounds.contains(key) ? key : nil
    }

    /// Opens a cursor over a range that cannot resolve outside the Partition.
    ///
    /// The binding scope takes ownership of the returned cursor, so closing the
    /// binding completes that cursor's backend cleanup even when the caller
    /// kept it. The caller may still finish it earlier.
    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .iterator
    ) throws -> KeyValueCursor {
        try requireLive(operation: .rangeRead)
        try bounds.requireBeginSelector(begin, operation: .rangeRead)
        try bounds.requireEndSelector(end, operation: .rangeRead)
        let registration = self.registration
        let scope = self.scope
        let backend = bounds.backend
        let cursor = transaction.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        ).validatingScope {
            try registration.requireActive(operation: .rangeRead, backend: backend)
            try scope.requireOpen(operation: .rangeRead, backend: backend)
        }
        try scope.adopt(cursor, operation: .rangeRead, backend: backend)
        return cursor
    }

    func requireLive(operation: StorageOperation) throws {
        try registration.requireActive(operation: operation, backend: bounds.backend)
        try scope.requireOpen(operation: operation, backend: bounds.backend)
    }
}
