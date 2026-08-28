import DatabaseTypes

/// Key and selector admission rules for one leased Partition (invariant L-8).
///
/// Every key must carry the Partition root prefix. A range end may equal
/// `strinc(prefix)` only through `firstGreaterOrEqual`, and selector forms
/// that can resolve outside the prefix are rejected before any I/O.
package struct PartitionKeyBounds: Sendable {
    package let prefix: ByteString
    package let end: ByteString
    package let backend: StorageBackend

    package init(partition: Partition, backend: StorageBackend) throws {
        let prefix = partition.root.root.prefix
        do {
            self.end = try strinc(prefix)
        } catch {
            throw StorageError(
                code: .invalidOperation,
                operation: .open,
                backend: backend,
                message: "Partition root prefix cannot bound a key range: \(error)"
            )
        }
        self.prefix = prefix
        self.backend = backend
    }

    package func contains(_ key: ByteString) -> Bool {
        key.starts(with: prefix)
    }

    package func requireKey(
        _ key: ByteString,
        operation: StorageOperation
    ) throws {
        guard contains(key) else {
            throw outside("Key lies outside the leased Partition", operation: operation)
        }
    }

    /// Begin selectors must be `firstGreaterOrEqual` or `firstGreaterThan`
    /// on a key inside the prefix.
    package func requireBeginSelector(
        _ selector: KeySelector,
        operation: StorageOperation
    ) throws {
        guard selector.offset == 1, contains(selector.key) else {
            throw outside(
                "Range begin selector can resolve outside the leased Partition",
                operation: operation
            )
        }
    }

    /// End selectors must be `firstGreaterOrEqual` on a key inside the prefix
    /// or equal to `strinc(prefix)`, or `lastLessOrEqual` / `lastLessThan`
    /// on a key inside the prefix.
    package func requireEndSelector(
        _ selector: KeySelector,
        operation: StorageOperation
    ) throws {
        let admitted: Bool
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            admitted = contains(selector.key) || selector.key == end
        case (true, 0), (false, 0):
            admitted = contains(selector.key)
        default:
            admitted = false
        }
        guard admitted else {
            throw outside(
                "Range end selector can resolve outside the leased Partition",
                operation: operation
            )
        }
    }

    /// Plain key ranges must begin inside the prefix and end inside it or at
    /// `strinc(prefix)`.
    package func requireRange(
        begin: ByteString,
        end rangeEnd: ByteString,
        operation: StorageOperation
    ) throws {
        guard contains(begin), contains(rangeEnd) || rangeEnd == end else {
            throw outside("Key range extends outside the leased Partition", operation: operation)
        }
    }

    private func outside(_ message: String, operation: StorageOperation) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: backend,
            message: message
        )
    }
}
