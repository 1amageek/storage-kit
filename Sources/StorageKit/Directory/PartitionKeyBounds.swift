import DatabaseTypes

/// Key and selector admission rules for one leased Partition (invariant L-8).
///
/// The bound region is the Partition's contiguous content range
/// `[keyspacePrefix, keyspacePrefix + FE)`. It covers the Partition's own data
/// root and every descendant Directory, Partition, and Subspace, because every
/// allocated content prefix below the Partition starts with a Tuple type code
/// or the reserved data byte, both below `FE`. It excludes the Partition's own
/// nested Directory Layer node subspace at `keyspacePrefix + FE`, which the
/// Directory catalog owns: a leaseholder that cleared its whole region would
/// otherwise destroy the metadata of the node its lease is bound to.
///
/// A range end may equal the region end through `firstGreaterOrEqual`, and
/// selector forms that can resolve outside the region are rejected before any
/// I/O.
package struct PartitionKeyBounds: Sendable {
    package let prefix: ByteString
    package let end: ByteString
    package let backend: StorageBackend

    package init(partition: Partition, backend: StorageBackend) {
        self.prefix = partition.keyspacePrefix
        self.end = partition.keyspacePrefix.appending(Directory.nodeSubspaceByte)
        self.backend = backend
    }

    /// A key is inside when it lies in `[prefix, end)`. Every such key starts
    /// with `prefix`, because a key at or above `prefix` that does not start
    /// with it sorts above `strinc(prefix)`, which sorts above `end`.
    package func contains(_ key: ByteString) -> Bool {
        key >= prefix && key < end
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
    /// on a key inside the region.
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

    /// End selectors must be `firstGreaterOrEqual` on a key inside the region
    /// or equal to the region end, or `lastLessOrEqual` / `lastLessThan` on a
    /// key inside the region.
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

    /// Plain key ranges must begin inside the region and end inside it or at
    /// the region end.
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
