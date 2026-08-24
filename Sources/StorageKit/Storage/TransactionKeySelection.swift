import DatabaseTypes

/// Resolves one key selector through the transaction's streaming cursor.
public enum TransactionKeySelection {
    public static func resolve(
        _ selector: KeySelector,
        in transaction: any TransactionReadAccess,
        snapshot: Bool
    ) async throws -> ByteString? {
        let (nextOffset, overflow) = selector.offset.addingReportingOverflow(1)
        guard !overflow else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "KeySelector offset cannot be advanced"
            )
        }
        var cursor = transaction.rangeCursor(
            from: selector,
            to: KeySelector(
                key: selector.key,
                orEqual: selector.orEqual,
                offset: nextOffset
            ),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .exact
        )
        let key: ByteString?
        do {
            key = try await cursor.next()?.0
        } catch let cleanupError as StorageRangeTerminalCleanupError {
            throw cleanupError
        } catch let cleanupError as StorageRangeCleanupError {
            throw cleanupError
        } catch {
            let selectionError = error
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: selectionError,
                    cleanupError: error
                )
            }
            throw selectionError
        }
        do {
            try await cursor.finish()
        } catch {
            throw StorageRangeTerminalCleanupError(cleanupError: error)
        }
        return key
    }
}
