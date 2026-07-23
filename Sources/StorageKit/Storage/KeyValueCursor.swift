/// Type-erased, zero-copy cursor used at a `Transaction` existential boundary.
///
/// The actor owns the backend iterator so `next()` calls remain ordered across
/// suspension points. Key and value buffers are returned unchanged; this type
/// erases only control flow and never materializes payload bytes.
public struct KeyValueCursor: Sendable {
    public typealias Element = (Bytes, Bytes)

    private let state: any KeyValueCursorState

    fileprivate init<Sequence: TransactionRangeResult>(
        sequence: sending Sequence
    ) {
        self.state = TypedKeyValueCursorState(sequence: sequence)
    }

    /// Advances this single-consumer cursor.
    public mutating func next() async throws -> Element? {
        try await state.next()
    }

    /// Closes the backend iterator and awaits all native cleanup.
    public mutating func finish() async throws {
        try await state.finish()
    }
}

extension Transaction {
    /// Opens a streaming cursor while preserving the backend's native buffers.
    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .wantAll
    ) -> KeyValueCursor {
        let sequence = getRange(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
        return KeyValueCursor(sequence: sequence)
    }
}

private protocol KeyValueCursorState: Actor {
    func next() async throws -> KeyValueCursor.Element?
    func finish() async throws
}

private actor TypedKeyValueCursorState<Sequence: TransactionRangeResult>:
    KeyValueCursorState {
    private var sequence: Sequence?
    private var iterator: Sequence.AsyncIterator?

    init(sequence: sending Sequence) {
        self.sequence = sequence
    }

    func next() async throws -> KeyValueCursor.Element? {
        if iterator == nil, let sequence {
            iterator = sequence.makeAsyncIterator()
            self.sequence = nil
        }
        guard var activeIterator = iterator else {
            return nil
        }
        iterator = nil
        do {
            let element = try await activeIterator.next(isolation: self)
            if let element {
                iterator = activeIterator
                return element
            }
            try await activeIterator.finish(isolation: self)
            return nil
        } catch {
            let iterationError = error
            do {
                try await activeIterator.finish(isolation: self)
            } catch {
                sequence = nil
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            iterator = nil
            sequence = nil
            throw iterationError
        }
    }

    func finish() async throws {
        sequence = nil
        guard var activeIterator = iterator else {
            return
        }
        iterator = nil
        try await activeIterator.finish(isolation: self)
    }
}
