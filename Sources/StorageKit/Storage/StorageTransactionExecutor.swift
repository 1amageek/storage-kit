/// Executes typed operations through a dynamically selected storage engine.
///
/// `StorageEngine.executeTransaction` deliberately has no generic result so it
/// remains an existential-callable Embedded boundary. This concrete executor
/// restores a typed result above that boundary. The transaction and every
/// `ByteString` payload stay borrowed or owned by their existing storage value;
/// only the result value itself is published through an isolated completion slot.
public struct StorageTransactionExecutor: Sendable {
    private let engine: any StorageEngine

    public init(engine: any StorageEngine) {
        self.engine = engine
    }

    /// Creates a transaction for a higher-level lifecycle coordinator.
    public func createOwnedTransaction() throws -> any Transaction {
        try engine.createOwnedTransaction()
    }

    public func withTransaction<Result: Sendable & Copyable>(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Result
    ) async throws -> Result {
        let publication = StorageTransactionResultPublication<Result>()

        try await engine.executeTransaction { transaction in
            let result = try await operation(transaction)
            await publication.publish(result)
        }

        return await publication.result()
    }
}

/// Publishes one transaction result across the existential storage boundary.
///
/// The actor is a copyable reference captured by the escaping transaction
/// operation. This avoids capturing `Mutex`, whose noncopyable ownership is not
/// supported by Embedded Swift in an escaping generic closure. The stored result
/// retains its existing backing storage; publishing it does not materialize
/// database payload bytes.
private actor StorageTransactionResultPublication<Result: Sendable & Copyable> {
    private var publishedResult: Result?

    func publish(_ result: Result) {
        precondition(
            publishedResult == nil,
            "A storage transaction must publish exactly one operation result"
        )
        publishedResult = result
    }

    func result() -> Result {
        guard let publishedResult else {
            preconditionFailure(
                "A successful storage transaction must publish its operation result"
            )
        }
        return publishedResult
    }
}
