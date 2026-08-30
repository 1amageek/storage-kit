/// Storage backend identifier used for diagnostics and error classification.
public enum StorageBackend: String, Sendable, Hashable {
    case foundationDB = "foundationdb"
    case sqlite
    case postgreSQL = "postgresql"
    case cloudflareDurableObject = "cloudflare_durable_object"
    case inMemory = "in_memory"
    case unknown
}

/// Storage operation identifier used for diagnostics and error classification.
public enum StorageOperation: String, Sendable, Hashable {
    case open
    case initialize
    case beginTransaction = "begin_transaction"
    case commit
    case rollback
    case cancel
    case read
    case rangeRead = "range_read"
    case write
    case delete
    case deleteRange = "delete_range"
    case prepare
    case execute
    case close
    case unknown
}

/// Structured error type for StorageEngine implementations.
public struct StorageError: Error, Sendable, CustomStringConvertible, Hashable {
    public enum Code: String, Sendable, Hashable {
        case transactionConflict = "transaction_conflict"
        case transactionTooOld = "transaction_too_old"
        case transactionFutureVersion = "transaction_future_version"
        case transactionTimedOut = "transaction_timed_out"
        case transactionCancelled = "transaction_cancelled"
        case transactionBusy = "transaction_busy"
        case transactionTooLarge = "transaction_too_large"
        case keyTooLarge = "key_too_large"
        case valueTooLarge = "value_too_large"
        /// The connection to the storage backend failed before the transaction
        /// reached its commit point. Retrying the whole transaction is safe.
        case connectionFailure = "connection_failure"
        /// The connection failed while a commit was in flight, so the outcome
        /// is unknown. A higher-level idempotency protocol must resolve the
        /// outcome before replaying the transaction.
        case commitUnknownResult = "commit_unknown_result"
        case keyNotFound = "key_not_found"
        case invalidOperation = "invalid_operation"
        case unsupportedOperation = "unsupported_operation"
        case backendFailure = "backend_failure"
        case backendContractViolation = "backend_contract_violation"
        case dataCorruption = "data_corruption"
        case resourceUnavailable = "resource_unavailable"
        /// The storage root holds data that no Directory catalog wrote.
        case incompatibleStorageLayout = "incompatible_storage_layout"
        /// A stated layer tag does not match the stored node tag.
        case directoryLayerMismatch = "directory_layer_mismatch"
        /// A move would cross a Partition boundary.
        case partitionBoundaryViolation = "partition_boundary_violation"
        /// An active Partition lease covers the affected subtree.
        case directoryLeased = "directory_leased"
        /// A transaction, Directory, or Partition belongs to another engine.
        case storageDomainMismatch = "storage_domain_mismatch"
        /// A lease, cursor, or binding no longer matches the catalog.
        case staleLease = "stale_lease"
        /// A Directory path, name, or layer tag violates the limits.
        case invalidDirectoryAddress = "invalid_directory_address"
    }

    public let code: Code
    public let operation: StorageOperation
    public let backend: StorageBackend
    public let message: String
    public let backendCode: Int32?
    public let underlyingDescription: String?
    public let byteLimitViolation: StorageByteLimitViolation?

    public init(
        code: Code,
        operation: StorageOperation = .unknown,
        backend: StorageBackend = .unknown,
        message: String,
        backendCode: Int32? = nil,
        underlyingDescription: String? = nil,
        byteLimitViolation: StorageByteLimitViolation? = nil
    ) {
        self.code = code
        self.operation = operation
        self.backend = backend
        self.message = message
        self.backendCode = backendCode
        self.underlyingDescription = underlyingDescription
        self.byteLimitViolation = byteLimitViolation
    }

    public var retryDisposition: StorageRetryDisposition {
        switch code {
        case .transactionConflict, .transactionTooOld, .transactionFutureVersion,
             .transactionTimedOut,
             .transactionBusy,
             .connectionFailure:
            return .safe
        case .commitUnknownResult:
            return .requiresIdempotency
        case .transactionCancelled, .transactionTooLarge, .keyTooLarge,
             .valueTooLarge, .keyNotFound, .invalidOperation,
             .unsupportedOperation, .backendFailure, .backendContractViolation,
             .dataCorruption,
             .resourceUnavailable,
             .incompatibleStorageLayout, .directoryLayerMismatch,
             .partitionBoundaryViolation, .directoryLeased,
             .storageDomainMismatch, .staleLease, .invalidDirectoryAddress:
            return .never
        }
    }

    /// Whether a generic transaction runner may replay the whole transaction.
    public var isRetryable: Bool { retryDisposition == .safe }

    /// Whether this error reports a caller-owned bounded point-read limit.
    ///
    /// The backend completed the read successfully; the value was rejected at
    /// the API boundary because returning it would exceed the caller's bound.
    /// This error must not poison the owning transaction.
    public var isPointReadValueTooLarge: Bool {
        code == .valueTooLarge
            && operation == .read
            && byteLimitViolation?.resource == .value
            && byteLimitViolation?.measurement == .exact
    }

    public var description: String {
        var parts = [
            "StorageError(\(code.rawValue))",
            "backend=\(backend.rawValue)",
            "operation=\(operation.rawValue)",
            "message=\(message)"
        ]
        if let underlyingDescription {
            parts.append("underlying=\(underlyingDescription)")
        }
        if let backendCode {
            parts.append("backendCode=\(backendCode)")
        }
        if let byteLimitViolation {
            parts.append("resource=\(byteLimitViolation.resource.rawValue)")
            parts.append("observedByteCount=\(byteLimitViolation.observedByteCount)")
            parts.append("maximumByteCount=\(byteLimitViolation.maximumByteCount)")
            parts.append("measurement=\(byteLimitViolation.measurement.rawValue)")
        }
        return parts.joined(separator: ", ")
    }
}

// MARK: - Convenience factories

extension StorageError {
    /// Reports an invalid upper bound supplied to a bounded point read.
    public static func invalidPointReadMaximum(
        _ maximumByteCount: Int,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        return StorageError(
            code: .invalidOperation,
            operation: .read,
            backend: backend,
            message: "Point-read maximumByteCount must be non-negative",
            underlyingDescription: "maximumByteCount=\(maximumByteCount)"
        )
    }

    /// Reports a value that cannot cross a bounded point-read boundary.
    public static func pointReadValueTooLarge(
        observedByteCount: Int,
        maximumByteCount: Int,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        precondition(observedByteCount >= 0)
        precondition(maximumByteCount >= 0)
        return StorageError(
            code: .valueTooLarge,
            operation: .read,
            backend: backend,
            message: "Point-read value exceeds maximumByteCount",
            byteLimitViolation: StorageByteLimitViolation(
                resource: .value,
                observedByteCount: UInt64(observedByteCount),
                maximumByteCount: UInt64(maximumByteCount),
                measurement: .exact
            )
        )
    }

    public static var transactionConflict: StorageError {
        StorageError(
            code: .transactionConflict,
            operation: .commit,
            message: "Transaction conflict"
        )
    }

    public static var transactionTooOld: StorageError {
        StorageError(
            code: .transactionTooOld,
            operation: .read,
            message: "Transaction read version is too old"
        )
    }

    public static var transactionTimedOut: StorageError {
        StorageError(
            code: .transactionTimedOut,
            operation: .execute,
            message: "Transaction timed out"
        )
    }

    public static var transactionBusy: StorageError {
        StorageError(
            code: .transactionBusy,
            operation: .beginTransaction,
            message: "Storage backend is busy"
        )
    }

    public static var keyNotFound: StorageError {
        StorageError(
            code: .keyNotFound,
            operation: .read,
            message: "Key not found"
        )
    }

    public static func invalidOperation(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .unknown,
            message: message
        )
    }

    public static func unsupportedOperation(
        _ message: String,
        operation: StorageOperation = .execute,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .unsupportedOperation,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func backendError(_ message: String) -> StorageError {
        StorageError(
            code: .backendFailure,
            operation: .unknown,
            message: message
        )
    }
}
