import Foundation

/// Storage backend identifier used for diagnostics and error classification.
public enum StorageBackend: String, Sendable, Hashable, Codable {
    case foundationDB = "foundationdb"
    case sqlite
    case postgreSQL = "postgresql"
    case inMemory = "in_memory"
    case unknown
}

/// Storage operation identifier used for diagnostics and error classification.
public enum StorageOperation: String, Sendable, Hashable, Codable {
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
public struct StorageError: Error, Sendable, LocalizedError, CustomStringConvertible, Hashable {
    public enum Code: String, Sendable, Hashable, Codable {
        case transactionConflict = "transaction_conflict"
        case transactionTooOld = "transaction_too_old"
        case transactionBusy = "transaction_busy"
        case keyNotFound = "key_not_found"
        case invalidOperation = "invalid_operation"
        case backendFailure = "backend_failure"
        case dataCorruption = "data_corruption"
        case resourceUnavailable = "resource_unavailable"
    }

    public let code: Code
    public let operation: StorageOperation
    public let backend: StorageBackend
    public let message: String
    public let underlyingDescription: String?

    public init(
        code: Code,
        operation: StorageOperation = .unknown,
        backend: StorageBackend = .unknown,
        message: String,
        underlyingDescription: String? = nil
    ) {
        self.code = code
        self.operation = operation
        self.backend = backend
        self.message = message
        self.underlyingDescription = underlyingDescription
    }

    public var isRetryable: Bool {
        switch code {
        case .transactionConflict, .transactionTooOld, .transactionBusy:
            return true
        case .keyNotFound, .invalidOperation, .backendFailure, .dataCorruption, .resourceUnavailable:
            return false
        }
    }

    public var errorDescription: String? {
        description
    }

    public var failureReason: String? {
        underlyingDescription
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
        return parts.joined(separator: ", ")
    }
}

// MARK: - Compatibility factories

extension StorageError {
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

    public static func backendError(_ message: String) -> StorageError {
        StorageError(
            code: .backendFailure,
            operation: .unknown,
            message: message
        )
    }
}
