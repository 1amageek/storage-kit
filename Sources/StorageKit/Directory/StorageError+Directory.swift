extension StorageError {
    public static func incompatibleStorageLayout(
        _ reason: String,
        operation: StorageOperation = .open,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .incompatibleStorageLayout,
            operation: operation,
            backend: backend,
            message: "Incompatible storage layout: \(reason)"
        )
    }

    public static func directoryLayerMismatch(
        _ message: String,
        operation: StorageOperation = .open,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .directoryLayerMismatch,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func partitionBoundaryViolation(
        _ message: String,
        operation: StorageOperation = .write,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .partitionBoundaryViolation,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func directoryLeased(
        _ message: String,
        operation: StorageOperation = .delete,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .directoryLeased,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func storageDomainMismatch(
        _ message: String,
        operation: StorageOperation = .open,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .storageDomainMismatch,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func staleLease(
        _ message: String,
        operation: StorageOperation = .open,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .staleLease,
            operation: operation,
            backend: backend,
            message: message
        )
    }

    public static func invalidDirectoryAddress(
        _ error: DirectoryAddressError,
        operation: StorageOperation = .open,
        backend: StorageBackend = .unknown
    ) -> StorageError {
        StorageError(
            code: .invalidDirectoryAddress,
            operation: operation,
            backend: backend,
            message: error.description
        )
    }
}
