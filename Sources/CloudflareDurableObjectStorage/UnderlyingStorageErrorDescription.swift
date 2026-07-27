import StorageKit

func underlyingStorageErrorDescription(
    _ error: any Error
) -> String? {
    if let storageError = error as? StorageError {
        return storageError.description
    }
    if let describedError = error as? any CustomStringConvertible {
        return describedError.description
    }
    return nil
}
