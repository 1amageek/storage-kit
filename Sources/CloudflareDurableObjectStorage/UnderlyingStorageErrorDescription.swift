import StorageKit

func underlyingStorageErrorDescription(
    _ error: any Error
) -> String? {
    if let storageError = error as? StorageError {
        return storageError.description
    }
    if let clockError = error as? StorageClockError {
        return clockError.storageFailureDescription
    }
    return nil
}
