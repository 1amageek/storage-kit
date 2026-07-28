import DatabaseTypes

/// Materializes one host-owned response directly into its final Swift storage.
enum StorageHostResponse {
    static func receive(
        byteCount: Int,
        maximumResponseBytes: Int,
        discard: () -> Void,
        copyInto: (UnsafeMutableRawBufferPointer) -> Void
    ) throws(StorageHostTransportError) -> ByteString {
        guard byteCount > 0 else {
            throw .hostReturnedNoResponse
        }
        guard byteCount <= maximumResponseBytes else {
            discard()
            throw .responseTooLarge(
                actual: byteCount,
                maximum: maximumResponseBytes
            )
        }

        // JavaScript and WebAssembly cannot share ownership of the host's
        // response allocation. This is the single required copy, written
        // directly into the final ByteString storage after dispatch returns.
        return ByteString.copying(count: byteCount, copyInto)
    }
}
