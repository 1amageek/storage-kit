import StorageKitEmbeddedCore

/// Retains a StorageKit byte owner while lending the same storage to the
/// Embedded core without materializing another byte collection.
struct RetainedStorageBytesOwner: EmbeddedByteOwner {
    let owner: any BytesOwner

    var count: Int {
        owner.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try owner.borrowBytes(body)
    }
}
