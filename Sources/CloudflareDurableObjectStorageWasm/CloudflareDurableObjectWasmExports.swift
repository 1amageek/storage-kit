@_expose(wasm, "storagekit_alloc")
@_cdecl("storagekit_alloc")
public func storagekit_alloc(_ byteCount: UInt32) -> UInt32 {
    CloudflareDurableObjectWasmMemory.allocate(byteCount: byteCount)
}

@_expose(wasm, "storagekit_dealloc")
@_cdecl("storagekit_dealloc")
public func storagekit_dealloc(_ pointer: UInt32, _ byteCount: UInt32) {
    CloudflareDurableObjectWasmMemory.deallocate(pointer: pointer, byteCount: byteCount)
}

@_expose(wasm, "storagekit_dispatch")
@_cdecl("storagekit_dispatch")
public func storagekit_dispatch(_ pointer: UInt32, _ length: UInt32) -> UInt32 {
    CloudflareDurableObjectWasmRequestDispatcher.dispatch(pointer: pointer, length: length)
}

@_expose(wasm, "storagekit_apply_mutation")
@_cdecl("storagekit_apply_mutation")
public func storagekit_apply_mutation(
    _ hasExisting: UInt32,
    _ existingPointer: UInt32,
    _ existingLength: UInt32,
    _ paramPointer: UInt32,
    _ paramLength: UInt32,
    _ mutationTypeRaw: UInt32
) -> UInt32 {
    CloudflareDurableObjectWasmMutationApplier.apply(
        hasExisting: hasExisting,
        existingPointer: existingPointer,
        existingLength: existingLength,
        paramPointer: paramPointer,
        paramLength: paramLength,
        mutationTypeRaw: mutationTypeRaw
    )
}
