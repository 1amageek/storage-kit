#if arch(wasm32)
@_extern(wasm, module: "storagekit_host", name: "dispatch")
func storagekit_host_dispatch(_ pointer: UInt32, _ length: UInt32) -> UInt32
#else
func storagekit_host_dispatch(_ pointer: UInt32, _ length: UInt32) -> UInt32 {
    0
}
#endif
