/// Owns immutable contiguous bytes and exposes them only through a scoped borrow.
///
/// Borrows of the same owner may overlap or nest when two views are compared.
/// Implementations must therefore permit concurrent and reentrant borrows and
/// must not hold a non-recursive lock while invoking `body`.
public protocol BytesOwner: Sendable {
    var count: Int { get }

    /// Lends the immutable storage for exactly one synchronous callback.
    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows
}
