/// Owns immutable contiguous bytes and exposes them only through a scoped borrow.
///
/// Implementations must keep the same bytes alive for their entire lifetime.
/// The pointer passed to `body` must remain valid only for that invocation and
/// must never escape it.
///
/// Borrows of the same owner may overlap or nest when two views are compared.
/// Implementations must therefore permit concurrent and reentrant borrows and
/// must not hold a non-recursive lock while invoking `body`.
public protocol EmbeddedByteOwner: Sendable {
    var count: Int { get }

    /// Lends the immutable storage for exactly one synchronous callback.
    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows
}
