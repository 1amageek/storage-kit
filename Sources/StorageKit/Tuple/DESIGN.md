# Tuple Component

## Purpose and Scope

The Tuple component owns FoundationDB-compatible tuple values, canonical byte
ordering, encoding, decoding, subspaces, and tuple cursors. It is a child of
the [`StorageKit` module design](../DESIGN.md).

This sprint adds one public admission-aware packing operation without changing
the tuple format or existing `pack()` behavior.

## Responsibilities and Boundaries

The component owns the intrinsic relationship between a Tuple's elements and
its canonical packed bytes. It owns the exact measurement and materialization
order required to admit a packed result before allocation.

It does not own storage transactions, backend limits, database queries,
authorization, meters, or the caller's resource ledger. The callback reports
an exact byte claim to its caller; it does not grant or enforce a backend
capability.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit module`](../DESIGN.md) | parent | Module ownership and storage boundary | Composes Tuple with storage contracts | Tuple must remain independent of storage execution. |
| `DatabaseTypes.ByteString` | depends on | Owned byte storage and scoped byte access | Stores the final packed bytes | Do not expose a borrowed buffer beyond the packing call. |

## Architecture

```text
Tuple
  ├─ stored TupleElement values
  ├─ TupleEncodingSink (measuring or exact buffer destination)
  └─ pack(admitting:)
       ├─ measuring traversal
       ├─ exact admission callback
       └─ one owned allocation + encoding traversal
```

Every TupleElement writes to the same sink abstraction in both traversals, so
the measured byte count and final encoded bytes use the established format.

## Contracts and Invariants

### Canonical Version 1 encoding

The encoded bytes are the FoundationDB Tuple Layer bytes, not merely an
order-preserving encoding of the same values. An encoding that sorts correctly
but differs from the reference bytes is nonconforming: the same store is read
by other Tuple Layer implementations, and SPEC 8.5 states that every backend
consumes the encoded bytes unchanged.

For integers this fixes the following forms, where `n` is the number of
payload bytes:

| Value range | Type code | Payload |
|---|---|---|
| `0` | `0x14` | none |
| positive, `n` in 1…8 | `0x14 + n` | big-endian magnitude |
| positive, `n` in 9…255 | `0x1D` | one length byte `n`, then the big-endian magnitude |
| negative, `n` in 1…8 | `0x14 - n` | big-endian `(2^(8n) - 1) + value` |
| negative, `n` in 9…255 | `0x0B` | one length byte `n ^ 0xFF`, then the big-endian `(2^(8n) - 1) + value` |

`Int64.min` therefore encodes as `0C 7F FF FF FF FF FF FF FF`. The raw two's
complement pattern `0C 80 00 …` sorts identically and is not this encoding.
The 9-to-255-byte forms carry a length byte, so a decoder that treats `0x1D`
or `0x0B` as a fixed nine-byte payload misreads the value and leaves the
offset inside the following element.

A decoder rejects with `TupleError.integerOverflow` any well-formed encoding
whose value does not fit the requested Swift type. It never truncates a wider
payload into the requested width.

The golden-vector suite named by SPEC 8.5 holds these bytes as literals and is
the version identity of the encoding. A vector is evidence only when it agrees
with the FoundationDB reference implementation; a vector derived from the
encoder under test proves nothing.

### Admission-aware packing

The public contract is:

```swift
public func pack<Failure: Error>(
    admitting allocation: (Int) throws(Failure) -> Void
) throws(Failure) -> ByteString
```

Its required sequence is:

1. Traverse every element once with a measuring sink and obtain the exact
   canonical byte count.
2. Invoke `admitting` synchronously exactly once with that count. The closure
   is borrowed and never retained.
3. If admission throws, stop immediately. Do not allocate or encode a packed
   result, and propagate the same typed error unchanged.
4. On admission success, allocate one exact `ByteString` and traverse the
   elements once into that buffer.
5. Validate the final byte count and return the owned bytes.

For every stable TupleElement sequence, `pack(admitting:)` returns bytes equal
to the existing `pack()` result. No caller may substitute an upper bound based
on source representation.

## Runtime Flows

```text
elements
  -> measure once
  -> callback(exact byte count)
      ├─ throws -> no packed result, same error
      └─ returns -> allocate once -> encode once -> validate -> return
```

## State, Ownership, and Lifecycle

Tuple remains an immutable value. The measuring sink is stack-local. The
admission closure is borrowed for one synchronous invocation and is not stored.
The final `ByteString` owns exactly the canonical encoded byte range. No sink
or pointer escapes the method.

## Failure, Concurrency, and Constraints

The operation does not suspend and introduces no shared state. Callback
failures are not wrapped, translated, or swallowed. Existing TupleError and
encoder precondition behavior remains unchanged. The component does not add
backend or transaction dependencies and does not alter the wire/tuple format.

## Verification and Change Impact

`TupleTests` uses a custom counting TupleElement. A successful call must report
one admission containing the same byte count as the returned result and two
element encode invocations total (one measuring traversal and one materializing
traversal). A rejected call must report one admission, one encode invocation,
no materializing traversal, and the exact typed callback error. Existing tuple
round-trip and byte-count tests remain the regression proof for format
compatibility.

The database-framework FullText caller is a dependent consumer and must adopt
this API only after this component's implementation is committed. No backend,
transaction, or encoding-format change belongs in this component.
