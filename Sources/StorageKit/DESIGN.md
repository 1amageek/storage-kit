# StorageKit Module

## Purpose and Scope

The StorageKit module provides the public platform-neutral storage contracts
and their in-memory implementation. It is a child of the package design in
[`../../DESIGN.md`](../../DESIGN.md).

The Tuple component is the directly composed child documented in
[`Tuple/DESIGN.md`](Tuple/DESIGN.md).

## Responsibilities and Boundaries

This module owns storage engine and transaction protocols, key/value and range
operations, lifecycle and failure semantics, and the Tuple component's
canonical key representation. It does not own backend-specific execution,
database-framework query planning, schema or model meaning, or authorization.

Tuple packing owns only representation and intrinsic encoding invariants. A
caller-provided admission callback may account for an exact packed allocation,
but the module does not decide what resource policy the caller applies.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit package`](../../DESIGN.md) | parent | Package ownership and storage boundary | Defines the package-level storage responsibilities | Module APIs must remain backend-neutral. |
| [`Tuple component`](Tuple/DESIGN.md) | child | Exact admission-aware Tuple packing | Owns tuple bytes and canonical encoding | Do not move query, backend, or transaction policy into Tuple. |

## Architecture

```text
StorageKit module
    ├─ Storage contracts
    │   ├─ StorageEngine / Transaction
    │   ├─ range / selector / mutation semantics
    │   └─ lifecycle / cancellation / failure contracts
    ├─ InMemory implementation
    └─ Tuple component
        ├─ Tuple / TupleElement
        ├─ TupleEncodingSink
        └─ Subspace / TupleCursor
```

Consumers depend on these module contracts. Backend adapters implement the
storage protocols; Tuple remains usable without a live engine or transaction.

## Contracts and Invariants

- Storage protocol behavior is independent of backend implementation.
- Tuple output is deterministic for a stable element sequence and remains
  byte-compatible with the established encoding.
- `Tuple.pack(admitting:)` invokes the exact synchronous admission callback
  before allocating its result and does not retain that callback.
- A rejected admission does not materialize a packed result and propagates the
  callback's typed error unchanged.
- The module does not derive an admission bound from source key length or
  backend limits.

## Runtime Flows

Storage operations follow the existing transaction lifecycle. Tuple callers
follow the component flow:

```text
caller
  -> Tuple.pack(admitting:)
      -> measure canonical bytes
      -> admit exact count
      -> allocate and encode once
      -> return ByteString
```

## State, Ownership, and Lifecycle

The module's transaction and engine owners remain unchanged. Tuple owns its
immutable element storage and returns an owned `ByteString`; the callback and
encoding sink are method-scoped borrows. No module-level registry or shared
admission state is introduced.

## Failure, Concurrency, and Constraints

Tuple admission is synchronous and non-suspending. It uses no new mutable
shared state. Existing storage cancellation, rollback, and lifecycle failures
remain typed and are not converted into empty or synthetic success.

## Verification and Change Impact

The Tuple component tests provide the focused evidence for admission ordering,
exact counts, result bytes, and typed failure propagation. Changes to storage
protocols or backend adapters require their owning tests; this sprint changes
none of those contracts. Database-framework integration is a dependent
consumer check after this module publishes its committed revision.
