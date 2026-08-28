# StorageKit

## Purpose and Scope

StorageKit is the platform-neutral storage package. It owns storage engine,
transaction, range, mutation, lifecycle, and tuple-key contracts that are
independent of database-framework query semantics and backend deployment.

This document is the package design authority. Its direct child module design
is [`StorageKit`](Sources/StorageKit/DESIGN.md). The Tuple component is designed
under that module.

## Responsibilities and Boundaries

StorageKit owns the semantic storage contracts and their backend adapters. The
Tuple component owns FoundationDB-compatible tuple representation, canonical
encoding, decoding, and bounded materialization admission.

StorageKit does not own database queries, schemas, index planning, model
semantics, transaction authorization, or application-specific resource policy.
The admission callback is a synchronous caller-owned resource boundary; it is
not a storage transaction, backend policy, or global meter.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit module`](Sources/StorageKit/DESIGN.md) | child | StorageKit module ownership and public storage contracts | Composes storage primitives and Tuple | Module changes must preserve the package boundary. |
| [`Tuple component`](Sources/StorageKit/Tuple/DESIGN.md) | child of the StorageKit module | Admission-aware canonical packing | Provides exact pre-allocation admission for callers | Tuple packing must not acquire backend or transaction knowledge. |

## Architecture

```text
StorageKit package
    -> StorageKit module
        -> Tuple component
            -> DatabaseTypes.ByteString
            -> TupleElement encoders
        -> StorageEngine / Transaction contracts
        -> backend adapters
```

The dependency direction points from storage semantics to primitive byte
representation. Tuple encoding is consumed by subspaces and storage keys; it
does not depend on any backend adapter.

## Contracts and Invariants

- Storage operations preserve their existing read-your-writes, range,
  cancellation, rollback, and lifecycle contracts.
- Tuple encoding remains canonical and byte-for-byte compatible with the
  existing `Tuple.pack()` result.
- Admission-aware packing measures the exact canonical output once, invokes a
  synchronous non-retained callback exactly once before result allocation, and
  then allocates and encodes the result exactly once.
- A callback failure is propagated unchanged and produces no packed result or
  second encoding traversal.
- Callers must admit the exact returned packed byte count; they must not infer
  a bound from source bytes or backend limits.
- No backend, transaction, query, or authorization behavior is introduced by
  Tuple packing.

## Runtime Flows

```text
Tuple elements
    -> one measuring traversal
    -> exact canonical byte count
    -> synchronous admission callback
        -> failure: propagate unchanged, no result allocation
        -> success: one exact result allocation
            -> one encoding traversal
            -> byte-count validation
            -> owned ByteString result
```

## State, Ownership, and Lifecycle

`Tuple` remains an immutable value containing its existing element storage. The
admission callback is borrowed for the duration of the synchronous method and
is never retained. The returned `ByteString` owns the packed bytes at the
existing value boundary. No pointer, sink, or callback escapes the method.

## Failure, Concurrency, and Constraints

The method is synchronous and does not suspend. It does not introduce shared
mutable state or a new synchronization boundary. Any error thrown by the
callback crosses the API boundary with its original type and value. Existing
tuple encoder preconditions and `TupleError` failures remain unchanged.

## Verification and Change Impact

`StorageKitTests/TupleTests` owns the behavioral proof. A counting
`TupleElement` verifies that successful admission performs one measuring and
one materializing encoding invocation, while rejected admission performs only
the measuring invocation. The tests also verify one exact admission count,
byte-for-byte equality with `pack()`, and unchanged typed callback failure.

Changes to this contract require rechecking the Tuple component design and the
database-framework caller that consumes the admission boundary. Backend,
transaction, encoding-format, and query tests are outside this component
sprint.
