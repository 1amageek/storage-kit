# StorageKit Module

## Purpose and Scope

This module composes the platform-neutral storage contracts and their
implementations. It is a child of the package design in
[`../../DESIGN.md`](../../DESIGN.md).

Its direct children are the Storage component in [`Storage/DESIGN.md`](Storage/DESIGN.md)
and the Tuple component in [`Tuple/DESIGN.md`](Tuple/DESIGN.md).

## Responsibilities and Boundaries

The module defines the boundary between storage contracts and their composed
components. Storage and Tuple own their respective behavioral details. The
module does not own backend deployment, database-framework semantics, schema,
query planning, or authorization.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit package`](../../DESIGN.md) | parent | Package ownership and storage boundary | Defines package-level storage responsibilities | Keep module APIs backend-neutral. |
| [`Storage component`](Storage/DESIGN.md) | child | Backend-neutral transaction and state-owned cursor contracts | Owns storage operations and cursor lifecycle | Consumers use its public contracts; they do not wrap its cursor state. |
| [`Tuple component`](Tuple/DESIGN.md) | child | Admission-aware canonical packing | Owns tuple bytes and encoding | Tuple does not acquire backend or transaction knowledge. |

## Architecture

```text
StorageKit module
    ├─ Storage component
    │   └─ StorageEngine / Transaction / KeyValueCursor
    └─ Tuple component
        └─ Tuple / TupleElement / Subspace
```

## Contracts and Invariants

- Storage and Tuple behavior remains independent of database-framework query
  semantics and backend deployment.
- Each child owns the invariants and lifecycle of the contract it publishes;
  this module composes those contracts without duplicating their details.
- Consumers use the child public APIs and do not depend on child internals.

## Verification and Change Impact

Child design documents own behavioral verification for their contracts. Changes
to Storage contracts require the Storage component tests and dependent consumer
checks. Changes to Tuple contracts require the Tuple component tests and
dependent admission checks. The package design remains the authority for
package-level ownership and boundaries.
