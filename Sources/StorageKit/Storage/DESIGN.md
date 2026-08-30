# Storage Component

## Purpose and Scope

The Storage component owns the platform-neutral engine, transaction, key/value,
range, mutation, cancellation, and lifecycle contracts implemented by the
StorageKit module. It is a child of [`../DESIGN.md`](../DESIGN.md) and the
package design at [`../../DESIGN.md`](../../DESIGN.md).

## Responsibilities and Boundaries

The component owns backend-neutral storage behavior and the state machine for a
transaction range cursor. It owns cursor cleanup, scope validation, cancellation,
and lifetime coordination. It does not own tuple encoding, query or schema
meaning, authorization policy, backend deployment, or caller resource policy.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit module`](../DESIGN.md) | parent | Module composition and child boundary | Composes this component with Tuple | Keep cursor details here rather than in the module index. |
| [`StorageKit package`](../../DESIGN.md) | ancestor | Package storage ownership | Defines the package boundary | Backend adapters must preserve this component's behavior. |
| Database framework Read consumer (`Sources/DatabaseEngine/Read/DESIGN.md`) | dependent consumer | Scoped range access uses `KeyValueCursor.validatingScope` | The framework consumer supplies the read-scope validation callback and lifetime owner | Recheck the consumer pin and cursor integration when this contract changes. |

## Architecture

```text
Transaction range request
    -> one KeyValueCursor state
        ├─ one backend cursor
        ├─ one advance/finish boundary
        ├─ one lifetime owner
        └─ zero-copy key/value results
```

The public `KeyValueCursor` erases the backend result while its private actor
state owns ordered suspension and cleanup. Scope validation is passed into that
state; no second cursor state is introduced at the caller boundary.

## Contracts and Invariants

- `KeyValueCursor.validatingScope` composes the caller's synchronous validation
  with the same cursor state. It does not copy returned key/value bytes.
- Every scoped `next()` follows exactly this order:

  ```text
  pre-validation
      -> one backend advance
      -> cancellation check
      -> post-validation
      -> publish ready/exhaustion or resolve finish boundary
  ```

- Post-validation is completed before a row becomes observable, before the
  ready state is published, and before a concurrent finish boundary resolves.
- A cursor issued by a bound Partition access belongs to the binding scope that
  issued it. Closing that scope completes the cursor's backend cleanup, so an
  escaped cursor has no outstanding capability — cleanup included — once the
  binding closes, and no cleanup is left to run against a transaction its owner
  has already closed.
- A commit whose outcome is unknown is reported as that unknown outcome. The
  transaction owner does not cancel, clean up, or wrap it into a different
  failure, because the transaction may already have been applied. This holds in
  every transaction owner, including the backends that override transaction
  execution.
- `finish()` waits for an in-flight advance boundary. Cursor lifetime owners
  are released only after backend cleanup and post-validation have completed.
- A validation, cancellation, iteration, or cleanup failure remains typed.
  When iteration and cleanup both fail, the existing cleanup error preserves
  both failures; cleanup is attempted exactly once.
- Returned key/value buffers retain their backend owners after cursor cleanup.
  The cursor does not materialize or retain a second copy.

## Runtime Flows

```text
next()
  -> scope admission
  -> backend cursor.next()
  -> task cancellation
  -> scope validation
      ├─ failure -> backend finish -> release owner -> typed failure
      └─ success
          ├─ row -> publish row only after validation
          └─ exhaustion -> backend finish -> release owner
              -> resolve waiting finish
```

Concurrent `finish()` observes the advancing state, requests the same advance
boundary to finish, and waits for the advance to perform post-validation and
backend cleanup. It never starts an independent backend cleanup path.

## State, Ownership, and Lifecycle

The actor state owns the backend cursor, its advance/finish boundary, and
terminal cleanup. `KeyValueCursorLifetime` owns attached scope or transaction
owners until terminal cleanup completes. Validation callbacks are retained by
the `KeyValueCursor` for its lifetime and invoked synchronously at the defined
boundaries. Backend-owned byte buffers are returned unchanged and remain owned
by their result values.

## Failure, Concurrency, and Constraints

Actor isolation orders suspendable backend I/O and finish coordination; no
mutex is held across `await`. Cancellation and scope revocation are terminal
for iteration and await backend cleanup before escaping. Cleanup failures retain
the `StorageRangeCleanupError` and `StorageRangeTerminalCleanupError` contracts.
The state has one backend advance per returned row and no ambient authority or
module-level registry.

## Verification and Change Impact

`StorageKitTests/TransactionRangeCleanupTests` owns the behavioral proof for
pre/post ordering, one backend advance per row, cancellation and revocation,
admission before backend open, finish coordination, typed cleanup composition,
exactly-once cleanup, and zero-copy byte ownership. The deterministic finish
test observes the actor's advancing state before resuming backend advance and
proves that backend finish and owner release occur after post-validation.

Changes to this component require its focused tests and a dependent
database-framework cursor check. Backend adapter tests are required when a
storage protocol or backend implementation changes.
