# Decision Record: PostgreSQLStorage stays on PostgresNIO

- Status: Accepted
- Date: 2026-08-03
- Scope: `Sources/PostgreSQLStorage` (SPM trait `PostgreSQL`)

## Decision

`PostgreSQLStorage` keeps [PostgresNIO](https://github.com/vapor/postgres-nio)
as its driver. Neither a libpq C-binding backend nor a self-written wire
protocol client will be pursued. This is a full commitment, not a transition
state: no abstraction seam is introduced for a hypothetical future driver, and
`PostgreSQLConfiguration.clientConfiguration: PostgresClient.Configuration`
remains a deliberate public re-export that gives callers the driver's complete
configuration surface.

## Context

Measured dependency surface at decision time:

- PostgresNIO imports are confined to 6 files in `Sources/PostgreSQLStorage`
  (~47 call sites).
- `database-framework` contains zero PostgresNIO references; it consumes this
  backend only through the `StorageKit` contracts.
- One public type re-export: `PostgreSQLConfiguration.clientConfiguration`.

Alternatives considered: (a) libpq C bindings behind the coordinator-actor
pattern used by SQLiteStorage, (b) a pure-Swift wire-protocol client.

## Rationale

1. **Static Musl cross-compilation is a gated production path.** The
   portability gate builds `--triple aarch64-swift-linux-musl --traits
   PostgreSQL` (see `database-framework/docs/production-readiness.md`), and the
   Cloud Run smoke deployment consumes the PostgreSQL trait end-to-end.
   PostgresNIO is pure Swift and passes this gate today. A libpq backend would
   require vendoring libpq and a TLS library as static Musl C artifacts into
   the cross-compilation SDK — a permanent toolchain burden.
2. **libpq's I/O model conflicts with the workspace concurrency contract.**
   libpq's synchronous API blocks the calling thread on network I/O, which is
   unbounded — unlike SQLiteStorage's microsecond-scale file locking, this
   cannot be confined acceptably inside an actor on the cooperative pool.
   Using libpq's asynchronous API instead requires owning socket-readiness
   eventing (an event loop), i.e. rebuilding the part of swift-nio this
   package actually uses. The FDB precedent does not transfer: the FDB C
   client runs its own network thread; libpq does not.
3. **Authentication and TLS stay with a maintained upstream.** SCRAM-SHA-256,
   channel binding, and TLS are a security surface this workspace should not
   own.
4. **Verified track record.** 98 tests against a real Cloud SQL instance
   (2026-06-13) and the 71-test PostgreSQL contract suite in CI run against
   this driver.

## Accepted risks and their guards

- `PostgresClient.run()` is assumed to publish its running state before its
  first suspension so that `Task.immediate` startup ordering prevents
  lease-before-run. This is undocumented upstream behavior. Guard:
  `scripts/postgresql-test-harness` fails the run if the PostgresNIO
  "run() hasn't been called yet" warning appears in logs, so an upstream
  behavior change is caught by CI, not production.
- The public `PostgresClient.Configuration` re-export ties this trait's API
  surface to postgres-nio major versions. Accepted deliberately in exchange
  for full driver configurability without a wrapper to maintain.
- Result and binding bytes are copied once at the driver boundary because
  PostgresNIO may outlive a borrow (`PostgreSQLBindingBytes.swift`,
  `PostgreSQLResultBytesOwner.swift`). Accepted; documented at the call sites.

## Re-evaluation triggers

Reopen this decision only if one of the following occurs:

1. An upstream change breaks the run()/lease ordering contract or its CI
   guard, and no supported replacement exists.
2. The static Musl gate fails because of swift-nio or postgres-nio and cannot
   be fixed upstream in reasonable time.
3. postgres-nio maintenance or security response demonstrably stalls.
4. The workspace drops the static-Linux PostgreSQL deployment path entirely.
