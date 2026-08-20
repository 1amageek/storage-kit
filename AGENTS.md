# AGENTS.md

## Responsibility

- This package owns the platform-neutral StorageEngine contract and backend implementations, including transaction, range, conflict, atomic mutation, rollback, and read-your-writes semantics.
- A backend adapter translates those semantics to its storage system. It does not implement database queries, schemas, graph behavior, application commands, or Worker routing.
- The Durable Object SQLite adapter is the single Cloudflare storage implementation.

## Naming

- Name declarations for their storage-domain responsibility, observable behavior, state transition, ownership, or lifetime contract.
- Follow the Swift API Design Guidelines at every access level, including tests and host-boundary declarations.
- Do not name ordinary declarations after implementation language, ABI, calling convention, module identity, binary format, toolchain, build mode, or memory-layout strategy.
- Keep externally fixed host symbols in ABI descriptors and give Swift wrappers semantic names.
- Name callbacks for the storage event they deliver. Names such as `regular`, `legacy`, `impl`, `helper`, `manager`, or a bare `callback` are invalid.
- Distinguish owned keys and values from transaction-scoped borrowed views.

## Transaction, Data, and Error Contracts

- Preserve read-your-writes, selectors, reverse and limited ranges, pagination, clearRange, atomics, conflicts, cancellation, and rollback across every backend.
- Never split one logical mutation across transaction boundaries.
- Keep key, value, range-page, and host-frame bytes in retained owners with bounded views. Copy only at an explicit backend ownership boundary and document why.
- Do not silently downgrade a storage failure, conflict, cancellation, unsupported primitive, or malformed host frame to an empty value or success.
- Platform dependencies must remain in their adapter products and out of the platform-neutral core.
- This is version 1. Remove duplicate storage protocols and compatibility adapters.

## PostgreSQL Test Harness

- Run PostgreSQL integration coverage with `scripts/postgresql-test-harness`.
- Set `TOOLCHAINS=org.swift.64202607231a` and explicitly provide
  `POSTGRES_TEST_HOST`, `POSTGRES_TEST_PORT`, `POSTGRES_TEST_USER`,
  `POSTGRES_TEST_PASSWORD`, and `POSTGRES_TEST_DB` for an isolated PostgreSQL
  16 database.
- The harness uses the `storage-kit-Package` Xcode scheme, selects only the
  `PostgreSQLStorageTests` target, injects the snapshot
  testing runtime and service environment into `.xctestrun`, and requires 128
  tests with zero failures, skips, expected failures, runtime warnings, and
  PostgresClient startup-order warnings.
- The Xcode package scheme compiles every package target before applying the
  test selection. Install the FoundationDB C SDK and keep its header and client
  library available under `/usr/local/include` and `/usr/local/lib`, or set
  `FDB_SDK_INCLUDE_DIRECTORY` and `FDB_SDK_LIBRARY_DIRECTORY` explicitly.
- The harness also runs only
  `PostgreSQLIntegrationEnvironmentTests.endpointIsConfigured()` without
  service variables and requires exactly one explicit failure, zero passes,
  zero skips, zero expected failures, and zero runtime warnings. A successful
  result is a false-green defect.
- Every raw engine test awaits `waitUntilShutdown()` before its test boundary
  ends. `requestShutdown()` alone is not complete cleanup for the asynchronous
  PostgreSQL connection pool.
- Compile the PostgreSQL product for the pinned static Linux SDK with
  `swift build --swift-sdk swift-6.4.x-DEVELOPMENT-SNAPSHOT-2026-07-23-a_static-linux-0.1.0 --triple aarch64-swift-linux-musl --product PostgreSQLStorage -c release -debug-info-format none`.
  This is a compile/link gate; the real PostgreSQL behavioral gate remains the
  Xcode harness above.
