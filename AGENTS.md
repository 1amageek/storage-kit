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
