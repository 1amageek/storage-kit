# StorageKit

A unified key-value storage abstraction for Swift, with pluggable backends for
**FoundationDB**, **SQLite**, **Cloudflare Durable Object SQLite**, and
**in-memory** storage.

StorageKit provides a single `Transaction` protocol that works identically across all backends. Write your data access code once, then swap the backend without changing application logic.

## Features

- **Unified API** — `StorageEngine` and `Transaction` protocols abstract away backend differences
- **FDB-compatible semantics** — Lexicographic key ordering, range scans, `KeySelector`, Tuple Layer, Subspace, and namespace resolution
- **Zero-copy design** — `getRange` returns backend-native `AsyncSequence` types without intermediate wrappers
- **Swift 6.4 concurrency** — Full `Sendable` conformance, `Mutex` for synchronization, no `@unchecked Sendable`
- **Nested transactions** — SQLite backend detects nested calls via `@TaskLocal` and creates strictly ordered savepoint-backed child transactions
- **Foundation-free Cloudflare protocol** — bounded StorageKit Wire v1 values, encoding, and decoding for Native, WASM, and Embedded Swift
- **Durable Object transactions** — SQLite persistence, pinned reads, selector-aware conflicts, bounded pagination, and atomic commit

## Installation

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/1amageek/database-types.git", from: "26.0727.5"),
    .package(url: "https://github.com/1amageek/storage-kit.git", from: "26.0727.2"),
]
```

Then add the targets you need:

```swift
.target(
    name: "YourApp",
    dependencies: [
        .product(name: "DatabaseTypes", package: "database-types"),
        .product(name: "StorageKit", package: "storage-kit"),
        // Add StorageKitSystemClock when the runtime supplies system clock/sleep.
        .product(name: "StorageKitSystemClock", package: "storage-kit"),
        // Add StorageKitFoundation only when Foundation Date/UUID tuple adapters are needed.
        .product(name: "StorageKitFoundation", package: "storage-kit"),
        // Pick one (or more) backends:
        .product(name: "SQLiteStorage", package: "storage-kit"),
        .product(name: "FDBStorage", package: "storage-kit"),
        .product(name: "CloudflareDurableObjectStorage", package: "storage-kit"),
    ]
)
```

## Quick Start

```swift
import DatabaseTypes
import StorageKit
import SQLiteStorage

// Create an engine
let engine = try SQLiteStorageEngine(configuration: .inMemory)

// Write and read within a transaction
try await engine.withTransaction { tx in
    let key = ByteString(utf8: "hello")
    try tx.setValue([1, 2, 3], for: key)

    let value = try await tx.getValue(for: key)
    // value == [1, 2, 3]
}
```

## Backends

All backends conform to `StorageEngine` with a unified `init(configuration:)` pattern.

### InMemory

No dependencies. Sorted array with snapshot isolation. Ideal for testing.

```swift
let engine = InMemoryEngine()
```

### SQLite

File-based or in-memory. Uses a `WITHOUT ROWID` table for efficient BLOB key B-tree storage. A coordinator actor owns each transaction from `BEGIN IMMEDIATE` through commit or rollback, while short synchronous connection access is protected by `Mutex`.

```swift
// File-based
let engine = try SQLiteStorageEngine(configuration: .file("/path/to/db.sqlite"))

// In-memory (testing)
let engine = try SQLiteStorageEngine(configuration: .inMemory)
```

### FoundationDB

Requires a running FDB cluster. Wraps FDB's native `TransactionProtocol` while
leaving whole-transaction retry policy to the higher database layer.

```swift
let engine = try await FDBStorageEngine(configuration: .init())
```

FoundationDB client startup is serialized automatically across concurrent
storage-engine initialization. `fdb-swift-bindings` owns FoundationDB C handle
and future lifetimes, while `DatabaseTypes.ByteString` is the shared byte value
exposed to StorageKit. FoundationDB result buffers therefore reach StorageKit as
borrowed `ByteString` storage without an intermediate byte wrapper or payload
copy.

```text
DatabaseTypes.ByteString
        ↑
fdb-swift-bindings
        ↑
     FDBStorage
```

### Cloudflare Durable Object SQLite

The Cloudflare backend is split by runtime boundary:

| Product | Use |
|---|---|
| `CloudflareDurableObjectStorageWire` | Foundation-free StorageKit Wire v1 values, bounded encoding, and bounded decoding |
| `CloudflareDurableObjectStorage` | `StorageEngine`, transaction state, and typed StorageKit Wire client |
| `CloudflareDurableObjectStorageHTTP` | URLSession transport for native clients |
| `CloudflareDurableObjectStorageHostTransport` | Synchronous `storage_host.dispatch` transport for a WASI reactor |

`StorageKit` itself is Foundation-free. `StorageKitFoundation` supplies the
explicit Foundation `Date` and `UUID` Tuple Layer adapters; canonical UUID tuple
decoding returns `DatabaseTypes.UUID`.

`StorageKit` owns the monotonic clock contract but no operating-system clock.
`StorageKitSystemClock` is the explicit adapter for runtimes that provide Swift's
`ContinuousClock`. Embedded runtimes inject their own clock without weakening
the storage contract.

The storage engine and typed client use the canonical
`CloudflareDurableObjectStorageWire` request, response, mutation, range, and
scope values directly. The backend does not define parallel DTOs for the same
wire semantics.

The reusable JavaScript host lives in
`Workers/CloudflareDurableObjectStorageHost`. An application-owned Durable
Object creates this host with `ctx.storage.sql`; the host persists keys,
metadata, and conflict history in Durable Object SQLite. HTTP routing,
authorization, Worker lifecycle, and deployment remain application concerns.
The Worker under `test/fixtures` exists only for real local SQLite validation.

StorageKit Wire v1 is a bounded binary storage protocol. It is intentionally
separate from database-framework's DatabaseWire query protocol. See
`Docs/CLOUDFLARE_DURABLE_OBJECT_STORAGE_DESIGN.md` for the complete contract.

## Core Concepts

### Transaction

All reads and writes go through `Transaction`. The protocol mirrors FDB's transaction semantics:

```swift
try await engine.withTransaction { tx in
    // Point read
    let value = try await tx.getValue(for: key)

    // Range scan (begin inclusive, end exclusive)
    let results = try await tx.collectRange(begin: startKey, end: endKey)

    // Write (buffered until commit)
    try tx.setValue(newValue, for: key)

    // Delete
    try tx.clear(key: key)

    // Range delete
    try tx.clearRange(beginKey: start, endKey: end)

    // Auto-committed on success, rolled back on error
}
```

`withTransaction` handles commit/rollback automatically. For manual control, use `createTransaction()`.

### KeySelector

FDB-compatible key selectors for precise range boundaries:

```swift
// First key >= target
KeySelector.firstGreaterOrEqual(key)

// First key > target
KeySelector.firstGreaterThan(key)

// Last key <= target
KeySelector.lastLessOrEqual(key)

// Last key < target
KeySelector.lastLessThan(key)
```

### Tuple Layer

Encodes multiple typed values into byte arrays where lexicographic order of the encoded bytes matches the logical order of the elements. Compatible with the FDB Tuple Layer specification.

```swift
let tuple = Tuple("users", Int64(42), "profile")
let packed: ByteString = tuple.pack()
let unpacked = try Tuple.unpack(from: packed)
```

Supported types: `String`, signed and unsigned integers, `Float`, `Double`,
`Bool`, `ByteString`, `DatabaseTypes.UUID`, nested `Tuple`, `TupleNil`, and
`Versionstamp`. Foundation `Date` and `UUID` adapters are provided by
`StorageKitFoundation`.

### Subspace

Manages key prefixes for logical partitioning:

```swift
let users = Subspace("users")
let user42 = users.subspace(Int64(42))

// Pack a key within the subspace
let key = user42.pack(Tuple("email"))

// Get the full range of keys in a subspace
let (begin, end) = users.range()

// Check membership
users.contains(key) // true
```

### Namespace resolution and catalog capability

Hierarchical namespace management (equivalent to FDB's DirectoryLayer):

```swift
let userSpace = try await engine.resolveOrCreateNamespace(
    path: ["app", "users"]
)

try await engine.withTransaction { transaction in
    let indexSpace = try await engine.namespaceResolver.resolveOrCreate(
        path: ["app", "users", "email_index"],
        transaction: transaction
    )
    // Directory metadata and application writes now share one transaction.
}
```

- **FDB**: a persistent namespace registry resolves paths and also exposes `NamespaceCatalog` for enumeration and removal.
- **SQLite / InMemory / PostgreSQL**: `DeterministicNamespaceResolver` maps every valid path directly to a `Subspace` and does not expose a catalog.

Every resolver and catalog operation receives the caller-owned transaction.
The one-shot `StorageEngine` helpers create a transaction for convenience; use
the transaction-aware capability whenever namespace metadata and data mutations
must commit atomically. Catalog absence is represented by `nil`, rather than by
methods that exist only to throw unsupported-operation errors.

### Physical Compaction

Physical storage maintenance is an optional transaction capability. Native
`SQLiteStorage` exposes `StorageCompactionTransaction`; Cloudflare
Durable Object SQLite does not, because the public platform does not expose the
required vacuum PRAGMAs. Capability absence is reported as unsupported and is
never treated as a successful no-op.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application Code                      │
│         (uses StorageEngine + Transaction protocols)     │
├─────────────────────────────────────────────────────────┤
│                       StorageKit                         │
│  ┌──────────┐  ┌────────────┐  ┌──────────────────────┐ │
│  │ Engine   │  │Transaction │  │ Tuple Layer          │ │
│  │ Protocol │  │ Protocol   │  │ Tuple, Subspace,     │ │
│  │          │  │            │  │ KeySelector,         │ │
│  │          │  │            │  │ NamespaceResolver    │ │
│  │          │  │            │  │ NamespaceCatalog?    │ │
│  └──────────┘  └────────────┘  └──────────────────────┘ │
├─────────────┬───────────────┬───────────────────────────┤
│  InMemory   │ SQLiteStorage │ FDBStorage │ Cloudflare DO │
│ Sorted array│ WITHOUT ROWID │ Native FDB │ DO SQLite     │
│ + snapshot  │ + serialized  │ + retry    │ + conflicts   │
└─────────────┴───────────────┴────────────┴───────────────┘
```

### Key Internal Types

| Type | Module | Purpose |
|------|--------|---------|
| `SortedKeyValueStore` | StorageKit | O(log n) sorted array with binary search, used by InMemory backend |
| `KeyValueRangeResult` | StorageKit | Array-backed reference result for the in-memory backend |
| `SQLiteRangeResult` | SQLiteStorage | Lazy cursor-backed result with explicit finish and ownership handling |
| `compareBytes` | StorageKit | `memcmp`-based lexicographic byte comparison (hot path) |
| `ActiveTransactionScope` | StorageKit | `@TaskLocal` for nested transaction detection in SQLite |

## Requirements

- Swift 6.4+
- macOS 15+ / iOS 18+
- FoundationDB 7.1+ (for FDBStorage only)

## License

MIT
