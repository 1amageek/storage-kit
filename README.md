# StorageKit

A unified key-value storage abstraction for Swift, with pluggable backends for **FoundationDB**, **SQLite**, and **in-memory** storage.

StorageKit provides a single `Transaction` protocol that works identically across all backends. Write your data access code once, then swap the backend without changing application logic.

## Features

- **Unified API** — `StorageEngine` and `Transaction` protocols abstract away backend differences
- **FDB-compatible semantics** — Lexicographic key ordering, range scans, `KeySelector`, Tuple Layer, Subspace, DirectoryService
- **Zero-copy design** — `getRange` returns backend-native `AsyncSequence` types without intermediate wrappers
- **Swift 6 concurrency** — Full `Sendable` conformance, `Mutex` for synchronization, no `@unchecked Sendable`
- **Nested transactions** — SQLite backend detects nested `withTransaction` calls via `@TaskLocal` and reuses the existing transaction

## Installation

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/1amageek/storage-kit.git", branch: "main"),
]
```

Then add the targets you need:

```swift
.target(
    name: "YourApp",
    dependencies: [
        .product(name: "StorageKit", package: "storage-kit"),
        // Pick one (or more) backends:
        .product(name: "SQLiteStorage", package: "storage-kit"),
        .product(name: "FDBStorage", package: "storage-kit"),
    ]
)
```

## Quick Start

```swift
import StorageKit
import SQLiteStorage

// Create an engine
let engine = try SQLiteStorageEngine(configuration: .inMemory)

// Write and read within a transaction
try await engine.withTransaction { tx in
    tx.setValue([1, 2, 3], for: Array("hello".utf8))

    let value = try await tx.getValue(for: Array("hello".utf8))
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

File-based or in-memory. Uses `WITHOUT ROWID` table for efficient BLOB key B-tree storage. Transactions are serialized with `NSLock`.

```swift
// File-based
let engine = try SQLiteStorageEngine(configuration: .file("/path/to/db.sqlite"))

// In-memory (testing)
let engine = try SQLiteStorageEngine(configuration: .inMemory)
```

### FoundationDB

Requires a running FDB cluster. Wraps FDB's native `TransactionProtocol` with automatic retry on conflict.

```swift
let engine = try await FDBStorageEngine(configuration: .init())
```

FDB client initialization is handled automatically with a thread-safe `InitializationGuard`.

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
    tx.setValue(newValue, for: key)

    // Delete
    tx.clear(key: key)

    // Range delete
    tx.clearRange(beginKey: start, endKey: end)

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
let packed: Bytes = tuple.pack()
let unpacked = try Tuple.unpack(from: packed)
```

Supported types: `String`, `Int64`, `Int32`, `Int`, `UInt64`, `Float`, `Double`, `Bool`, `Bytes`, `TupleNil`, `Versionstamp`.

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

### DirectoryService

Hierarchical namespace management (equivalent to FDB's DirectoryLayer):

```swift
let dir = engine.directoryService
let userSpace = try await dir.createOrOpen(path: ["app", "users"])
let indexSpace = try await dir.createOrOpen(path: ["app", "users", "email_index"])
```

- **FDB**: `FDBDirectoryService` — dynamic prefix allocation via DirectoryLayer with HCA
- **SQLite / InMemory**: `StaticDirectoryService` — deterministic Tuple encoding (same API, no dynamic allocation)

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
│  │          │  │            │  │ DirectoryService     │ │
│  └──────────┘  └────────────┘  └──────────────────────┘ │
├─────────────┬───────────────┬───────────────────────────┤
│  InMemory   │  SQLiteStorage│      FDBStorage           │
│             │               │                           │
│ Sorted array│ WITHOUT ROWID │ Native FDB transaction    │
│ + snapshot  │ + NSLock      │ + automatic retry         │
│ isolation   │ serialization │ + zero-copy range results │
└─────────────┴───────────────┴───────────────────────────┘
```

### Key Internal Types

| Type | Module | Purpose |
|------|--------|---------|
| `SortedKeyValueStore` | StorageKit | O(log n) sorted array with binary search, used by InMemory backend |
| `KeyValueRangeResult` | StorageKit | Shared array-backed `AsyncSequence` for InMemory and SQLite range results |
| `compareBytes` | StorageKit | `memcmp`-based lexicographic byte comparison (hot path) |
| `ActiveTransactionScope` | StorageKit | `@TaskLocal` for nested transaction detection in SQLite |

## Requirements

- Swift 6.0+
- macOS 15+ / iOS 18+
- FoundationDB 7.1+ (for FDBStorage only)

## License

MIT
