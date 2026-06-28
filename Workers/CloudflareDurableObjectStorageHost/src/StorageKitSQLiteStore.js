import { decodeBase64URL, encodeBase64URL } from "./StorageKitBase64URL.js";
import { compareBytes } from "./StorageKitByteOrdering.js";
import { applyMutation } from "./StorageKitMutation.js";
import { keySelectorKind, operation } from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

const schemaVersion = 1;
const defaultPageLimit = 128;
const maxPageLimit = 1024;
const conflictVersionWindow = 4096n;
const defaultMutationApplier = Object.freeze({ applyMutation });

export class StorageKitSQLiteStore {
  constructor(sql, transactionSync = null, mutationApplier = defaultMutationApplier) {
    this.sql = sql;
    this.transactionSync = transactionSync ?? ((callback) => callback());
    this.mutationApplier = mutationApplier;
  }

  setMutationApplier(mutationApplier) {
    this.mutationApplier = mutationApplier ?? defaultMutationApplier;
  }

  dispatch(request) {
    switch (request.operation) {
      case operation.readiness:
        return this.readiness();
      case operation.read:
        return this.read(request);
      case operation.range:
        return this.range(request);
      case operation.commit:
        return this.commit(request);
      default:
        throw StorageKitWireError.unknownOperation(request.operation);
    }
  }

  readiness() {
    this.ensureInitialized();
    return {
      operation: operation.readiness,
      schemaVersion,
      commitVersion: this.currentCommitVersion(),
      metadataInitialized: true,
    };
  }

  read(request) {
    this.ensureInitialized();
    if (!request.snapshot) {
      this.verifyReadVersion(request.expectedReadVersion);
    }
    const row = this.first("SELECT value FROM storagekit_kv WHERE key = ?", request.key);
    return {
      operation: operation.read,
      value: row === null ? null : toBytes(row.value),
      currentCommitVersion: this.currentCommitVersion(),
    };
  }

  range(request) {
    this.ensureInitialized();
    if (!request.snapshot) {
      this.verifyReadVersion(request.expectedReadVersion);
    }
    const bounds = this.resolveBounds(request.begin, request.end);
    if (bounds.empty) {
      return {
        operation: operation.range,
        rows: [],
        nextCursor: null,
        currentCommitVersion: this.currentCommitVersion(),
        conflictRange: bounds.conflictRange,
      };
    }

    const predicates = [];
    const bindings = [];
    if (bounds.startKey !== null) {
      predicates.push("key >= ?");
      bindings.push(bounds.startKey);
    }
    if (bounds.endKey !== null) {
      predicates.push("key < ?");
      bindings.push(bounds.endKey);
    }
    if (request.cursor !== null) {
      const cursorKey = decodeCursor(request.cursor);
      predicates.push(request.reverse ? "key < ?" : "key > ?");
      bindings.push(cursorKey);
    }

    const where = predicates.length === 0 ? "" : `WHERE ${predicates.join(" AND ")}`;
    const order = request.reverse ? "DESC" : "ASC";
    const pageLimit = boundedPageLimit(request.limit);
    const rows = this.all(
      `SELECT key, value FROM storagekit_kv ${where} ORDER BY key ${order} LIMIT ?`,
      ...bindings,
      pageLimit + 1
    );
    const hasMore = rows.length > pageLimit;
    const page = hasMore ? rows.slice(0, pageLimit) : rows;
    const responseRows = page.map((row) => ({
      key: toBytes(row.key),
      value: toBytes(row.value),
    }));
    const nextCursor = hasMore && responseRows.length > 0
      ? encodeBase64URL(responseRows[responseRows.length - 1].key)
      : null;

    return {
      operation: operation.range,
      rows: responseRows,
      nextCursor,
      currentCommitVersion: this.currentCommitVersion(),
      conflictRange: bounds.conflictRange,
    };
  }

  commit(request) {
    return this.transactionSync(() => {
      this.ensureInitialized();
      this.verifyReadConflicts(request.observedReadVersion, request.readConflictRanges ?? []);
      const committedVersion = this.currentCommitVersion() + 1n;
      for (const mutation of request.mutations) {
        this.applyWrite(mutation, committedVersion);
      }
      this.setMetadata("commitVersion", committedVersion.toString());
      this.pruneConflictRanges(committedVersion);
      return {
        operation: operation.commit,
        committedVersion,
      };
    });
  }

  applyWrite(mutation, committedVersion) {
    this.recordWriteConflict(mutation, committedVersion);
    switch (mutation.tag) {
      case 1:
        this.exec("INSERT OR REPLACE INTO storagekit_kv(key, value) VALUES (?, ?)", mutation.key, mutation.value);
        break;
      case 2:
        this.exec("DELETE FROM storagekit_kv WHERE key = ?", mutation.key);
        break;
      case 3:
        this.exec("DELETE FROM storagekit_kv WHERE key >= ? AND key < ?", mutation.begin, mutation.end);
        break;
      case 4:
        this.applyAtomic(mutation);
        break;
      default:
        throw StorageKitWireError.unknownWriteOperation(mutation.tag);
    }
  }

  applyAtomic(mutation) {
    const current = this.first("SELECT value FROM storagekit_kv WHERE key = ?", mutation.key);
    const result = this.mutationApplier.applyMutation(
      current === null ? null : toBytes(current.value),
      mutation.param,
      mutation.mutationType
    );
    switch (result.kind) {
      case "set":
        this.exec("INSERT OR REPLACE INTO storagekit_kv(key, value) VALUES (?, ?)", mutation.key, result.value);
        break;
      case "clear":
        this.exec("DELETE FROM storagekit_kv WHERE key = ?", mutation.key);
        break;
      case "unchanged":
        break;
      default:
        throw StorageKitWireError.invalidOperation("Unknown atomic mutation result");
    }
  }

  resolveBounds(begin, end) {
    const start = this.resolveSelector(begin, "begin");
    const finish = this.resolveSelector(end, "end");
    const readConflictRange = conflictRange(
      lowerConflictBound(begin, start.key),
      upperConflictBound(end, finish.key)
    );
    if (start.empty || finish.empty) {
      return {
        empty: true,
        startKey: null,
        endKey: null,
        conflictRange: readConflictRange,
      };
    }
    if (start.key !== null && finish.key !== null && compareBytes(start.key, finish.key) >= 0) {
      return {
        empty: true,
        startKey: null,
        endKey: null,
        conflictRange: readConflictRange,
      };
    }
    return {
      empty: false,
      startKey: start.key,
      endKey: finish.key,
      conflictRange: readConflictRange,
    };
  }

  resolveSelector(selector, role) {
    switch (selector.kind) {
      case keySelectorKind.firstGreaterOrEqual:
        return this.resolveFirst(selector.key, ">=", role);
      case keySelectorKind.firstGreaterThan:
        return this.resolveFirst(selector.key, ">", role);
      case keySelectorKind.lastLessOrEqual:
        return this.resolveLast(selector.key, "<=", role);
      case keySelectorKind.lastLessThan:
        return this.resolveLast(selector.key, "<", role);
      default:
        throw StorageKitWireError.unknownKeySelector(selector.kind);
    }
  }

  resolveFirst(key, operator, role) {
    const row = this.first(`SELECT key FROM storagekit_kv WHERE key ${operator} ? ORDER BY key ASC LIMIT 1`, key);
    if (row === null) {
      return role === "begin"
        ? { empty: true, key }
        : { empty: false, key: null };
    }
    return { empty: false, key: toBytes(row.key) };
  }

  resolveLast(key, operator, role) {
    const row = this.first(`SELECT key FROM storagekit_kv WHERE key ${operator} ? ORDER BY key DESC LIMIT 1`, key);
    if (row === null) {
      return role === "begin"
        ? { empty: false, key: null }
        : { empty: true, key };
    }
    return { empty: false, key: toBytes(row.key) };
  }

  ensureInitialized() {
    this.exec("CREATE TABLE IF NOT EXISTS storagekit_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
    this.exec("CREATE TABLE IF NOT EXISTS storagekit_kv(key BLOB PRIMARY KEY, value BLOB NOT NULL)");
    this.exec(
      "CREATE TABLE IF NOT EXISTS storagekit_conflicts(version_hi INTEGER NOT NULL, version_lo INTEGER NOT NULL, begin_key BLOB NOT NULL, end_key BLOB NOT NULL)"
    );
    this.exec("CREATE INDEX IF NOT EXISTS storagekit_conflicts_version ON storagekit_conflicts(version_hi, version_lo)");
    this.exec(
      "INSERT OR IGNORE INTO storagekit_metadata(key, value) VALUES ('schemaVersion', ?)",
      String(schemaVersion)
    );
    this.exec("INSERT OR IGNORE INTO storagekit_metadata(key, value) VALUES ('commitVersion', '0')");
    const storedSchemaVersion = Number(this.requireMetadata("schemaVersion"));
    if (storedSchemaVersion !== schemaVersion) {
      throw StorageKitWireError.invalidOperation("Unsupported StorageKit Durable Object schema version");
    }
  }

  verifyReadVersion(expectedReadVersion) {
    if (expectedReadVersion === null || expectedReadVersion === undefined) {
      return;
    }
    if (this.currentCommitVersion() !== BigInt(expectedReadVersion)) {
      throw StorageKitWireError.transactionConflict();
    }
  }

  verifyReadConflicts(observedReadVersion, readConflictRanges) {
    if (observedReadVersion === null || observedReadVersion === undefined) {
      return;
    }
    const observedVersion = BigInt(observedReadVersion);
    if (observedVersion < this.minimumRetainedConflictVersion()) {
      throw StorageKitWireError.transactionConflict();
    }
    for (const range of readConflictRanges) {
      if (this.hasConflictingWrite(observedVersion, normalizeReadConflictRange(range))) {
        throw StorageKitWireError.transactionConflict();
      }
    }
  }

  hasConflictingWrite(observedReadVersion, range) {
    if (range === null) {
      return false;
    }
    const splitObservedVersion = splitVersion(observedReadVersion);
    const predicates = ["(version_hi > ? OR (version_hi = ? AND version_lo > ?))"];
    const bindings = [splitObservedVersion.hi, splitObservedVersion.hi, splitObservedVersion.lo];
    if (range.end !== null) {
      predicates.push("begin_key < ?");
      bindings.push(range.end);
    }
    if (range.begin !== null) {
      predicates.push("end_key > ?");
      bindings.push(range.begin);
    }
    const row = this.first(
      `SELECT 1 FROM storagekit_conflicts WHERE ${predicates.join(" AND ")} LIMIT 1`,
      ...bindings
    );
    return row !== null;
  }

  recordWriteConflict(mutation, committedVersion) {
    const range = writeConflictRange(mutation);
    if (range === null) {
      return;
    }
    const version = splitVersion(committedVersion);
    this.exec(
      "INSERT INTO storagekit_conflicts(version_hi, version_lo, begin_key, end_key) VALUES (?, ?, ?, ?)",
      version.hi,
      version.lo,
      range.begin,
      range.end
    );
  }

  pruneConflictRanges(committedVersion) {
    const pruneThrough = committedVersion - conflictVersionWindow;
    if (pruneThrough <= 0n) {
      return;
    }
    const version = splitVersion(pruneThrough);
    this.exec(
      "DELETE FROM storagekit_conflicts WHERE version_hi < ? OR (version_hi = ? AND version_lo <= ?)",
      version.hi,
      version.hi,
      version.lo
    );
  }

  minimumRetainedConflictVersion() {
    const minimum = this.currentCommitVersion() - conflictVersionWindow;
    return minimum > 0n ? minimum : 0n;
  }

  currentCommitVersion() {
    return BigInt(this.requireMetadata("commitVersion"));
  }

  requireMetadata(key) {
    const row = this.first("SELECT value FROM storagekit_metadata WHERE key = ?", key);
    if (row === null) {
      throw StorageKitWireError.invalidOperation("StorageKit metadata is not initialized");
    }
    return row.value;
  }

  setMetadata(key, value) {
    this.exec("INSERT OR REPLACE INTO storagekit_metadata(key, value) VALUES (?, ?)", key, value);
  }

  first(statement, ...bindings) {
    const rows = this.all(statement, ...bindings);
    return rows.length === 0 ? null : rows[0];
  }

  all(statement, ...bindings) {
    const cursor = this.sql.exec(statement, ...bindings);
    if (Array.isArray(cursor)) {
      return cursor;
    }
    if (typeof cursor?.toArray === "function") {
      return cursor.toArray();
    }
    return Array.from(cursor ?? []);
  }

  exec(statement, ...bindings) {
    this.sql.exec(statement, ...bindings);
  }
}

function boundedPageLimit(limit) {
  if (!Number.isInteger(limit) || limit <= 0) {
    return defaultPageLimit;
  }
  return Math.min(limit, maxPageLimit);
}

function decodeCursor(cursor) {
  try {
    return decodeBase64URL(cursor);
  } catch {
    throw StorageKitWireError.invalidCursor();
  }
}

function writeConflictRange(mutation) {
  switch (mutation.tag) {
    case 1:
    case 2:
    case 4:
      return singleKeyRange(mutation.key);
    case 3:
      return normalizeWriteConflictRange(conflictRange(mutation.begin, mutation.end));
    default:
      throw StorageKitWireError.unknownWriteOperation(mutation.tag);
  }
}

function singleKeyRange(key) {
  const end = new Uint8Array(key.length + 1);
  end.set(key, 0);
  end[key.length] = 0;
  return { begin: key, end };
}

function conflictRange(begin, end) {
  return { begin, end };
}

function lowerConflictBound(selector, resolvedKey) {
  switch (selector.kind) {
    case keySelectorKind.firstGreaterOrEqual:
    case keySelectorKind.firstGreaterThan:
      return selector.key;
    case keySelectorKind.lastLessOrEqual:
    case keySelectorKind.lastLessThan:
      return resolvedKey;
    default:
      throw StorageKitWireError.unknownKeySelector(selector.kind);
  }
}

function upperConflictBound(selector, resolvedKey) {
  switch (selector.kind) {
    case keySelectorKind.firstGreaterOrEqual:
    case keySelectorKind.lastLessThan:
      return selector.key;
    case keySelectorKind.firstGreaterThan:
    case keySelectorKind.lastLessOrEqual:
      return singleKeyRange(selector.key).end;
    default:
      throw StorageKitWireError.unknownKeySelector(selector.kind);
  }
}

function normalizeWriteConflictRange(range) {
  if (range.begin !== null && range.end !== null && compareBytes(range.begin, range.end) >= 0) {
    return null;
  }
  if (range.begin === null || range.end === null) {
    throw StorageKitWireError.invalidOperation("Write conflict range must be bounded");
  }
  return range;
}

function normalizeReadConflictRange(range) {
  const begin = range?.begin ?? null;
  const end = range?.end ?? null;
  if (begin !== null && end !== null && compareBytes(begin, end) >= 0) {
    return null;
  }
  return { begin, end };
}

function splitVersion(version) {
  const unsigned = BigInt.asUintN(64, version);
  return {
    hi: Number((unsigned >> 32n) & 0xffff_ffffn),
    lo: Number(unsigned & 0xffff_ffffn),
  };
}

function toBytes(value) {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw StorageKitWireError.invalidOperation("SQLite returned a non-binary value");
}
