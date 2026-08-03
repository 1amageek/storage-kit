import { compareBytes } from "./StorageKitByteOrdering.js";
import { applyMutation } from "./StorageKitMutation.js";
import { nameForScope } from "./StorageKitScope.js";
import {
  mutationType,
  operation,
} from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";
import { materializeMutation } from "./StorageKitVersionstamp.js";

const schemaVersion = 1;
// Retention invariant: with consecutive commit versions the window bounds the
// retained history at conflictVersionWindow * maximumPersistedConflictRangesPerCommit
// entries (4096 * 256 = 1,048,576 = storageKitWireLimits.maxConflictEntries).
// Byte retention has no such structural bound (ranges carry up to ~2KB of key
// bytes each), so pruneConflictRanges must also enforce
// storageKitWireLimits.maxConflictEntries / maxConflictBytes as hard limits.
// Pruning is always safe: verifyReadConflicts conservatively rejects any
// commit whose observed read version predates the retained history.
const conflictVersionWindow = 4096n;
const responseOverheadReserve = 16 * 1024;
const maximumPersistedConflictRangesPerCommit = 256;
const requiredMetadataKeys = Object.freeze([
  "schemaVersion",
  "commitVersion",
  "conflictEntryCount",
  "conflictByteCount",
  "minimumConflictVersion",
]);

export class StorageKitSQLiteStore {
  constructor(sql, transactionSync) {
    if (typeof transactionSync !== "function") {
      throw StorageKitWireError.invalidOperation(
        "A synchronous Durable Object transaction executor is required"
      );
    }
    this.sql = sql;
    this.transactionSync = transactionSync;
    this.initialized = false;
  }

  migrate() {
    if (this.hasCurrentSchema()) {
      this.initialized = true;
      return;
    }
    this.transactionSync(() => {
      this.exec("CREATE TABLE IF NOT EXISTS storagekit_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
      this.exec("CREATE TABLE IF NOT EXISTS storagekit_kv(key BLOB PRIMARY KEY, value BLOB NOT NULL)");
      this.exec(
        "CREATE TABLE IF NOT EXISTS storagekit_conflicts(version_hi INTEGER NOT NULL, version_lo INTEGER NOT NULL, begin_key BLOB NOT NULL, end_key BLOB NOT NULL)"
      );
      this.exec(
        "CREATE INDEX IF NOT EXISTS storagekit_conflicts_version ON storagekit_conflicts(version_hi, version_lo)"
      );
      this.exec(
        "CREATE TABLE IF NOT EXISTS storagekit_conflict_versions(version_hi INTEGER NOT NULL, version_lo INTEGER NOT NULL, entry_count INTEGER NOT NULL, byte_count INTEGER NOT NULL, PRIMARY KEY(version_hi, version_lo))"
      );
      this.insertMetadataIfMissing("schemaVersion", String(schemaVersion));
      this.insertMetadataIfMissing("commitVersion", "0");
      this.insertMetadataIfMissing("conflictEntryCount", "0");
      this.insertMetadataIfMissing("conflictByteCount", "0");
      this.insertMetadataIfMissing("minimumConflictVersion", "0");
      const storedSchemaVersion = Number(this.requireMetadata("schemaVersion"));
      if (storedSchemaVersion !== schemaVersion) {
        throw StorageKitWireError.invalidOperation(
          "Unsupported StorageKit Durable Object schema version"
        );
      }
    });
    this.initialized = true;
  }

  hasCurrentSchema() {
    let metadataRows;
    try {
      metadataRows = this.all("SELECT key, value FROM storagekit_metadata");
    } catch (error) {
      if (isMissingMetadataTable(error)) {
        return false;
      }
      throw error;
    }

    const metadata = new Map(metadataRows.map((row) => [row.key, row.value]));
    const storedSchemaVersion = Number(metadata.get("schemaVersion"));
    if (metadata.has("schemaVersion")
        && storedSchemaVersion !== schemaVersion) {
      throw StorageKitWireError.invalidOperation(
        "Unsupported StorageKit Durable Object schema version"
      );
    }
    return requiredMetadataKeys.every((key) => metadata.has(key));
  }

  dispatch(request) {
    this.requireInitialized();
    this.bindOrVerifyScope(request.scope);
    switch (request.operation) {
      case operation.readiness:
        return this.readiness();
      case operation.read:
        return this.read(request);
      case operation.range:
        return this.range(request);
      case operation.commit:
        return this.commit(request);
      case operation.rangeSize:
        return this.rangeSize(request);
      case operation.rangeSplitPoints:
        return this.rangeSplitPoints(request);
      default:
        throw StorageKitWireError.unknownOperation(request.operation);
    }
  }

  readiness() {
    return {
      operation: operation.readiness,
      schemaVersion,
      commitVersion: this.currentCommitVersion(),
      metadataInitialized: true,
    };
  }

  read(request) {
    this.verifyReadVersion(request.expectedReadVersion);
    validateKey(request.key);
    const row = this.first("SELECT value FROM storagekit_kv WHERE key = ?", request.key);
    return {
      operation: operation.read,
      value: row === null ? null : validatedValue(row.value),
      currentCommitVersion: this.currentCommitVersion(),
    };
  }

  range(request) {
    this.verifyReadVersion(request.expectedReadVersion);
    const pageLimit = validateRangeLimit(request.limit);
    const bounds = this.resolveBounds(request.begin, request.end);
    const cursorKey = request.cursorKey === null
      ? null
      : validatedKey(request.cursorKey);
    const readConflictRanges = [...bounds.selectorConflictRanges];

    if (bounds.empty) {
      return {
        operation: operation.range,
        rows: [],
        hasMore: false,
        currentCommitVersion: this.currentCommitVersion(),
        readConflictRanges: mergeConflictRanges(readConflictRanges),
      };
    }

    const query = rangeQuery(bounds, cursorKey, request.reverse);
    const metadataRows = this.all(
      `SELECT key, length(value) AS value_size FROM storagekit_kv ${query.where} ORDER BY key ${query.order} LIMIT ?`,
      ...query.bindings,
      pageLimit + 1
    );
    const selectedCount = boundedResponseRowCount(metadataRows, pageLimit);
    if (metadataRows.length > 0 && selectedCount === 0) {
      throw StorageKitWireError.resourceUnavailable(
        "A valid StorageKit value cannot fit in the range response budget"
      );
    }
    const hasMore = metadataRows.length > selectedCount;
    const rows = selectedCount === 0
      ? []
      : this.all(
        `SELECT key, value FROM storagekit_kv ${query.where} ORDER BY key ${query.order} LIMIT ?`,
        ...query.bindings,
        selectedCount
      );
    const responseRows = rows.map((row) => ({
      key: validatedKey(row.key),
      value: validatedValue(row.value),
    }));
    const scanConflictRange = pagedScanConflictRange(
      bounds,
      cursorKey,
      request.reverse,
      metadataRows,
      selectedCount,
      hasMore
    );
    if (scanConflictRange !== null) {
      readConflictRanges.push(scanConflictRange);
    }

    return {
      operation: operation.range,
      rows: responseRows,
      hasMore,
      currentCommitVersion: this.currentCommitVersion(),
      readConflictRanges: mergeConflictRanges(readConflictRanges),
    };
  }

  rangeSize(request) {
    this.verifyReadVersion(request.expectedReadVersion);
    const range = validatedOrderedRange(request.begin, request.end);
    const row = this.first(
      "SELECT COALESCE(SUM(length(key) + length(value)), 0) AS byte_count FROM storagekit_kv WHERE key >= ? AND key < ?",
      range.begin,
      range.end
    );
    return {
      operation: operation.rangeSize,
      byteCount: validatedSQLInt64(row?.byte_count ?? 0, "Range byte count"),
      currentCommitVersion: this.currentCommitVersion(),
    };
  }

  rangeSplitPoints(request) {
    this.verifyReadVersion(request.expectedReadVersion);
    const range = validatedOrderedRange(request.begin, request.end);
    const chunkSize = validatedPositiveInt64(
      request.chunkSize,
      "Split point chunk size"
    );
    const points = [new Uint8Array(range.begin)];
    if (compareBytes(range.begin, range.end) === 0) {
      return {
        operation: operation.rangeSplitPoints,
        splitPoints: points,
        currentCommitVersion: this.currentCommitVersion(),
      };
    }

    let chunkBytes = 0n;
    const rows = this.iterate(
      "SELECT key, length(key) + length(value) AS byte_count FROM storagekit_kv WHERE key >= ? AND key < ? ORDER BY key ASC",
      range.begin,
      range.end
    );
    for (const row of rows) {
      const key = validatedKey(row.key);
      const rowBytes = validatedSQLInt64(row.byte_count, "Stored row byte count");
      if (chunkBytes > 0n
          && rowBytes > chunkSize - minimumBigInt(chunkBytes, chunkSize)) {
        if (points.length >= storageKitWireLimits.maxSplitPoints - 1) {
          throw StorageKitWireError.resourceUnavailable(
            `Split point count exceeds the response limit of ${storageKitWireLimits.maxSplitPoints}`
          );
        }
        points.push(new Uint8Array(key));
        chunkBytes = 0n;
      }
      chunkBytes += rowBytes;
    }
    points.push(new Uint8Array(range.end));
    return {
      operation: operation.rangeSplitPoints,
      splitPoints: points,
      currentCommitVersion: this.currentCommitVersion(),
    };
  }

  commit(request) {
    return this.transactionSync(() => {
      const readConflictRanges = request.readConflictRanges.map(normalizeReadConflictRange);
      this.verifyReadConflicts(request.observedReadVersion, readConflictRanges);
      const explicitWriteRanges = request.writeConflictRanges.map(
        normalizeWriteConflictRange
      );
      for (const mutation of request.mutations) {
        validateMutation(mutation);
      }
      if (request.mutations.length === 0
          && explicitWriteRanges.length === 0) {
        return {
          operation: operation.commit,
          committedVersion: this.currentCommitVersion(),
        };
      }

      const currentVersion = this.currentCommitVersion();
      if (currentVersion === 0x7fff_ffff_ffff_ffffn) {
        throw StorageKitWireError.invalidOperation("Commit version exhausted Int64 capacity");
      }
      const committedVersion = currentVersion + 1n;
      const mutations = request.mutations.map((mutation) =>
        materializeMutation(mutation, committedVersion));
      const mutationRanges = mutations.map(writeConflictRange);
      const writeConflictRanges = mergeConflictRanges([
        ...mutationRanges,
        ...explicitWriteRanges,
      ]);
      if (writeConflictRanges.length > storageKitWireLimits.maxConflictRangesPerCommit) {
        throw StorageKitWireError.limitExceeded(
          "Merged write conflict range count",
          storageKitWireLimits.maxConflictRangesPerCommit
        );
      }
      for (const mutation of mutations) {
        this.applyWrite(mutation);
      }
      this.recordConflictRanges(
        coalesceConflictRangesForPersistence(
          writeConflictRanges,
          maximumPersistedConflictRangesPerCommit
        ),
        committedVersion
      );
      this.setMetadata("commitVersion", committedVersion.toString());
      this.pruneConflictRanges(committedVersion);
      return {
        operation: operation.commit,
        committedVersion,
      };
    });
  }

  applyWrite(mutation) {
    switch (mutation.tag) {
      case 1:
        this.exec(
          "INSERT OR REPLACE INTO storagekit_kv(key, value) VALUES (?, ?)",
          mutation.key,
          mutation.value
        );
        break;
      case 2:
        this.exec("DELETE FROM storagekit_kv WHERE key = ?", mutation.key);
        break;
      case 3:
        this.exec(
          "DELETE FROM storagekit_kv WHERE key >= ? AND key < ?",
          mutation.begin,
          mutation.end
        );
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
    const result = applyMutation(
      current === null ? null : validatedValue(current.value),
      mutation.param,
      mutation.mutationType
    );
    switch (result.kind) {
      case "set":
        validateValue(result.value);
        this.exec(
          "INSERT OR REPLACE INTO storagekit_kv(key, value) VALUES (?, ?)",
          mutation.key,
          result.value
        );
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
    const start = begin === null
      ? { position: "beforeAll", key: null }
      : this.resolveRangeBoundary(begin);
    const finish = end === null
      ? { position: "pastEnd", key: null }
      : this.resolveRangeBoundary(end);
    const selectorConflictRanges = [];
    const beginDependency = selectorConflictRange(begin, start);
    const endDependency = selectorConflictRange(end, finish);
    if (beginDependency !== null) {
      selectorConflictRanges.push(beginDependency);
    }
    if (endDependency !== null) {
      selectorConflictRanges.push(endDependency);
    }
    const empty = start.position === "pastEnd"
      || finish.position === "beforeAll"
      || (start.key !== null
        && finish.key !== null
        && compareBytes(start.key, finish.key) >= 0);
    return {
      empty,
      startKey: empty ? null : start.key,
      endKey: empty ? null : finish.key,
      selectorConflictRanges,
    };
  }

  resolveRangeBoundary(selector) {
    validateKey(selector.key);
    const offset = BigInt(selector.offset);
    if (offset === 1n) {
      return {
        position: "direct",
        key: selector.orEqual ? keySuccessor(selector.key) : selector.key,
      };
    }
    return this.resolveSelector(selector);
  }

  resolveSelector(selector) {
    validateKey(selector.key);
    const offset = BigInt(selector.offset);
    if (offset < -storageKitWireLimits.maxSelectorResolutionSteps
        || offset > storageKitWireLimits.maxSelectorResolutionSteps) {
      throw StorageKitWireError.limitExceeded(
        "Key selector offset",
        storageKitWireLimits.maxSelectorResolutionSteps
      );
    }
    if (offset > 0n) {
      const operator = selector.orEqual ? ">" : ">=";
      const row = this.first(
        `SELECT key FROM storagekit_kv WHERE key ${operator} ? ORDER BY key ASC LIMIT 1 OFFSET ?`,
        selector.key,
        Number(offset - 1n)
      );
      return row === null
        ? { position: "pastEnd", key: null }
        : { position: "key", key: validatedKey(row.key) };
    }
    const operator = selector.orEqual ? "<=" : "<";
    const row = this.first(
      `SELECT key FROM storagekit_kv WHERE key ${operator} ? ORDER BY key DESC LIMIT 1 OFFSET ?`,
      selector.key,
      Number(-offset)
    );
    return row === null
      ? { position: "beforeAll", key: null }
      : { position: "key", key: validatedKey(row.key) };
  }

  verifyReadVersion(expectedReadVersion) {
    if (expectedReadVersion === null || expectedReadVersion === undefined) {
      return;
    }
    const version = validatedVersion(expectedReadVersion);
    if (this.currentCommitVersion() !== version) {
      throw StorageKitWireError.transactionConflict();
    }
  }

  verifyReadConflicts(observedReadVersion, readConflictRanges) {
    const currentVersion = this.currentCommitVersion();
    if (observedReadVersion === null || observedReadVersion === undefined) {
      if (readConflictRanges.length > 0) {
        throw StorageKitWireError.invalidOperation(
          "Read conflict ranges require an observed read version"
        );
      }
      return;
    }
    const observedVersion = validatedVersion(observedReadVersion);
    if (observedVersion > currentVersion) {
      throw StorageKitWireError.transactionConflict();
    }
    if (readConflictRanges.length === 0) {
      return;
    }
    if (observedVersion < this.minimumRetainedConflictVersion()) {
      throw StorageKitWireError.transactionConflict();
    }
    for (const range of readConflictRanges) {
      if (this.hasConflictingWrite(observedVersion, range)) {
        throw StorageKitWireError.transactionConflict();
      }
    }
  }

  hasConflictingWrite(observedReadVersion, range) {
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
    return this.first(
      `SELECT 1 FROM storagekit_conflicts WHERE ${predicates.join(" AND ")} LIMIT 1`,
      ...bindings
    ) !== null;
  }

  recordConflictRanges(ranges, committedVersion) {
    if (ranges.length === 0) {
      return;
    }
    const version = splitVersion(committedVersion);
    let byteCount = 0;
    for (const range of ranges) {
      const normalized = normalizeWriteConflictRange(range);
      byteCount += normalized.begin.byteLength + normalized.end.byteLength;
      this.exec(
        "INSERT INTO storagekit_conflicts(version_hi, version_lo, begin_key, end_key) VALUES (?, ?, ?, ?)",
        version.hi,
        version.lo,
        normalized.begin,
        normalized.end
      );
    }
    this.exec(
      "INSERT INTO storagekit_conflict_versions(version_hi, version_lo, entry_count, byte_count) VALUES (?, ?, ?, ?)",
      version.hi,
      version.lo,
      ranges.length,
      byteCount
    );
    this.setMetadata(
      "conflictEntryCount",
      String(this.metadataInteger("conflictEntryCount") + ranges.length)
    );
    this.setMetadata(
      "conflictByteCount",
      String(this.metadataInteger("conflictByteCount") + byteCount)
    );
  }

  pruneConflictRanges(committedVersion) {
    const pruneThrough = committedVersion - conflictVersionWindow;
    // Prune oldest versions until the window and both retention limits hold.
    // A single-version step per commit cannot converge: one commit may add
    // maximumPersistedConflictRangesPerCommit entries while the evicted oldest
    // version held only one, so the limits would stop being hard bounds.
    while (true) {
      const retainedEntryCount = this.metadataInteger("conflictEntryCount");
      const retainedByteCount = this.metadataInteger("conflictByteCount");
      const oldest = this.first(
        "SELECT version_hi, version_lo FROM storagekit_conflict_versions ORDER BY version_hi ASC, version_lo ASC LIMIT 1"
      );
      if (oldest === null) {
        if (retainedEntryCount !== 0 || retainedByteCount !== 0) {
          throw StorageKitWireError.invalidOperation(
            "Conflict retention metadata is inconsistent"
          );
        }
        return;
      }
      const oldestVersion = joinVersion(oldest.version_hi, oldest.version_lo);
      if (oldestVersion >= committedVersion) {
        // Never prune the version recorded by the current commit.
        return;
      }
      const exceedsRetentionTarget =
        retainedEntryCount > storageKitWireLimits.maxConflictEntries
        || retainedByteCount > storageKitWireLimits.maxConflictBytes;
      if (oldestVersion > pruneThrough && !exceedsRetentionTarget) {
        return;
      }
      this.pruneConflictVersionsThrough(oldestVersion);
    }
  }

  pruneConflictVersionsThrough(version) {
    const split = splitVersion(version);
    const totals = this.first(
      "SELECT COALESCE(SUM(entry_count), 0) AS entry_count, COALESCE(SUM(byte_count), 0) AS byte_count FROM storagekit_conflict_versions WHERE version_hi < ? OR (version_hi = ? AND version_lo <= ?)",
      split.hi,
      split.hi,
      split.lo
    );
    this.exec(
      "DELETE FROM storagekit_conflicts WHERE version_hi < ? OR (version_hi = ? AND version_lo <= ?)",
      split.hi,
      split.hi,
      split.lo
    );
    this.exec(
      "DELETE FROM storagekit_conflict_versions WHERE version_hi < ? OR (version_hi = ? AND version_lo <= ?)",
      split.hi,
      split.hi,
      split.lo
    );
    this.setMetadata(
      "conflictEntryCount",
      String(Math.max(0, this.metadataInteger("conflictEntryCount") - Number(totals.entry_count)))
    );
    this.setMetadata(
      "conflictByteCount",
      String(Math.max(0, this.metadataInteger("conflictByteCount") - Number(totals.byte_count)))
    );
    if (version > this.minimumRetainedConflictVersion()) {
      this.setMetadata("minimumConflictVersion", version.toString());
    }
  }

  bindOrVerifyScope(scope) {
    const canonicalName = nameForScope(scope);
    const row = this.first(
      "SELECT value FROM storagekit_metadata WHERE key = 'scopeName'"
    );
    if (row === null) {
      this.setMetadata("scopeName", canonicalName);
      return;
    }
    if (row.value !== canonicalName) {
      throw StorageKitWireError.scopeMismatch();
    }
  }

  requireInitialized() {
    if (!this.initialized) {
      throw StorageKitWireError.invalidOperation(
        "StorageKit Durable Object has not completed migration"
      );
    }
  }

  minimumRetainedConflictVersion() {
    return BigInt(this.requireMetadata("minimumConflictVersion"));
  }

  currentCommitVersion() {
    return BigInt(this.requireMetadata("commitVersion"));
  }

  metadataInteger(key) {
    const value = Number(this.requireMetadata(key));
    if (!Number.isSafeInteger(value) || value < 0) {
      throw StorageKitWireError.invalidOperation(`Invalid ${key} metadata`);
    }
    return value;
  }

  requireMetadata(key) {
    const row = this.first("SELECT value FROM storagekit_metadata WHERE key = ?", key);
    if (row === null) {
      throw StorageKitWireError.invalidOperation("StorageKit metadata is not initialized");
    }
    return row.value;
  }

  insertMetadataIfMissing(key, value) {
    this.exec(
      "INSERT OR IGNORE INTO storagekit_metadata(key, value) VALUES (?, ?)",
      key,
      value
    );
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
    if (typeof cursor?.[Symbol.iterator] === "function") {
      return Array.from(cursor);
    }
    throw StorageKitWireError.backendContractViolation(
      "SQLite exec returned an unsupported cursor"
    );
  }

  iterate(statement, ...bindings) {
    const cursor = this.sql.exec(statement, ...bindings);
    if (Array.isArray(cursor)) {
      return cursor;
    }
    if (typeof cursor?.[Symbol.iterator] === "function") {
      return cursor;
    }
    if (typeof cursor?.toArray === "function") {
      return cursor.toArray();
    }
    throw StorageKitWireError.backendContractViolation(
      "SQLite exec returned an unsupported cursor"
    );
  }

  exec(statement, ...bindings) {
    this.sql.exec(statement, ...bindings);
  }
}

function isMissingMetadataTable(error) {
  return typeof error === "object"
    && error !== null
    && "message" in error
    && typeof error.message === "string"
    && /no such table:\s*(?:main\.)?storagekit_metadata\b/i.test(error.message);
}

function rangeQuery(bounds, cursorKey, reverse) {
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
  if (cursorKey !== null) {
    predicates.push(reverse ? "key < ?" : "key > ?");
    bindings.push(cursorKey);
  }
  return {
    where: predicates.length === 0 ? "" : `WHERE ${predicates.join(" AND ")}`,
    order: reverse ? "DESC" : "ASC",
    bindings,
  };
}

function boundedResponseRowCount(rows, requestedLimit) {
  let byteCount = responseOverheadReserve;
  let count = 0;
  for (const row of rows) {
    if (count >= requestedLimit) {
      break;
    }
    const key = validatedKey(row.key);
    const valueSize = Number(row.value_size);
    if (!Number.isSafeInteger(valueSize)
        || valueSize < 0
        || valueSize > storageKitWireLimits.maxValueBytes) {
      throw StorageKitWireError.limitExceeded(
        "Stored value bytes",
        storageKitWireLimits.maxValueBytes
      );
    }
    const rowBytes = 8 + key.byteLength + valueSize;
    if (byteCount + rowBytes > storageKitWireLimits.maxRangeResponseBytes) {
      break;
    }
    byteCount += rowBytes;
    count += 1;
  }
  return count;
}

function pagedScanConflictRange(
  bounds,
  cursorKey,
  reverse,
  metadataRows,
  selectedCount,
  hasMore
) {
  let begin = bounds.startKey;
  let end = bounds.endKey;
  if (cursorKey !== null) {
    if (reverse) {
      end = minimumBound(end, cursorKey);
    } else {
      begin = maximumBound(begin, keySuccessor(cursorKey));
    }
  }
  if (hasMore) {
    const probeKey = validatedKey(metadataRows[selectedCount].key);
    if (reverse) {
      begin = maximumBound(begin, probeKey);
    } else {
      end = minimumBound(end, keySuccessor(probeKey));
    }
  }
  return normalizedOptionalReadConflictRange({ begin, end });
}

function selectorConflictRange(selector, resolved) {
  if (selector === null) {
    return null;
  }
  const offset = BigInt(selector.offset);
  if (offset === 1n) {
    return null;
  }
  if (offset > 0n) {
    const begin = selector.orEqual ? keySuccessor(selector.key) : selector.key;
    const end = resolved.key === null ? null : keySuccessor(resolved.key);
    return normalizedOptionalReadConflictRange({ begin, end });
  }
  const begin = resolved.key;
  const end = selector.orEqual ? keySuccessor(selector.key) : selector.key;
  return normalizedOptionalReadConflictRange({ begin, end });
}

function validateRangeLimit(limit) {
  if (!Number.isInteger(limit)
      || limit <= 0
      || limit > storageKitWireLimits.maxRangeLimit) {
    throw StorageKitWireError.limitExceeded(
      "Range limit",
      storageKitWireLimits.maxRangeLimit
    );
  }
  return limit;
}

function validateMutation(mutation) {
  switch (mutation.tag) {
    case 1:
      validateKey(mutation.key);
      validateValue(mutation.value);
      return;
    case 2:
      validateKey(mutation.key);
      return;
    case 3:
      validatedBoundary(mutation.begin);
      validatedBoundary(mutation.end);
      normalizeWriteConflictRange({ begin: mutation.begin, end: mutation.end });
      return;
    case 4:
      if (mutation.mutationType === mutationType.setVersionstampedKey) {
        validateByteLimit(
          mutation.key,
          storageKitWireLimits.maxVersionstampedKeyOperandBytes,
          "Versionstamped key operand"
        );
      } else {
        validateKey(mutation.key);
      }
      if (mutation.mutationType === mutationType.setVersionstampedValue) {
        validateByteLimit(
          mutation.param,
          storageKitWireLimits.maxVersionstampedValueOperandBytes,
          "Versionstamped value operand"
        );
      } else {
        validateValue(mutation.param);
      }
      return;
    default:
      throw StorageKitWireError.unknownWriteOperation(mutation.tag);
  }
}

function writeConflictRange(mutation) {
  switch (mutation.tag) {
    case 1:
    case 2:
    case 4:
      return singleKeyRange(mutation.key);
    case 3:
      return normalizeWriteConflictRange({ begin: mutation.begin, end: mutation.end });
    default:
      throw StorageKitWireError.unknownWriteOperation(mutation.tag);
  }
}

function singleKeyRange(key) {
  return { begin: key, end: keySuccessor(key) };
}

function keySuccessor(key) {
  const end = new Uint8Array(key.length + 1);
  end.set(key, 0);
  return end;
}

function normalizeWriteConflictRange(range) {
  if (range?.begin === null
      || range?.begin === undefined
      || range?.end === null
      || range?.end === undefined) {
    throw StorageKitWireError.invalidOperation("Write conflict range must be bounded");
  }
  const begin = validatedBoundary(range.begin);
  const end = validatedBoundary(range.end);
  if (compareBytes(begin, end) >= 0) {
    throw StorageKitWireError.invalidOperation(
      "Write conflict range must be non-empty and ordered"
    );
  }
  return { begin, end };
}

function normalizeReadConflictRange(range) {
  const begin = range?.begin === null || range?.begin === undefined
    ? null
    : validatedBoundary(range.begin);
  const end = range?.end === null || range?.end === undefined
    ? null
    : validatedBoundary(range.end);
  if (begin !== null && end !== null && compareBytes(begin, end) >= 0) {
    throw StorageKitWireError.invalidOperation(
      "Read conflict range must be non-empty and ordered"
    );
  }
  return { begin, end };
}

function normalizedOptionalReadConflictRange(range) {
  if (range.begin !== null
      && range.end !== null
      && compareBytes(range.begin, range.end) >= 0) {
    return null;
  }
  return normalizeReadConflictRange(range);
}

function mergeConflictRanges(ranges) {
  if (ranges.length === 0) {
    return [];
  }
  const normalized = ranges.map(normalizeReadConflictRange).sort(compareRangeBegins);
  const merged = [];
  for (const range of normalized) {
    const previous = merged[merged.length - 1];
    if (previous === undefined || !rangesOverlapOrTouch(previous, range)) {
      merged.push({ begin: range.begin, end: range.end });
      continue;
    }
    previous.end = maximumEnd(previous.end, range.end);
  }
  return merged;
}

function coalesceConflictRangesForPersistence(ranges, maximumRangeCount) {
  if (ranges.length <= maximumRangeCount) {
    return ranges;
  }
  const rangesPerGroup = Math.ceil(ranges.length / maximumRangeCount);
  const coalesced = [];
  for (let start = 0; start < ranges.length; start += rangesPerGroup) {
    const first = ranges[start];
    const last = ranges[Math.min(start + rangesPerGroup, ranges.length) - 1];
    coalesced.push({ begin: first.begin, end: last.end });
  }
  return coalesced;
}

function compareRangeBegins(left, right) {
  if (left.begin === null) {
    return right.begin === null ? 0 : -1;
  }
  if (right.begin === null) {
    return 1;
  }
  return compareBytes(left.begin, right.begin);
}

function rangesOverlapOrTouch(left, right) {
  if (left.end === null || right.begin === null) {
    return true;
  }
  return compareBytes(right.begin, left.end) <= 0;
}

function maximumEnd(left, right) {
  if (left === null || right === null) {
    return null;
  }
  return compareBytes(left, right) >= 0 ? left : right;
}

function minimumBound(left, right) {
  if (left === null) {
    return right;
  }
  return compareBytes(left, right) <= 0 ? left : right;
}

function maximumBound(left, right) {
  if (left === null) {
    return right;
  }
  return compareBytes(left, right) >= 0 ? left : right;
}

function validatedVersion(version) {
  const value = BigInt(version);
  if (value < 0n || value > 0x7fff_ffff_ffff_ffffn) {
    throw StorageKitWireError.invalidOperation("Version must be a non-negative Int64");
  }
  return value;
}

function validatedPositiveInt64(value, field) {
  let result;
  try {
    result = BigInt(value);
  } catch {
    throw StorageKitWireError.invalidOperation(
      `${field} must be a positive Int64`
    );
  }
  if (result <= 0n || result > 0x7fff_ffff_ffff_ffffn) {
    throw StorageKitWireError.invalidOperation(
      `${field} must be a positive Int64`
    );
  }
  return result;
}

function validatedSQLInt64(value, field) {
  let result;
  try {
    result = BigInt(value);
  } catch {
    throw StorageKitWireError.invalidOperation(
      `SQLite returned an invalid ${field}`
    );
  }
  if (result < 0n || result > 0x7fff_ffff_ffff_ffffn) {
    throw StorageKitWireError.invalidOperation(
      `SQLite returned an invalid ${field}`
    );
  }
  return result;
}

function validatedOrderedRange(beginValue, endValue) {
  const begin = validatedBoundary(beginValue);
  const end = validatedBoundary(endValue);
  if (compareBytes(begin, end) > 0) {
    throw StorageKitWireError.invalidOperation(
      "Range boundaries are not ordered"
    );
  }
  return { begin, end };
}

function minimumBigInt(left, right) {
  return left <= right ? left : right;
}

function splitVersion(version) {
  const value = validatedVersion(version);
  return {
    hi: Number((value >> 32n) & 0xffff_ffffn),
    lo: Number(value & 0xffff_ffffn),
  };
}

function joinVersion(hi, lo) {
  return (BigInt(hi) << 32n) | BigInt(lo);
}

function validateKey(value) {
  const bytes = byteView(value);
  if (bytes.byteLength > storageKitWireLimits.maxKeyBytes) {
    throw StorageKitWireError.limitExceeded(
      "Key bytes",
      storageKitWireLimits.maxKeyBytes
    );
  }
}

function validateValue(value) {
  const bytes = byteView(value);
  if (bytes.byteLength > storageKitWireLimits.maxValueBytes) {
    throw StorageKitWireError.limitExceeded(
      "Value bytes",
      storageKitWireLimits.maxValueBytes
    );
  }
}

function validateByteLimit(value, maximum, field) {
  const bytes = byteView(value);
  if (bytes.byteLength > maximum) {
    throw StorageKitWireError.limitExceeded(field, maximum);
  }
}

function validatedKey(value) {
  const bytes = byteView(value);
  validateKey(bytes);
  return bytes;
}

function validatedValue(value) {
  const bytes = byteView(value);
  validateValue(bytes);
  return bytes;
}

function validatedBoundary(value) {
  const bytes = byteView(value);
  if (bytes.byteLength > storageKitWireLimits.maxBoundaryBytes) {
    throw StorageKitWireError.limitExceeded(
      "Boundary bytes",
      storageKitWireLimits.maxBoundaryBytes
    );
  }
  return bytes;
}

function byteView(value) {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw StorageKitWireError.invalidOperation("SQLite returned a non-binary value");
}
