import assert from "node:assert/strict";
import test from "node:test";
import { StorageKitWireWriter } from "../src/StorageKitWireWriter.js";
import {
  nameForPartitionIdentity,
  StorageKitDurableObjectHost,
} from "../src/index.js";
import { StorageKitWire } from "../src/StorageKitWire.js";
import { mutationType, operation, protocolVersion, statusCode } from "../src/StorageKitWireConstants.js";
import { storageKitWireLimits } from "../src/StorageKitWireLimits.js";
import { StorageKitSQLiteStore } from "../src/StorageKitSQLiteStore.js";
import { InMemorySQLiteStorage } from "./InMemorySQLiteStorage.js";

const partitionIdentity = Object.freeze({
  databaseID: "main",
  tenantID: null,
  workspaceID: null,
});

test("canonical StorageKit wire starts at protocol version one", () => {
  assert.equal(protocolVersion, 1);
});

test("partition identity name encoding matches the StorageKit v1 canonical format", () => {
  assert.equal(
    nameForPartitionIdentity({ databaseID: "main", tenantID: "tenant-a", workspaceID: "workspace-a" }),
    "storage-kit/cfdo/v1/database/bWFpbg/tenant/dGVuYW50LWE/workspace/d29ya3NwYWNlLWE"
  );
});

test("partition identity preserves exact UTF-8 identity", () => {
  assert.notEqual(
    nameForPartitionIdentity({
      databaseID: "caf\u{00E9}",
      tenantID: null,
      workspaceID: null,
    }),
    nameForPartitionIdentity({
      databaseID: "cafe\u{0301}",
      tenantID: null,
      workspaceID: null,
    })
  );
});

test("partition identity rejects non-scalar JavaScript strings", () => {
  assert.throws(
    () => nameForPartitionIdentity({
      databaseID: "\ud800",
      tenantID: null,
      workspaceID: null,
    }),
    /partition identity/i
  );
});

test("partition identity rejects non-object input as a typed wire error", () => {
  assert.throws(
    () => nameForPartitionIdentity(null),
    /partition identity/i
  );
  assert.throws(
    () => nameForPartitionIdentity([]),
    /partition identity/i
  );
});

test("host dispatch rejects trailing bytes as a typed failure", () => {
  const host = makeHost();
  const bytes = StorageKitWire.encodeRequest({
    operation: operation.readiness,
    partitionIdentity,
  });
  const response = StorageKitWire.decodeResponse(host.dispatchBytes([...bytes, 0xff]));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Trailing bytes/);
});

test("host dispatch rejects invalid UTF-8 as a typed failure", () => {
  const host = makeHost();
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(operation.readiness);
  writer.writeBytes(new Uint8Array([0xff]));
  writer.writeBool(false);
  writer.writeBool(false);
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(writer.toBytes()));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Invalid UTF-8/);
});

test("host dispatch rejects invalid bool as a typed failure", () => {
  const host = makeHost();
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(operation.read);
  writer.writeString("main");
  writer.writeBool(false);
  writer.writeBool(false);
  writer.writeBytes(bytes(0x01));
  writer.writeUInt8(2);
  writer.writeBool(false);
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(writer.toBytes()));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Invalid bool/);
});

test("host dispatch rejects removed operation 7 as a typed failure", () => {
  const host = makeHost();
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(new Uint8Array([
    protocolVersion,
    0x07,
  ])));

  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Unknown operation: 7/);
});

test("migration and CRUD do not depend on unavailable SQLite PRAGMAs", () => {
  const sql = new RejectingPragmaSqlStorage();
  const host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );

  host.migrate();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0x41) },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  });

  assert.deepEqual([...readValue(host, 0x01)], [0x41]);
});

test("migration reopens an initialized store without SQL writes", () => {
  const sql = new InMemorySQLiteStorage();
  const firstHost = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  firstHost.migrate();
  send(firstHost, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0x41) },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  });

  const readOnlySQL = {
    exec(statement, ...bindings) {
      if (!/^\s*SELECT\b/i.test(statement)) {
        throw new Error(`Unexpected SQL write during restart: ${statement}`);
      }
      if (/\bIN\s*\(/i.test(statement)) {
        throw new Error(`Unexpected temporary table query during restart: ${statement}`);
      }
      if (/\bsqlite_schema\b/i.test(statement)) {
        throw new Error(`Unexpected internal schema query during restart: ${statement}`);
      }
      return sql.exec(statement, ...bindings);
    },
  };
  const restartedHost = new StorageKitDurableObjectHost(
    readOnlySQL,
    () => {
      throw new Error("Unexpected migration transaction during restart");
    }
  );

  restartedHost.migrate();

  assert.deepEqual([...readValue(restartedHost, 0x01)], [0x41]);
});

test("migration preserves metadata inspection failures", () => {
  const expected = new Error("Exceeded allowed rows written in Durable Objects free tier.");
  const host = new StorageKitDurableObjectHost(
    {
      exec() {
        throw expected;
      },
    },
    () => {
      throw new Error("Unexpected migration transaction after inspection failure");
    }
  );

  assert.throws(() => host.migrate(), (error) => error === expected);
});

test("response decoder rejects an unknown status", () => {
  assert.throws(
    () => StorageKitWire.decodeResponse(new Uint8Array([protocolVersion, 0xff])),
    /Unknown status/
  );
});

test("request decoder rejects oversized collections before reading elements", () => {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(operation.commit);
  writer.writeString("main");
  writer.writeBool(false);
  writer.writeBool(false);
  writer.writeBool(false);
  writer.writeUInt32(10_001);

  const host = makeHost();
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(writer.toBytes()));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Mutation count/);
});

test("wire accepts physical mutation batches above the former database-level ceiling", () => {
  const request = {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: Array.from(
      { length: 1_001 },
      (_, index) => ({
        tag: 2,
        key: bytes((index >>> 8) & 0xff, index & 0xff),
      }),
    ),
    readConflictRanges: [],
    writeConflictRanges: [],
  };

  const decoded = StorageKitWire.decodeRequest(
    StorageKitWire.encodeRequest(request),
  );
  assert.equal(decoded.mutations.length, request.mutations.length);

  const host = makeHost();
  const response = StorageKitWire.decodeResponse(
    host.dispatchBytes(StorageKitWire.encodeRequest(request)),
  );
  assert.equal(response.status, statusCode.ok);
  assert.equal(response.committedVersion, 1n);
});

test("request encoder rejects oversized keys and values", () => {
  assert.throws(() => StorageKitWire.encodeRequest({
    operation: operation.read,
    partitionIdentity,
    key: new Uint8Array(storageKitWireLimits.maxKeyBytes + 1),
    snapshot: false,
    expectedReadVersion: null,
  }), /Key bytes/);

  assert.throws(() => StorageKitWire.encodeRequest({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      {
        tag: 1,
        key: bytes(0x01),
        value: new Uint8Array(storageKitWireLimits.maxValueBytes + 1),
      },
    ],
  }), /Value bytes/);
});

test("wire enforces the exact stored key and value combined limit", () => {
  const half = Math.floor(storageKitWireLimits.maxStoredKeyValueBytes / 2);
  const accepted = {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [{
      tag: 1,
      key: new Uint8Array(half),
      value: new Uint8Array(storageKitWireLimits.maxStoredKeyValueBytes - half),
    }],
    readConflictRanges: [],
    writeConflictRanges: [],
  };
  assert.equal(
    StorageKitWire.decodeRequest(StorageKitWire.encodeRequest(accepted))
      .mutations.length,
    1
  );

  assert.throws(() => StorageKitWire.encodeRequest({
    ...accepted,
    mutations: [{
      tag: 1,
      key: new Uint8Array(half),
      value: new Uint8Array(
        storageKitWireLimits.maxStoredKeyValueBytes - half + 1
      ),
    }],
  }), /Stored key and value bytes/);
});

test("SQLite store rejects a versionstamped result above the combined limit", () => {
  const sql = new InMemorySQLiteStorage();
  const store = new StorageKitSQLiteStore(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  store.migrate();
  const key = new Uint8Array(1_000_000);
  const valueOperand = versionstampOperand(
    new Uint8Array(999_991),
    new Uint8Array()
  );

  assert.throws(() => store.dispatch({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 0n,
    mutations: [{
      tag: 4,
      key,
      param: valueOperand,
      mutationType: mutationType.setVersionstampedValue,
    }],
    readConflictRanges: [],
    writeConflictRanges: [],
  }), /Stored key and value bytes/);
  assert.equal(store.currentCommitVersion(), 0n);
});

test("host dispatch rejects an oversized raw range cursor as a typed failure", () => {
  const host = makeHost();
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(rawRangeRequestWithCursor(
    new Uint8Array(storageKitWireLimits.maxKeyBytes + 1),
    {
      operation: operation.range,
      partitionIdentity,
      begin: firstGreaterOrEqual(bytes(0x01)),
      end: firstGreaterThan(bytes(0x01)),
      limit: 1,
      reverse: false,
      snapshot: false,
      expectedReadVersion: 0n,
      cursorKey: null,
    }
  )));

  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Key bytes/);
});

test("set, atomic, read, and commit persistence round trip through StorageKit Wire dispatch", () => {
  const host = makeHost();

  let response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(10) },
    ],
  });
  assert.equal(response.committedVersion, 1n);

  response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 1n,
    mutations: [
      { tag: 4, key: bytes(0x01), param: bytes(5), mutationType: mutationType.add },
    ],
  });
  assert.equal(response.committedVersion, 2n);

  response = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 2n,
  });
  assert.deepEqual([...response.value], [15]);
  assert.equal(response.currentCommitVersion, 2n);
});

test("clearRange removes committed keys using begin-inclusive end-exclusive bounds", () => {
  const host = makeHost();

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x02), value: bytes(2) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
      { tag: 1, key: bytes(0x04), value: bytes(4) },
      { tag: 3, begin: bytes(0x02), end: bytes(0x04) },
    ],
  });

  const response = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: firstGreaterOrEqual(bytes(0x01)),
    end: firstGreaterThan(bytes(0x04)),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });

  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x04]]);
});

test("atomic mutation semantics cover bitwise unsigned max min and compareAndClear", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0b1010) },
      { tag: 4, key: bytes(0x01), param: bytes(0b0101), mutationType: mutationType.bitOr },
      { tag: 1, key: bytes(0x02), value: bytes(0b1111) },
      { tag: 4, key: bytes(0x02), param: bytes(0b0110), mutationType: mutationType.bitAnd },
      { tag: 1, key: bytes(0x03), value: bytes(0b1010) },
      { tag: 4, key: bytes(0x03), param: bytes(0b0011), mutationType: mutationType.bitXor },
      { tag: 1, key: bytes(0x04), value: bytes(0x00, 0x02) },
      { tag: 4, key: bytes(0x04), param: bytes(0xff, 0x01), mutationType: mutationType.max },
      { tag: 1, key: bytes(0x05), value: bytes(0x00, 0x02) },
      { tag: 4, key: bytes(0x05), param: bytes(0xff, 0x01), mutationType: mutationType.min },
      { tag: 1, key: bytes(0x06), value: bytes(0x09) },
      { tag: 4, key: bytes(0x06), param: bytes(0x09), mutationType: mutationType.compareAndClear },
    ],
  });

  assert.deepEqual([...readValue(host, 0x01)], [0b1111]);
  assert.deepEqual([...readValue(host, 0x02)], [0b0110]);
  assert.deepEqual([...readValue(host, 0x03)], [0b1001]);
  assert.deepEqual([...readValue(host, 0x04)], [0x00, 0x02]);
  assert.deepEqual([...readValue(host, 0x05)], [0xff, 0x01]);
  assert.equal(readValue(host, 0x06), null);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 1n,
    mutations: [
      { tag: 1, key: bytes(0x07), value: bytes(0x80) },
      { tag: 4, key: bytes(0x07), param: bytes(0x7f), mutationType: mutationType.max },
      { tag: 1, key: bytes(0x08), value: bytes(0x80) },
      { tag: 4, key: bytes(0x08), param: bytes(0x7f), mutationType: mutationType.min },
    ],
  });
  assert.deepEqual([...readValue(host, 0x07)], [0x80]);
  assert.deepEqual([...readValue(host, 0x08)], [0x7f]);
});

test("versionstamped key and value materialize atomically and survive restart", () => {
  const sql = new InMemorySQLiteStorage();
  let host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();
  const firstStamp = commitVersionstamp(1n);
  const versionstampedKey = versionstampOperand(
    bytes(0x10),
    bytes(0x01)
  );
  const versionstampedValue = versionstampOperand(
    bytes(0xaa),
    bytes(0xbb)
  );

  const firstCommit = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 0n,
    mutations: [
      {
        tag: 4,
        key: versionstampedKey,
        param: bytes(0x41),
        mutationType: mutationType.setVersionstampedKey,
      },
      {
        tag: 4,
        key: bytes(0x20),
        param: versionstampedValue,
        mutationType: mutationType.setVersionstampedValue,
      },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  });
  assert.equal(firstCommit.committedVersion, 1n);

  const materializedKey = concatenate(
    bytes(0x10),
    firstStamp,
    bytes(0x01)
  );
  assert.deepEqual(
    [...read(host, materializedKey)],
    [0x41]
  );
  assert.deepEqual(
    [...readValue(host, 0x20)],
    [...concatenate(bytes(0xaa), firstStamp, bytes(0xbb))]
  );

  host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();
  const secondStamp = commitVersionstamp(2n);
  const secondCommit = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 1n,
    mutations: [
      {
        tag: 4,
        key: versionstampOperand(bytes(0x30), new Uint8Array()),
        param: bytes(0x42),
        mutationType: mutationType.setVersionstampedKey,
      },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  });
  assert.equal(secondCommit.committedVersion, 2n);
  assert.deepEqual(
    [...read(host, concatenate(bytes(0x30), secondStamp))],
    [0x42]
  );
});

test("failed mutation batch rolls back values versions and conflict history", () => {
  const sql = new InMemorySQLiteStorage();
  const host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();

  const response = StorageKitWire.decodeResponse(
    host.dispatchBytes(
      StorageKitWire.encodeRequest({
        operation: operation.commit,
        partitionIdentity,
        observedReadVersion: 0n,
        mutations: [
          { tag: 1, key: bytes(0x08), value: bytes(8) },
          {
            tag: 4,
            key: bytes(0x09),
            param: bytes(0x01),
            mutationType: mutationType.setVersionstampedValue,
          },
        ],
        readConflictRanges: [],
        writeConflictRanges: [],
      })
    )
  );

  assert.equal(response.status, statusCode.invalidOperation);
  assert.equal(readValue(host, 0x08), null);
  const readiness = send(host, {
    operation: operation.readiness,
    partitionIdentity,
  });
  assert.equal(readiness.commitVersion, 0n);
  assert.equal(
    sql.exec("SELECT COUNT(*) AS count FROM storagekit_conflicts")[0].count,
    0
  );
  assert.equal(
    sql.exec("SELECT COUNT(*) AS count FROM storagekit_conflict_versions")[0]
      .count,
    0
  );
});

test("range size and split points use exact stored bytes and include endpoints", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0x10, 0x11) },
      { tag: 1, key: bytes(0x02), value: bytes(0x20, 0x21) },
      { tag: 1, key: bytes(0x03), value: bytes(0x30, 0x31, 0x32, 0x33) },
    ],
  });

  const size = send(host, {
    operation: operation.rangeSize,
    partitionIdentity,
    begin: bytes(0x01),
    end: bytes(0x04),
    expectedReadVersion: 1n,
  });
  assert.equal(size.byteCount, 11n);
  assert.equal(size.currentCommitVersion, 1n);

  const split = send(host, {
    operation: operation.rangeSplitPoints,
    partitionIdentity,
    begin: bytes(0x01),
    end: bytes(0x04),
    chunkSize: 6n,
    expectedReadVersion: 1n,
  });
  assert.deepEqual(
    split.splitPoints.map((point) => [...point]),
    [[0x01], [0x03], [0x04]]
  );
  assert.equal(split.currentCommitVersion, 1n);

  const empty = send(host, {
    operation: operation.rangeSplitPoints,
    partitionIdentity,
    begin: bytes(0x04),
    end: bytes(0x04),
    chunkSize: 1n,
    expectedReadVersion: 1n,
  });
  assert.deepEqual(empty.splitPoints.map((point) => [...point]), [[0x04]]);
});

test("range metric requests reject invalid bounds chunk sizes and stale versions", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0x10) },
    ],
  });

  assert.throws(() => StorageKitWire.encodeRequest({
    operation: operation.rangeSize,
    partitionIdentity,
    begin: bytes(0x02),
    end: bytes(0x01),
    expectedReadVersion: 1n,
  }), /not ordered/);
  assert.throws(() => StorageKitWire.encodeRequest({
    operation: operation.rangeSplitPoints,
    partitionIdentity,
    begin: bytes(0x01),
    end: bytes(0x02),
    chunkSize: 0n,
    expectedReadVersion: 1n,
  }), /positive/);

  const stale = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.rangeSize,
      partitionIdentity,
      begin: bytes(0x01),
      end: bytes(0x02),
      expectedReadVersion: 0n,
    })
  ));
  assert.equal(stale.status, statusCode.transactionConflict);
});

test("range size response decoder rejects negative byte counts", () => {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(statusCode.ok);
  writer.writeUInt8(operation.rangeSize);
  writer.writeInt64(-1n);
  writer.writeInt64(0n);

  assert.throws(
    () => StorageKitWire.decodeResponse(writer.toBytes()),
    /Range byte count must be a non-negative Int64/
  );
});

test("range split response decoder rejects invalid point ordering", () => {
  const cases = [
    {
      points: [],
      message: /must include the requested range boundaries/,
    },
    {
      points: [bytes(0x01), bytes(0x01)],
      message: /must be strictly ordered/,
    },
    {
      points: [bytes(0x02), bytes(0x01)],
      message: /must be strictly ordered/,
    },
  ];

  for (const item of cases) {
    assert.throws(
      () => StorageKitWire.decodeResponse(rawSplitPointsResponse(item.points)),
      item.message
    );
  }
});

test("range split response decoder rejects oversized count before element decode", () => {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(statusCode.ok);
  writer.writeUInt8(operation.rangeSplitPoints);
  writer.writeUInt32(10_001);

  assert.throws(
    () => StorageKitWire.decodeResponse(writer.toBytes()),
    /Split point count exceeds the configured limit of 10000/
  );
});

test("range split response decoder rejects a truncated point collection", () => {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(statusCode.ok);
  writer.writeUInt8(operation.rangeSplitPoints);
  writer.writeUInt32(1);

  assert.throws(
    () => StorageKitWire.decodeResponse(writer.toBytes()),
    /Truncated StorageKit Wire frame/
  );
});

test("range metric dispatch rejects raw zero chunks and reversed bounds", () => {
  const host = makeHost();
  const cases = [
    {
      request: rawRangeMetricRequest({
        operation: operation.rangeSplitPoints,
        begin: bytes(0x01),
        end: bytes(0x02),
        chunkSize: 0n,
      }),
      message: /Split point chunk size must be positive/,
    },
    {
      request: rawRangeMetricRequest({
        operation: operation.rangeSize,
        begin: bytes(0x02),
        end: bytes(0x01),
      }),
      message: /Range boundaries are not ordered/,
    },
    {
      request: rawRangeMetricRequest({
        operation: operation.rangeSplitPoints,
        begin: bytes(0x02),
        end: bytes(0x01),
        chunkSize: 1n,
      }),
      message: /Range boundaries are not ordered/,
    },
  ];

  for (const item of cases) {
    assert.throws(
      () => StorageKitWire.decodeRequest(item.request),
      item.message
    );
    const response = StorageKitWire.decodeResponse(
      host.dispatchBytes(item.request)
    );
    assert.equal(response.status, statusCode.invalidOperation);
    assert.match(response.message, item.message);
  }
});

test("host construction requires a synchronous transaction executor", () => {
  const sql = new InMemorySQLiteStorage();
  assert.throws(
    () => new StorageKitDurableObjectHost(sql),
    /transaction executor/
  );
});

test("snapshot reads do not participate in commit conflict", () => {
  const host = makeHost();
  const snapshot = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x01),
    snapshot: true,
    expectedReadVersion: 0n,
  });
  assert.equal(snapshot.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
  });
  assert.equal(response.committedVersion, 2n);
});

test("snapshot reads still enforce a pinned read version", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.read,
      partitionIdentity,
      key: bytes(0x01),
      snapshot: true,
      expectedReadVersion: 0n,
    })
  ));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("commit rejects future versions and read conflicts without a version", () => {
  const host = makeHost();
  let response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: 1n,
      mutations: [
        { tag: 1, key: bytes(0x02), value: bytes(2) },
      ],
      readConflictRanges: [singleKeyRange(bytes(0x01))],
    })
  ));
  assert.equal(response.status, statusCode.transactionConflict);

  response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: null,
      mutations: [
        { tag: 1, key: bytes(0x02), value: bytes(2) },
      ],
      readConflictRanges: [singleKeyRange(bytes(0x01))],
    })
  ));
  assert.equal(response.status, statusCode.invalidOperation);
});

test("empty commit validates reads without advancing the commit version", () => {
  const host = makeHost();
  const response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: 0n,
    mutations: [],
    readConflictRanges: [],
    writeConflictRanges: [],
  });
  assert.equal(response.committedVersion, 0n);
  assert.equal(send(host, { operation: operation.readiness, partitionIdentity }).commitVersion, 0n);
});

test("one Durable Object rejects a different persisted partitionIdentity", () => {
  const host = makeHost();
  send(host, { operation: operation.readiness, partitionIdentity });
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.readiness,
      partitionIdentity: { databaseID: "other", tenantID: null, workspaceID: null },
    })
  ));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /does not match/);
});

test("non-snapshot read conflict range detects conflicting commit", () => {
  const host = makeHost();
  const firstRead = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });
  assert.equal(firstRead.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(StorageKitWire.encodeRequest({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: firstRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
    readConflictRanges: [
      singleKeyRange(bytes(0x01)),
    ],
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("range read conflict range catches inserts into selector gaps", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: firstGreaterOrEqual(bytes(0x10)),
    end: firstGreaterOrEqual(bytes(0x20)),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });
  assert.deepEqual(rangeRead.rows.map((row) => [...row.key]), [[0x15]]);
  assert.deepEqual([...onlyConflictRange(rangeRead).begin], [0x10]);
  assert.deepEqual([...onlyConflictRange(rangeRead).end], [0x20]);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x12), value: bytes(12) },
    ],
    readConflictRanges: [],
  });

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(StorageKitWire.encodeRequest({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x30), value: bytes(30) },
    ],
    readConflictRanges: rangeRead.readConflictRanges,
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("selector dependency conflicts when begin and end collapse on an inserted key", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
      { tag: 1, key: bytes(0x04), value: bytes(4) },
    ],
  });
  const rangeRead = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: lastLessOrEqual(bytes(0x03)),
    end: firstGreaterOrEqual(bytes(0x03)),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });
  assert.deepEqual(rangeRead.rows.map((row) => [...row.key]), [[0x02]]);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x03), value: bytes(3) },
    ],
  });
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: rangeRead.currentCommitVersion,
      mutations: [
        { tag: 1, key: bytes(0x30), value: bytes(30) },
      ],
      readConflictRanges: rangeRead.readConflictRanges,
    })
  ));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("range read conflict range does not catch writes after direct end selector", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: firstGreaterOrEqual(bytes(0x10)),
    end: firstGreaterOrEqual(bytes(0x20)),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });
  assert.deepEqual([...onlyConflictRange(rangeRead).begin], [0x10]);
  assert.deepEqual([...onlyConflictRange(rangeRead).end], [0x20]);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x30), value: bytes(30) },
    ],
    readConflictRanges: [],
  });

  const response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x40), value: bytes(40) },
    ],
    readConflictRanges: rangeRead.readConflictRanges,
  });
  assert.equal(response.committedVersion, 3n);
});

test("range read conflict range includes exact key for firstGreaterThan end selector", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: firstGreaterOrEqual(bytes(0x01)),
    end: firstGreaterThan(bytes(0x03)),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });
  assert.deepEqual(rangeRead.rows.map((row) => [...row.key]), [[0x01], [0x03]]);
  assert.deepEqual([...onlyConflictRange(rangeRead).end], [0x03, 0x00]);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x03), value: bytes(33) },
    ],
    readConflictRanges: [],
  });

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(StorageKitWire.encodeRequest({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x04), value: bytes(4) },
    ],
    readConflictRanges: rangeRead.readConflictRanges,
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("old conflict entries are pruned and stale readers conflict", () => {
  const sql = new InMemorySQLiteStorage();
  const host = new StorageKitDurableObjectHost(sql, (operation) => sql.transactionSync(operation));
  host.migrate();
  const initialRead = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });

  for (let index = 0; index < 4100; index += 1) {
    send(host, {
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: null,
      mutations: [
        { tag: 1, key: bytes(0x80, index & 0xff), value: bytes(index & 0xff) },
      ],
      readConflictRanges: [],
    });
  }

  const rows = sql.exec("SELECT COUNT(*) AS count FROM storagekit_conflicts");
  assert.ok(rows[0].count <= 4096);

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(StorageKitWire.encodeRequest({
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: initialRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
    readConflictRanges: [
      singleKeyRange(bytes(0x01)),
    ],
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("large commits persist a bounded conservative conflict history", () => {
  const sql = new InMemorySQLiteStorage();
  const host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();
  const initialRead = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x40, 0x80),
    snapshot: false,
    expectedReadVersion: null,
  });
  const mutations = Array.from({ length: 300 }, (_, index) => ({
    tag: 1,
    key: bytes(0x40, index >>> 8, index & 0xff),
    value: bytes(index & 0xff),
  }));

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations,
    readConflictRanges: [],
  });

  const versions = sql.exec(
    "SELECT entry_count FROM storagekit_conflict_versions WHERE version_hi = 0 AND version_lo = 1"
  );
  assert.equal(versions.length, 1);
  assert.ok(versions[0].entry_count <= 256);
  assert.ok(versions[0].entry_count < mutations.length);

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: initialRead.currentCommitVersion,
      mutations: [
        { tag: 1, key: bytes(0x50), value: bytes(0x50) },
      ],
      readConflictRanges: [
        singleKeyRange(bytes(0x40, 0x00, 0x80)),
      ],
    })
  ));
  assert.equal(response.status, statusCode.transactionConflict);
});

function makeHostWithPlantedConflictHistory({ perVersionByteCount, entryCountMetadata, byteCountMetadata }) {
  const sql = new InMemorySQLiteStorage();
  const host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();
  for (let version = 1; version <= 3; version += 1) {
    sql.exec(
      "INSERT INTO storagekit_conflicts(version_hi, version_lo, begin_key, end_key) VALUES (0, ?, ?, ?)",
      version,
      bytes(version),
      bytes(version, 0)
    );
    sql.exec(
      "INSERT INTO storagekit_conflict_versions(version_hi, version_lo, entry_count, byte_count) VALUES (0, ?, 1, ?)",
      version,
      perVersionByteCount
    );
  }
  sql.exec(
    "UPDATE storagekit_metadata SET value = '3' WHERE key = 'commitVersion'"
  );
  sql.exec(
    `UPDATE storagekit_metadata SET value = '${entryCountMetadata}' WHERE key = 'conflictEntryCount'`
  );
  sql.exec(
    `UPDATE storagekit_metadata SET value = '${byteCountMetadata}' WHERE key = 'conflictByteCount'`
  );
  return { sql, host };
}

function retainedConflictVersions(sql) {
  return sql.exec(
    "SELECT version_lo FROM storagekit_conflict_versions ORDER BY version_lo"
  ).map((row) => row.version_lo);
}

function metadataNumber(sql, key) {
  const rows = sql.exec(
    "SELECT value FROM storagekit_metadata WHERE key = ?",
    key
  );
  assert.equal(rows.length, 1);
  return Number(rows[0].value);
}

test("a commit prunes retained conflict versions until the entry limit holds", () => {
  // One entry over the limit before the commit; the commit adds one more, so
  // two oldest versions must be pruned before the limit holds again.
  const { sql, host } = makeHostWithPlantedConflictHistory({
    perVersionByteCount: 3,
    entryCountMetadata: storageKitWireLimits.maxConflictEntries + 1,
    byteCountMetadata: 9,
  });

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x70), value: bytes(0x70) },
    ],
    readConflictRanges: [],
  });

  assert.deepEqual(retainedConflictVersions(sql), [3, 4]);
  assert.ok(
    metadataNumber(sql, "conflictEntryCount")
      <= storageKitWireLimits.maxConflictEntries
  );
});

test("a commit prunes retained conflict versions until the byte limit holds", () => {
  // 3 x 150MB planted history: pruning one version (300MB) is still over the
  // 256MB limit, so the prune loop must evict a second version.
  const { sql, host } = makeHostWithPlantedConflictHistory({
    perVersionByteCount: 150_000_000,
    entryCountMetadata: 3,
    byteCountMetadata: 450_000_000,
  });

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x70), value: bytes(0x70) },
    ],
    readConflictRanges: [],
  });

  assert.deepEqual(retainedConflictVersions(sql), [3, 4]);
  assert.ok(
    metadataNumber(sql, "conflictByteCount")
      <= storageKitWireLimits.maxConflictBytes
  );
});

test("a commit whose read version predates retained conflict history is rejected", () => {
  const { sql, host } = makeHostWithPlantedConflictHistory({
    perVersionByteCount: 3,
    entryCountMetadata: storageKitWireLimits.maxConflictEntries + 1,
    byteCountMetadata: 9,
  });

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x70), value: bytes(0x70) },
    ],
    readConflictRanges: [],
  });
  assert.equal(metadataNumber(sql, "minimumConflictVersion"), 2);

  const response = StorageKitWire.decodeResponse(host.dispatchBytes(
    StorageKitWire.encodeRequest({
      operation: operation.commit,
      partitionIdentity,
      observedReadVersion: 1n,
      mutations: [
        { tag: 1, key: bytes(0x71), value: bytes(0x71) },
      ],
      readConflictRanges: [
        singleKeyRange(bytes(0x01)),
      ],
    })
  ));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("unrelated commit after read version does not conflict at commit", () => {
  const host = makeHost();
  const firstRead = send(host, {
    operation: operation.read,
    partitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });
  assert.equal(firstRead.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
  });

  const response = send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: firstRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x03), value: bytes(3) },
    ],
    readConflictRanges: [
      singleKeyRange(bytes(0x01)),
    ],
  });

  assert.equal(response.committedVersion, 2n);
});

test("key selectors and key cursor pagination preserve range order", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
      { tag: 1, key: bytes(0x05), value: bytes(5) },
      { tag: 1, key: bytes(0x07), value: bytes(7) },
    ],
  });

  const firstPage = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: lastLessOrEqual(bytes(0x03)),
    end: firstGreaterThan(bytes(0x05)),
    limit: 1,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });

  assert.deepEqual(firstPage.rows.map((row) => [...row.key]), [[0x03]]);
  assert.equal(firstPage.hasMore, true);

  const secondPage = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: lastLessOrEqual(bytes(0x03)),
    end: firstGreaterThan(bytes(0x05)),
    limit: 1,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: firstPage.rows[firstPage.rows.length - 1].key,
  });

  assert.deepEqual(secondPage.rows.map((row) => [...row.key]), [[0x05]]);
  assert.equal(secondPage.hasMore, false);
});

test("all key selector kinds are preserved by SQLite host pagination", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
      { tag: 1, key: bytes(0x05), value: bytes(5) },
      { tag: 1, key: bytes(0x07), value: bytes(7) },
    ],
  });

  const cases = [
    [
      firstGreaterOrEqual(bytes(0x03)),
      firstGreaterOrEqual(bytes(0x07)),
      [[0x03], [0x05]],
    ],
    [
      firstGreaterThan(bytes(0x03)),
      firstGreaterThan(bytes(0x05)),
      [[0x05]],
    ],
    [
      lastLessOrEqual(bytes(0x05)),
      firstGreaterThan(bytes(0x07)),
      [[0x05], [0x07]],
    ],
    [
      lastLessThan(bytes(0x05)),
      lastLessOrEqual(bytes(0x07)),
      [[0x03], [0x05]],
    ],
  ];

  for (const [begin, end, expectedKeys] of cases) {
    assert.deepEqual(collectRangeKeys(host, begin, end), expectedKeys);
  }
});

test("arbitrary key selector offsets resolve deterministically", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    partitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
      { tag: 1, key: bytes(0x05), value: bytes(5) },
      { tag: 1, key: bytes(0x07), value: bytes(7) },
    ],
  });

  const response = send(host, {
    operation: operation.range,
    partitionIdentity,
    begin: { key: bytes(0x01), orEqual: false, offset: 2n },
    end: { key: bytes(0x07), orEqual: true, offset: 0n },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  });
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x03], [0x05]]);
});

function makeHost() {
  const sql = new InMemorySQLiteStorage();
  const host = new StorageKitDurableObjectHost(
    sql,
    (operation) => sql.transactionSync(operation)
  );
  host.migrate();
  return host;
}

function send(host, request) {
  const bytes = StorageKitWire.encodeRequest(request);
  const response = StorageKitWire.decodeResponse(host.dispatchBytes(bytes));
  if (response.status !== statusCode.ok) {
    throw new Error(response.message);
  }
  return response;
}

class RejectingPragmaSqlStorage extends InMemorySQLiteStorage {
  exec(statement, ...bindings) {
    if (/^\s*PRAGMA\b/i.test(statement)) {
      throw new Error("Cloudflare SqlStorage does not expose this PRAGMA");
    }
    return super.exec(statement, ...bindings);
  }
}

function readValue(host, key) {
  return read(host, bytes(key));
}

function read(host, key) {
  return send(host, {
    operation: operation.read,
    partitionIdentity,
    key,
    snapshot: false,
    expectedReadVersion: null,
  }).value;
}

function rawRangeRequestWithCursor(cursor, request) {
  const encoded = StorageKitWire.encodeRequest(request);
  assert.equal(encoded[encoded.byteLength - 1], 0);
  const result = new Uint8Array(
    encoded.byteLength - 1 + 1 + 4 + cursor.byteLength
  );
  result.set(encoded.subarray(0, encoded.byteLength - 1));
  let offset = encoded.byteLength - 1;
  result[offset] = 1;
  offset += 1;
  new DataView(result.buffer).setUint32(offset, cursor.byteLength, true);
  offset += 4;
  result.set(cursor, offset);
  return result;
}

function rawRangeMetricRequest(request) {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(request.operation);
  writer.writeString(partitionIdentity.databaseID);
  writer.writeBool(false);
  writer.writeBool(false);
  writer.writeBytes(request.begin);
  writer.writeBytes(request.end);
  if (request.operation === operation.rangeSplitPoints) {
    writer.writeInt64(request.chunkSize);
  }
  writer.writeBool(false);
  return writer.toBytes();
}

function rawSplitPointsResponse(points) {
  const writer = new StorageKitWireWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(statusCode.ok);
  writer.writeUInt8(operation.rangeSplitPoints);
  writer.writeUInt32(points.length);
  for (const point of points) {
    writer.writeBytes(point);
  }
  writer.writeInt64(0n);
  return writer.toBytes();
}

function versionstampOperand(prefix, suffix) {
  const placeholder = new Uint8Array(10).fill(0xff);
  const payload = concatenate(prefix, placeholder, suffix);
  const offset = new Uint8Array(4);
  new DataView(offset.buffer).setUint32(0, prefix.byteLength, true);
  return concatenate(payload, offset);
}

function commitVersionstamp(version) {
  const result = new Uint8Array(10);
  let remaining = BigInt(version);
  for (let index = 7; index >= 0; index -= 1) {
    result[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return result;
}

function concatenate(...parts) {
  const result = new Uint8Array(
    parts.reduce((count, part) => count + part.byteLength, 0)
  );
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

function collectRangeKeys(host, begin, end) {
  const keys = [];
  let cursorKey = null;
  do {
    const response = send(host, {
      operation: operation.range,
      partitionIdentity,
      begin,
      end,
      limit: 1,
      reverse: false,
      snapshot: false,
      expectedReadVersion: 1n,
      cursorKey,
    });
    for (const row of response.rows) {
      keys.push([...row.key]);
    }
    if (!response.hasMore) {
      break;
    }
    cursorKey = response.rows[response.rows.length - 1].key;
  } while (true);
  return keys;
}

function bytes(...values) {
  return new Uint8Array(values);
}

function firstGreaterOrEqual(key) {
  return { key, orEqual: false, offset: 1n };
}

function firstGreaterThan(key) {
  return { key, orEqual: true, offset: 1n };
}

function lastLessOrEqual(key) {
  return { key, orEqual: true, offset: 0n };
}

function lastLessThan(key) {
  return { key, orEqual: false, offset: 0n };
}

function singleKeyRange(key) {
  const end = new Uint8Array(key.length + 1);
  end.set(key, 0);
  end[key.length] = 0;
  return { begin: key, end };
}

function onlyConflictRange(response) {
  assert.equal(response.readConflictRanges.length, 1);
  return response.readConflictRanges[0];
}
