import assert from "node:assert/strict";
import test from "node:test";
import { StorageKitBinaryWriter } from "../src/StorageKitBinaryWriter.js";
import { StorageKitDurableObjectHost } from "../src/StorageKitDurableObjectHost.js";
import { nameForScope } from "../src/StorageKitScope.js";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import { keySelectorKind, mutationType, operation, protocolVersion, statusCode } from "../src/StorageKitWireConstants.js";
import { NodeSqlStorage } from "./NodeSqlStorage.js";

const scope = Object.freeze({
  databaseID: "main",
  tenantID: null,
  workspaceID: null,
});

test("scope name codec matches the StorageKit v1 canonical format", () => {
  assert.equal(
    nameForScope({ databaseID: "main", tenantID: "tenant-a", workspaceID: "workspace-a" }),
    "storage-kit/cfdo/v1/database/bWFpbg/tenant/dGVuYW50LWE/workspace/d29ya3NwYWNlLWE"
  );
});

test("host dispatch rejects trailing bytes as a typed failure", () => {
  const host = makeHost();
  const bytes = StorageKitWireCodec.encodeRequest({
    operation: operation.readiness,
    scope,
  });
  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes([...bytes, 0xff]));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Trailing bytes/);
});

test("host dispatch rejects invalid UTF-8 as a typed failure", () => {
  const host = makeHost();
  const writer = new StorageKitBinaryWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(operation.readiness);
  writer.writeBytes(new Uint8Array([0xff]));
  writer.writeBool(false);
  writer.writeBool(false);
  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(writer.toBytes()));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Invalid UTF-8/);
});

test("host dispatch rejects invalid bool as a typed failure", () => {
  const host = makeHost();
  const writer = new StorageKitBinaryWriter();
  writer.writeUInt8(protocolVersion);
  writer.writeUInt8(operation.read);
  writer.writeString("main");
  writer.writeBool(false);
  writer.writeBool(false);
  writer.writeBytes(bytes(0x01));
  writer.writeUInt8(2);
  writer.writeBool(false);
  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(writer.toBytes()));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Invalid bool/);
});

test("host dispatch rejects unknown operation as a typed failure", () => {
  const host = makeHost();
  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(new Uint8Array([
    protocolVersion,
    0xff,
  ])));

  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Unknown operation/);
});

test("host dispatch rejects invalid range cursor as a typed failure", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.range,
    scope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x01) },
    limit: 1,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: "not valid!",
  })));

  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Invalid range cursor/);
});

test("set, atomic, read, and commit persistence round trip through binary dispatch", () => {
  const host = makeHost();

  let response = send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(10) },
    ],
  });
  assert.equal(response.committedVersion, 1n);

  response = send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: 1n,
    mutations: [
      { tag: 4, key: bytes(0x01), param: bytes(5), mutationType: mutationType.add },
    ],
  });
  assert.equal(response.committedVersion, 2n);

  response = send(host, {
    operation: operation.read,
    scope,
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
    scope,
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
    scope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x04) },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  });

  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x04]]);
});

test("atomic mutation semantics cover bitwise, max, min, compareAndClear, and versionstamp failure", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    scope,
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

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: 1n,
    mutations: [
      { tag: 4, key: bytes(0x07), param: bytes(0x01), mutationType: mutationType.setVersionstampedValue },
    ],
  })));
  assert.equal(response.status, statusCode.invalidOperation);
  assert.match(response.message, /Versionstamp/);
});

test("snapshot reads do not participate in commit conflict", () => {
  const host = makeHost();
  const snapshot = send(host, {
    operation: operation.read,
    scope,
    key: bytes(0x01),
    snapshot: true,
    expectedReadVersion: 99n,
  });
  assert.equal(snapshot.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
  });
  assert.equal(response.committedVersion, 2n);
});

test("non-snapshot read conflict range detects conflicting commit", () => {
  const host = makeHost();
  const firstRead = send(host, {
    operation: operation.read,
    scope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });
  assert.equal(firstRead.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
  });

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
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
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    scope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x10) },
    end: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x20) },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  });
  assert.deepEqual(rangeRead.rows.map((row) => [...row.key]), [[0x15]]);
  assert.deepEqual([...rangeRead.conflictRange.begin], [0x10]);
  assert.deepEqual([...rangeRead.conflictRange.end], [0x20]);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x12), value: bytes(12) },
    ],
    readConflictRanges: [],
  });

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x30), value: bytes(30) },
    ],
    readConflictRanges: [
      rangeRead.conflictRange,
    ],
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("range read conflict range does not catch writes after direct end selector", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    scope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x10) },
    end: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x20) },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  });
  assert.deepEqual([...rangeRead.conflictRange.begin], [0x10]);
  assert.deepEqual([...rangeRead.conflictRange.end], [0x20]);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x30), value: bytes(30) },
    ],
    readConflictRanges: [],
  });

  const response = send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x40), value: bytes(40) },
    ],
    readConflictRanges: [
      rangeRead.conflictRange,
    ],
  });
  assert.equal(response.committedVersion, 3n);
});

test("range read conflict range includes exact key for firstGreaterThan end selector", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
    ],
    readConflictRanges: [],
  });

  const rangeRead = send(host, {
    operation: operation.range,
    scope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x03) },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  });
  assert.deepEqual(rangeRead.rows.map((row) => [...row.key]), [[0x01], [0x03]]);
  assert.deepEqual([...rangeRead.conflictRange.end], [0x03, 0x00]);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x03), value: bytes(33) },
    ],
    readConflictRanges: [],
  });

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: rangeRead.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x04), value: bytes(4) },
    ],
    readConflictRanges: [
      rangeRead.conflictRange,
    ],
  })));
  assert.equal(response.status, statusCode.transactionConflict);
});

test("old conflict entries are pruned and stale readers conflict", () => {
  const sql = new NodeSqlStorage();
  const host = new StorageKitDurableObjectHost(sql, (callback) => sql.transactionSync(callback));
  const initialRead = send(host, {
    operation: operation.read,
    scope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });

  for (let index = 0; index < 4100; index += 1) {
    send(host, {
      operation: operation.commit,
      scope,
      observedReadVersion: null,
      mutations: [
        { tag: 1, key: bytes(0x80, index & 0xff), value: bytes(index & 0xff) },
      ],
      readConflictRanges: [],
    });
  }

  const rows = sql.exec("SELECT COUNT(*) AS count FROM storagekit_conflicts");
  assert.ok(rows[0].count <= 4096);

  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
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

test("unrelated commit after read version does not conflict at commit", () => {
  const host = makeHost();
  const firstRead = send(host, {
    operation: operation.read,
    scope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: null,
  });
  assert.equal(firstRead.currentCommitVersion, 0n);

  send(host, {
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x02), value: bytes(2) },
    ],
  });

  const response = send(host, {
    operation: operation.commit,
    scope,
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
    scope,
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
    scope,
    begin: { kind: keySelectorKind.lastLessOrEqual, key: bytes(0x03) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x05) },
    limit: 1,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  });

  assert.deepEqual(firstPage.rows.map((row) => [...row.key]), [[0x03]]);
  assert.notEqual(firstPage.nextCursor, null);

  const secondPage = send(host, {
    operation: operation.range,
    scope,
    begin: { kind: keySelectorKind.lastLessOrEqual, key: bytes(0x03) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x05) },
    limit: 1,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: firstPage.nextCursor,
  });

  assert.deepEqual(secondPage.rows.map((row) => [...row.key]), [[0x05]]);
  assert.equal(secondPage.nextCursor, null);
});

test("all key selector kinds are preserved by SQLite host pagination", () => {
  const host = makeHost();
  send(host, {
    operation: operation.commit,
    scope,
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
      { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x03) },
      { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x07) },
      [[0x03], [0x05]],
    ],
    [
      { kind: keySelectorKind.firstGreaterThan, key: bytes(0x03) },
      { kind: keySelectorKind.firstGreaterThan, key: bytes(0x05) },
      [[0x05]],
    ],
    [
      { kind: keySelectorKind.lastLessOrEqual, key: bytes(0x05) },
      { kind: keySelectorKind.firstGreaterThan, key: bytes(0x07) },
      [[0x05], [0x07]],
    ],
    [
      { kind: keySelectorKind.lastLessThan, key: bytes(0x05) },
      { kind: keySelectorKind.lastLessOrEqual, key: bytes(0x07) },
      [[0x03], [0x05]],
    ],
  ];

  for (const [begin, end, expectedKeys] of cases) {
    assert.deepEqual(collectRangeKeys(host, begin, end), expectedKeys);
  }
});

function makeHost() {
  const sql = new NodeSqlStorage();
  return new StorageKitDurableObjectHost(sql, (callback) => sql.transactionSync(callback));
}

function send(host, request) {
  const bytes = StorageKitWireCodec.encodeRequest(request);
  const response = StorageKitWireCodec.decodeResponse(host.dispatchBytes(bytes));
  if (response.status !== statusCode.ok) {
    throw new Error(response.message);
  }
  return response;
}

function readValue(host, key) {
  return send(host, {
    operation: operation.read,
    scope,
    key: bytes(key),
    snapshot: false,
    expectedReadVersion: null,
  }).value;
}

function collectRangeKeys(host, begin, end) {
  const keys = [];
  let cursor = null;
  do {
    const response = send(host, {
      operation: operation.range,
      scope,
      begin,
      end,
      limit: 1,
      reverse: false,
      snapshot: false,
      expectedReadVersion: 1n,
      cursor,
    });
    for (const row of response.rows) {
      keys.push([...row.key]);
    }
    cursor = response.nextCursor;
  } while (cursor !== null);
  return keys;
}

function bytes(...values) {
  return new Uint8Array(values);
}

function singleKeyRange(key) {
  const end = new Uint8Array(key.length + 1);
  end.set(key, 0);
  end[key.length] = 0;
  return { begin: key, end };
}
