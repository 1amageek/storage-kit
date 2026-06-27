import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import {
  keySelectorKind,
  mutationType,
  operation,
  statusCode,
} from "../src/StorageKitWireConstants.js";

const host = process.env.STORAGEKIT_SMOKE_HOST ?? "127.0.0.1";
const port = Number(process.env.STORAGEKIT_SMOKE_PORT ?? "18787");
const endpoint = process.env.STORAGEKIT_SMOKE_ENDPOINT ?? `http://${host}:${port}`;
const readyTimeoutMilliseconds = 30_000;
const packageDirectory = fileURLToPath(new URL("..", import.meta.url));
const smokeRunID = `${process.pid}-${Date.now()}`;
const shouldStartWorker = process.env.STORAGEKIT_SMOKE_ENDPOINT === undefined && !process.argv.includes("--remote");

if (!shouldStartWorker && !process.env.STORAGEKIT_SMOKE_ENDPOINT) {
  throw new Error("STORAGEKIT_SMOKE_ENDPOINT is required for remote smoke mode");
}

const worker = shouldStartWorker ? startWorker() : null;
try {
  await waitForWorker();
  await smokeReadiness();
  await smokeWasmAtomicReadRangeAndPagination();
  await smokeQuerySelectorMatrix();
  await smokeBytewisePrefixQuery();
  await smokeClearRangeAndReverseRange();
  await smokeScopeIsolation();
  await smokeReadConflictRanges();
  await smokeTypedBadRequest();
  console.log("Cloudflare Durable Object Storage smoke E2E passed");
} finally {
  if (worker !== null) {
    await stopWorker(worker);
  }
}

function startWorker() {
  const wrangler = process.platform === "win32"
    ? "node_modules/.bin/wrangler.cmd"
    : "node_modules/.bin/wrangler";
  const child = spawn(wrangler, ["dev", "--port", String(port), "--ip", host], {
    cwd: packageDirectory,
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => process.stdout.write(chunk));
  child.stderr.on("data", (chunk) => process.stderr.write(chunk));
  child.on("exit", (code, signal) => {
    if (code !== 0 && signal === null) {
      process.stderr.write(`wrangler dev exited with code ${code}\n`);
    }
  });
  return child;
}

async function waitForWorker() {
  const deadline = Date.now() + readyTimeoutMilliseconds;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(endpoint, { method: "GET" });
      if (response.status === 405) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await delay(250);
  }
  throw new Error(`Worker did not become ready: ${String(lastError)}`);
}

async function smokeReadiness() {
  const response = expectOk(await send({
    operation: operation.readiness,
    scope: scope("readiness"),
  }));
  assert.equal(response.operation, operation.readiness);
  assert.equal(response.schemaVersion, 1);
  assert.equal(response.commitVersion, 0n);
  assert.equal(response.metadataInitialized, true);
}

async function smokeWasmAtomicReadRangeAndPagination() {
  const testScope = scope("atomic-range");
  expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(10) },
      { tag: 1, key: bytes(0x02), value: bytes(20) },
      { tag: 1, key: bytes(0x03), value: bytes(30) },
      { tag: 4, key: bytes(0x01), param: bytes(5), mutationType: mutationType.add },
    ],
    readConflictRanges: [],
  }));

  let response = expectOk(await send({
    operation: operation.read,
    scope: testScope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [15]);

  response = expectOk(await send({
    operation: operation.range,
    scope: testScope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x03) },
    limit: 2,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x02]]);
  assert.notEqual(response.nextCursor, null);
  assert.deepEqual([...response.conflictRange.begin], [0x01]);
  assert.equal(response.conflictRange.end, null);

  response = expectOk(await send({
    operation: operation.range,
    scope: testScope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x03) },
    limit: 2,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: response.nextCursor,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x03]]);
  assert.equal(response.nextCursor, null);
}

async function smokeQuerySelectorMatrix() {
  const testScope = scope("query-selectors");
  await seedKeys(testScope, [
    [0x10],
    [0x20],
    [0x30],
    [0x40],
    [0x50],
  ]);

  const patterns = [
    {
      begin: selector(keySelectorKind.firstGreaterOrEqual, [0x20]),
      end: selector(keySelectorKind.firstGreaterOrEqual, [0x50]),
      expected: [[0x20], [0x30], [0x40]],
    },
    {
      begin: selector(keySelectorKind.firstGreaterThan, [0x20]),
      end: selector(keySelectorKind.firstGreaterThan, [0x40]),
      expected: [[0x30], [0x40]],
    },
    {
      begin: selector(keySelectorKind.lastLessOrEqual, [0x35]),
      end: selector(keySelectorKind.firstGreaterThan, [0x40]),
      expected: [[0x30], [0x40]],
    },
    {
      begin: selector(keySelectorKind.lastLessThan, [0x30]),
      end: selector(keySelectorKind.lastLessOrEqual, [0x50]),
      expected: [[0x20], [0x30], [0x40]],
    },
    {
      begin: selector(keySelectorKind.lastLessThan, [0x10]),
      end: selector(keySelectorKind.firstGreaterThan, [0x20]),
      expected: [[0x10], [0x20]],
    },
    {
      begin: selector(keySelectorKind.firstGreaterOrEqual, [0x40]),
      end: selector(keySelectorKind.firstGreaterThan, [0x99]),
      expected: [[0x40], [0x50]],
    },
    {
      begin: selector(keySelectorKind.firstGreaterThan, [0x99]),
      end: selector(keySelectorKind.firstGreaterThan, [0x99]),
      expected: [],
    },
    {
      begin: selector(keySelectorKind.firstGreaterOrEqual, [0x40]),
      end: selector(keySelectorKind.firstGreaterOrEqual, [0x30]),
      expected: [],
    },
  ];

  for (const pattern of patterns) {
    const response = expectOk(await send(rangeRequest(testScope, pattern)));
    assertRangeKeys(response, pattern.expected);
    assert.equal(response.nextCursor, null);
  }

  let page = expectOk(await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
  })));
  assertRangeKeys(page, [[0x50], [0x40]]);
  assert.notEqual(page.nextCursor, null);

  page = expectOk(await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
    cursor: page.nextCursor,
  })));
  assertRangeKeys(page, [[0x30], [0x20]]);
  assert.notEqual(page.nextCursor, null);

  page = expectOk(await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
    cursor: page.nextCursor,
  })));
  assertRangeKeys(page, [[0x10]]);
  assert.equal(page.nextCursor, null);

  const snapshotResponse = expectOk(await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x20]),
    snapshot: true,
    expectedReadVersion: 0n,
  })));
  assertRangeKeys(snapshotResponse, [[0x10], [0x20]]);

  const staleResponse = await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x20]),
    snapshot: false,
    expectedReadVersion: 0n,
  }));
  assert.equal(staleResponse.status, statusCode.transactionConflict);
}

async function smokeBytewisePrefixQuery() {
  const testScope = scope("query-bytewise-prefix");
  await seedKeys(testScope, [
    [0x01],
    [0x01, 0x00],
    [0x01, 0xff],
    [0x02],
  ]);

  const response = expectOk(await send(rangeRequest(testScope, {
    begin: selector(keySelectorKind.firstGreaterThan, [0x01]),
    end: selector(keySelectorKind.firstGreaterOrEqual, [0x02]),
  })));
  assertRangeKeys(response, [[0x01, 0x00], [0x01, 0xff]]);
}

async function smokeClearRangeAndReverseRange() {
  const testScope = scope("clear-range");
  expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
      { tag: 1, key: bytes(0x02), value: bytes(2) },
      { tag: 1, key: bytes(0x03), value: bytes(3) },
      { tag: 1, key: bytes(0x04), value: bytes(4) },
      { tag: 3, begin: bytes(0x02), end: bytes(0x04) },
    ],
    readConflictRanges: [],
  }));

  let response = expectOk(await send({
    operation: operation.range,
    scope: testScope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x04) },
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x04]]);

  response = expectOk(await send({
    operation: operation.range,
    scope: testScope,
    begin: { kind: keySelectorKind.firstGreaterOrEqual, key: bytes(0x01) },
    end: { kind: keySelectorKind.firstGreaterThan, key: bytes(0x04) },
    limit: 10,
    reverse: true,
    snapshot: false,
    expectedReadVersion: 1n,
    cursor: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x04], [0x01]]);
}

async function smokeScopeIsolation() {
  const firstScope = scope("scope-isolation", "tenant-a");
  const secondScope = scope("scope-isolation", "tenant-b");
  expectOk(await send({
    operation: operation.commit,
    scope: firstScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
    readConflictRanges: [],
  }));
  expectOk(await send({
    operation: operation.commit,
    scope: secondScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(2) },
    ],
    readConflictRanges: [],
  }));

  let response = expectOk(await send({
    operation: operation.read,
    scope: firstScope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [1]);

  response = expectOk(await send({
    operation: operation.read,
    scope: secondScope,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [2]);
}

async function smokeReadConflictRanges() {
  const testScope = scope("conflicts");
  let response = expectOk(await send({
    operation: operation.read,
    scope: testScope,
    key: bytes(0x09),
    snapshot: false,
    expectedReadVersion: null,
  }));
  const readVersion = response.currentCommitVersion;

  expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x08), value: bytes(8) },
    ],
    readConflictRanges: [],
  }));

  response = expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: readVersion,
    mutations: [
      { tag: 1, key: bytes(0x07), value: bytes(7) },
    ],
    readConflictRanges: [singleKeyRange(bytes(0x09))],
  }));
  assert.equal(response.committedVersion, 2n);

  response = expectOk(await send({
    operation: operation.read,
    scope: testScope,
    key: bytes(0x0a),
    snapshot: false,
    expectedReadVersion: null,
  }));
  const conflictReadVersion = response.currentCommitVersion;

  expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x0a), value: bytes(10) },
    ],
    readConflictRanges: [],
  }));

  response = await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: conflictReadVersion,
    mutations: [
      { tag: 1, key: bytes(0x0b), value: bytes(11) },
    ],
    readConflictRanges: [singleKeyRange(bytes(0x0a))],
  });
  assert.equal(response.status, statusCode.transactionConflict);
}

async function smokeTypedBadRequest() {
  const httpResponse = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
    },
    body: new Uint8Array([0xff]),
  });
  assert.equal(httpResponse.status, 200);
  const response = StorageKitWireCodec.decodeResponse(new Uint8Array(await httpResponse.arrayBuffer()));
  assert.equal(response.status, statusCode.invalidOperation);
}

async function send(request) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
    },
    body: StorageKitWireCodec.encodeRequest(request),
  });
  assert.equal(response.status, 200);
  return StorageKitWireCodec.decodeResponse(new Uint8Array(await response.arrayBuffer()));
}

function expectOk(response) {
  assert.equal(response.status, statusCode.ok, response.message ?? "expected ok");
  return response;
}

async function stopWorker(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  child.stdin.write("x");
  const exit = once(child, "exit");
  const timeout = delay(5_000).then(() => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
    }
  });
  await Promise.race([exit, timeout]);
}

async function seedKeys(testScope, keys) {
  expectOk(await send({
    operation: operation.commit,
    scope: testScope,
    observedReadVersion: null,
    mutations: keys.map((key, index) => ({
      tag: 1,
      key: bytes(...key),
      value: bytes(index + 1),
    })),
    readConflictRanges: [],
  }));
}

function rangeRequest(testScope, {
  begin,
  end,
  limit = 10,
  reverse = false,
  snapshot = false,
  expectedReadVersion = 1n,
  cursor = null,
}) {
  return {
    operation: operation.range,
    scope: testScope,
    begin,
    end,
    limit,
    reverse,
    snapshot,
    expectedReadVersion,
    cursor,
  };
}

function selector(kind, key) {
  return {
    kind,
    key: bytes(...key),
  };
}

function assertRangeKeys(response, expected) {
  assert.deepEqual(response.rows.map((row) => [...row.key]), expected);
}

function scope(databaseID, tenantID = "tenant-a", workspaceID = "workspace-a") {
  return {
    databaseID: `smoke-${databaseID}-${smokeRunID}`,
    tenantID,
    workspaceID,
  };
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

function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
