import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { StorageKitWire } from "../src/StorageKitWire.js";
import {
  mutationType,
  operation,
  statusCode,
} from "../src/StorageKitWireConstants.js";

const keySelectorKind = Object.freeze({
  firstGreaterOrEqual: "firstGreaterOrEqual",
  firstGreaterThan: "firstGreaterThan",
  lastLessOrEqual: "lastLessOrEqual",
  lastLessThan: "lastLessThan",
});

const host = process.env.STORAGEKIT_SMOKE_HOST ?? "127.0.0.1";
const port = Number(process.env.STORAGEKIT_SMOKE_PORT ?? "18787");
const endpoint = `http://${host}:${port}`;
const readyTimeoutMilliseconds = 30_000;
const packageDirectory = fileURLToPath(new URL("..", import.meta.url));
const devVarsPath = fileURLToPath(new URL("../.dev.vars", import.meta.url));
const smokeRunID = `${process.pid}-${Date.now()}`;
const accessToken = process.env.STORAGEKIT_ACCESS_TOKEN ?? "local-storage-kit-smoke-token";

let worker = null;
let originalDevVars = null;
const expectedShutdowns = new WeakSet();
try {
  writeDevVars();
  worker = startWorker();
  await waitForWorker();
  await smokeHttpGuards();
  await smokeReadiness();
  await smokeMaximumStoredPair();
  await smokeAtomicReadRangeAndPagination();
  await smokeQuerySelectorMatrix();
  await smokeBytewisePrefixQuery();
  await smokeClearRangeAndReverseRange();
  await smokePartitionIdentityIsolation();
  await smokeReadConflictRanges();
  await smokeRangeConflictGaps();
  await smokeTypedBadRequest();
  console.log("Cloudflare Durable Object Storage smoke E2E passed");
} finally {
  if (worker !== null) {
    await stopWorker(worker);
  }
  removeDevVars();
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
    if (code !== 0 && signal === null && !expectedShutdowns.has(child)) {
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

async function smokeHttpGuards() {
  const methodResponse = await fetch(endpoint, { method: "GET" });
  assert.equal(methodResponse.status, 405);

  const unauthorizedResponse = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
    },
    body: StorageKitWire.encodeRequest({
      operation: operation.readiness,
      partitionIdentity: partitionIdentity("unauthorized"),
    }),
  });
  assert.equal(unauthorizedResponse.status, 401);
}

async function smokeReadiness() {
  const response = expectOk(await send({
    operation: operation.readiness,
    partitionIdentity: partitionIdentity("readiness"),
  }));
  assert.equal(response.operation, operation.readiness);
  assert.equal(response.schemaVersion, 1);
  assert.equal(response.commitVersion, 0n);
  assert.equal(response.metadataInitialized, true);
}

async function smokeMaximumStoredPair() {
  const testPartitionIdentity = partitionIdentity("maximum-stored-pair");
  const key = bytes(0x01);
  const value = new Uint8Array(1_999_999);

  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key, value },
    ],
    readConflictRanges: [],
  }));

  const response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: testPartitionIdentity,
    key,
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.equal(response.value.byteLength, value.byteLength);
}

async function smokeAtomicReadRangeAndPagination() {
  const testPartitionIdentity = partitionIdentity("atomic-range");
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
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
    partitionIdentity: testPartitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [15]);

  response = expectOk(await send({
    operation: operation.range,
    partitionIdentity: testPartitionIdentity,
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x01]),
    end: selector(keySelectorKind.firstGreaterThan, [0x03]),
    limit: 2,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x02]]);
  assert.equal(response.hasMore, true);
  assert.deepEqual([...onlyConflictRange(response).begin], [0x01]);
  assert.deepEqual([...onlyConflictRange(response).end], [0x03, 0x00]);

  response = expectOk(await send({
    operation: operation.range,
    partitionIdentity: testPartitionIdentity,
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x01]),
    end: selector(keySelectorKind.firstGreaterThan, [0x03]),
    limit: 2,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: response.rows[response.rows.length - 1].key,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x03]]);
  assert.equal(response.hasMore, false);
}

async function smokeQuerySelectorMatrix() {
  const testPartitionIdentity = partitionIdentity("query-selectors");
  await seedKeys(testPartitionIdentity, [
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
    const response = expectOk(await send(rangeRequest(testPartitionIdentity, pattern)));
    assertRangeKeys(response, pattern.expected);
    assert.equal(response.hasMore, false);
  }

  let page = expectOk(await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
  })));
  assertRangeKeys(page, [[0x50], [0x40]]);
  assert.equal(page.hasMore, true);

  page = expectOk(await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
    cursorKey: page.rows[page.rows.length - 1].key,
  })));
  assertRangeKeys(page, [[0x30], [0x20]]);
  assert.equal(page.hasMore, true);

  page = expectOk(await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x50]),
    limit: 2,
    reverse: true,
    cursorKey: page.rows[page.rows.length - 1].key,
  })));
  assertRangeKeys(page, [[0x10]]);
  assert.equal(page.hasMore, false);

  const snapshotResponse = expectOk(await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x20]),
    snapshot: true,
    expectedReadVersion: 1n,
  })));
  assertRangeKeys(snapshotResponse, [[0x10], [0x20]]);

  const staleResponse = await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterThan, [0x20]),
    snapshot: false,
    expectedReadVersion: 0n,
  }));
  assert.equal(staleResponse.status, statusCode.transactionConflict);
}

async function smokeBytewisePrefixQuery() {
  const testPartitionIdentity = partitionIdentity("query-bytewise-prefix");
  await seedKeys(testPartitionIdentity, [
    [0x01],
    [0x01, 0x00],
    [0x01, 0xff],
    [0x02],
  ]);

  const response = expectOk(await send(rangeRequest(testPartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterThan, [0x01]),
    end: selector(keySelectorKind.firstGreaterOrEqual, [0x02]),
  })));
  assertRangeKeys(response, [[0x01, 0x00], [0x01, 0xff]]);
}

async function smokeClearRangeAndReverseRange() {
  const testPartitionIdentity = partitionIdentity("clear-range");
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
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
    partitionIdentity: testPartitionIdentity,
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x01]),
    end: selector(keySelectorKind.firstGreaterThan, [0x04]),
    limit: 10,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x01], [0x04]]);

  response = expectOk(await send({
    operation: operation.range,
    partitionIdentity: testPartitionIdentity,
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x01]),
    end: selector(keySelectorKind.firstGreaterThan, [0x04]),
    limit: 10,
    reverse: true,
    snapshot: false,
    expectedReadVersion: 1n,
    cursorKey: null,
  }));
  assert.deepEqual(response.rows.map((row) => [...row.key]), [[0x04], [0x01]]);
}

async function smokePartitionIdentityIsolation() {
  const firstPartitionIdentity = partitionIdentity("partitionIdentity-isolation", "tenant-a");
  const secondPartitionIdentity = partitionIdentity("partitionIdentity-isolation", "tenant-b");
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: firstPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(1) },
    ],
    readConflictRanges: [],
  }));
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: secondPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(2) },
    ],
    readConflictRanges: [],
  }));

  let response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: firstPartitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [1]);

  response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: secondPartitionIdentity,
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  }));
  assert.deepEqual([...response.value], [2]);
}

async function smokeReadConflictRanges() {
  const testPartitionIdentity = partitionIdentity("conflicts");
  let response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: testPartitionIdentity,
    key: bytes(0x09),
    snapshot: false,
    expectedReadVersion: null,
  }));
  const readVersion = response.currentCommitVersion;

  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x08), value: bytes(8) },
    ],
    readConflictRanges: [],
  }));

  response = expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: readVersion,
    mutations: [
      { tag: 1, key: bytes(0x07), value: bytes(7) },
    ],
    readConflictRanges: [singleKeyRange(bytes(0x09))],
  }));
  assert.equal(response.committedVersion, 2n);

  response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: testPartitionIdentity,
    key: bytes(0x0a),
    snapshot: false,
    expectedReadVersion: null,
  }));
  const conflictReadVersion = response.currentCommitVersion;

  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x0a), value: bytes(10) },
    ],
    readConflictRanges: [],
  }));

  response = await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: conflictReadVersion,
    mutations: [
      { tag: 1, key: bytes(0x0b), value: bytes(11) },
    ],
    readConflictRanges: [singleKeyRange(bytes(0x0a))],
  });
  assert.equal(response.status, statusCode.transactionConflict);
}

async function smokeRangeConflictGaps() {
  const outsidePartitionIdentity = partitionIdentity("range-conflict-outside");
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: outsidePartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  }));

  let rangeResponse = expectOk(await send(rangeRequest(outsidePartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterOrEqual, [0x20]),
    expectedReadVersion: 1n,
  })));
  assert.deepEqual(rangeResponse.rows.map((row) => [...row.key]), [[0x15]]);
  assert.deepEqual([...onlyConflictRange(rangeResponse).begin], [0x10]);
  assert.deepEqual([...onlyConflictRange(rangeResponse).end], [0x20]);

  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: outsidePartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x30), value: bytes(30) },
    ],
    readConflictRanges: [],
  }));

  let commitResponse = expectOk(await send({
    operation: operation.commit,
    partitionIdentity: outsidePartitionIdentity,
    observedReadVersion: rangeResponse.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x40), value: bytes(40) },
    ],
    readConflictRanges: rangeResponse.readConflictRanges,
  }));
  assert.equal(commitResponse.committedVersion, 3n);

  const insidePartitionIdentity = partitionIdentity("range-conflict-inside");
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: insidePartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x15), value: bytes(15) },
    ],
    readConflictRanges: [],
  }));

  rangeResponse = expectOk(await send(rangeRequest(insidePartitionIdentity, {
    begin: selector(keySelectorKind.firstGreaterOrEqual, [0x10]),
    end: selector(keySelectorKind.firstGreaterOrEqual, [0x20]),
    expectedReadVersion: 1n,
  })));

  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: insidePartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x12), value: bytes(12) },
    ],
    readConflictRanges: [],
  }));

  commitResponse = await send({
    operation: operation.commit,
    partitionIdentity: insidePartitionIdentity,
    observedReadVersion: rangeResponse.currentCommitVersion,
    mutations: [
      { tag: 1, key: bytes(0x40), value: bytes(40) },
    ],
    readConflictRanges: rangeResponse.readConflictRanges,
  });
  assert.equal(commitResponse.status, statusCode.transactionConflict);
}

async function smokeTypedBadRequest() {
  const httpResponse = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
      authorization: `Bearer ${accessToken}`,
    },
    body: new Uint8Array([0xff]),
  });
  assert.equal(httpResponse.status, 200);
  const response = StorageKitWire.decodeResponse(new Uint8Array(await httpResponse.arrayBuffer()));
  assert.equal(response.status, statusCode.invalidOperation);
}

async function send(request) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
      authorization: `Bearer ${accessToken}`,
    },
    body: StorageKitWire.encodeRequest(request),
  });
  assert.equal(response.status, 200);
  return StorageKitWire.decodeResponse(new Uint8Array(await response.arrayBuffer()));
}

function expectOk(response) {
  assert.equal(response.status, statusCode.ok, response.message ?? "expected ok");
  return response;
}

function writeDevVars() {
  if (existsSync(devVarsPath)) {
    originalDevVars = readFileSync(devVarsPath);
  }
  writeFileSync(devVarsPath, [
    `STORAGEKIT_ACCESS_TOKEN=${accessToken}`,
    "STORAGEKIT_MAX_REQUEST_BYTES=4194304",
    "",
  ].join("\n"));
}

function removeDevVars() {
  if (originalDevVars === null) {
    rmSync(devVarsPath, { force: true });
  } else {
    writeFileSync(devVarsPath, originalDevVars);
  }
}

async function stopWorker(child) {
  expectedShutdowns.add(child);
  if (child.exitCode === null && child.signalCode === null) {
    child.stdin.end("x");
    await Promise.race([once(child, "exit"), delay(5_000)]);
  }
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGTERM");
    const exited = await Promise.race([
      once(child, "exit").then(() => true),
      delay(5_000).then(() => false),
    ]);
    assert.equal(exited, true, "wrangler dev did not stop after SIGTERM");
  }
  await requireWorkerStopped();
}

async function requireWorkerStopped() {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      await fetch(endpoint, { method: "GET", signal: AbortSignal.timeout(500) });
    } catch {
      return;
    }
    await delay(100);
  }
  assert.fail("wrangler dev remained reachable after shutdown");
}

async function seedKeys(testPartitionIdentity, keys) {
  expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: null,
    mutations: keys.map((key, index) => ({
      tag: 1,
      key: bytes(...key),
      value: bytes(index + 1),
    })),
    readConflictRanges: [],
  }));
}

function rangeRequest(testPartitionIdentity, {
  begin,
  end,
  limit = 10,
  reverse = false,
  snapshot = false,
  expectedReadVersion = 1n,
  cursorKey = null,
}) {
  return {
    operation: operation.range,
    partitionIdentity: testPartitionIdentity,
    begin,
    end,
    limit,
    reverse,
    snapshot,
    expectedReadVersion,
    cursorKey,
  };
}

function selector(kind, key) {
  switch (kind) {
    case keySelectorKind.firstGreaterOrEqual:
      return { key: bytes(...key), orEqual: false, offset: 1n };
    case keySelectorKind.firstGreaterThan:
      return { key: bytes(...key), orEqual: true, offset: 1n };
    case keySelectorKind.lastLessOrEqual:
      return { key: bytes(...key), orEqual: true, offset: 0n };
    case keySelectorKind.lastLessThan:
      return { key: bytes(...key), orEqual: false, offset: 0n };
    default:
      throw new Error(`Unknown selector kind: ${kind}`);
  }
}

function assertRangeKeys(response, expected) {
  assert.deepEqual(response.rows.map((row) => [...row.key]), expected);
}

function partitionIdentity(databaseID, tenantID = "tenant-a", workspaceID = "workspace-a") {
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

function onlyConflictRange(response) {
  assert.equal(response.readConflictRanges.length, 1);
  return response.readConflictRanges[0];
}

function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
