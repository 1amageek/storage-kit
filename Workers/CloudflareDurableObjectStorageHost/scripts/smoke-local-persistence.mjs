import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { StorageKitWire } from "../src/StorageKitWire.js";
import {
  operation,
  statusCode,
} from "../src/StorageKitWireConstants.js";

const host = process.env.STORAGEKIT_PERSISTENCE_HOST ?? "127.0.0.1";
const port = Number(process.env.STORAGEKIT_PERSISTENCE_PORT ?? "18789");
const endpoint = `http://${host}:${port}`;
const readyTimeoutMilliseconds = 30_000;
const packageDirectory = fileURLToPath(new URL("..", import.meta.url));
const devVarsPath = fileURLToPath(new URL("../.dev.vars", import.meta.url));
const statePath = fileURLToPath(new URL("../.wrangler/storagekit-persistence-smoke", import.meta.url));
const accessToken = "local-storage-kit-persistence-token";
const token = randomBytes(16);
const testPartitionIdentity = {
  databaseID: `storagekit-local-persistence-${process.pid}-${Date.now()}`,
  tenantID: "tenant-persistence",
  workspaceID: "workspace-persistence",
};
const key = new Uint8Array([0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74]);

let originalDevVars = null;
rmSync(statePath, { recursive: true, force: true });
writeDevVars();

let worker = null;
try {
  worker = startWorker();
  await waitForWorker(worker);
  await writeToken();
  await stopWorker(worker);
  worker = null;

  worker = startWorker();
  await waitForWorker(worker);
  await readToken();
  console.log("Cloudflare Durable Object Storage local persistence smoke passed");
} finally {
  if (worker !== null) {
    await stopWorker(worker);
  }
  removeDevVars();
  rmSync(statePath, { recursive: true, force: true });
}

function startWorker() {
  const wrangler = process.platform === "win32"
    ? "node_modules/.bin/wrangler.cmd"
    : "node_modules/.bin/wrangler";
  const child = spawn(wrangler, [
    "dev",
    "--port",
    String(port),
    "--ip",
    host,
    "--persist-to",
    statePath,
  ], {
    cwd: packageDirectory,
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => process.stdout.write(chunk));
  child.stderr.on("data", (chunk) => process.stderr.write(chunk));
  return child;
}

async function waitForWorker(child) {
  const deadline = Date.now() + readyTimeoutMilliseconds;
  let lastError = null;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`wrangler dev exited early with code ${child.exitCode}`);
    }
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

async function writeToken() {
  const response = expectOk(await send({
    operation: operation.commit,
    partitionIdentity: testPartitionIdentity,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key, value: token },
    ],
    readConflictRanges: [],
  }));
  assert.equal(response.committedVersion, 1n);
}

async function readToken() {
  const response = expectOk(await send({
    operation: operation.read,
    partitionIdentity: testPartitionIdentity,
    key,
    snapshot: false,
    expectedReadVersion: null,
  }));
  assert.deepEqual([...response.value], [...token]);
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

function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
