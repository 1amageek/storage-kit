import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import {
  operation,
  statusCode,
} from "../src/StorageKitWireConstants.js";

const endpoint = process.env.STORAGEKIT_SMOKE_ENDPOINT;
const accessToken = process.env.STORAGEKIT_ACCESS_TOKEN;
const mode = process.env.STORAGEKIT_PERSISTENCE_MODE ?? "write-read";
const runID = process.env.STORAGEKIT_PERSISTENCE_RUN_ID ?? "release-candidate";
const tokenHex = process.env.STORAGEKIT_PERSISTENCE_TOKEN ?? randomBytes(16).toString("hex");
const scope = {
  databaseID: `storagekit-cloudflare-persistence-${runID}`,
  tenantID: "tenant-persistence",
  workspaceID: "workspace-persistence",
};
const keyBytes = new Uint8Array([0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74]);

if (endpoint === undefined || endpoint.length === 0) {
  throw new Error("STORAGEKIT_SMOKE_ENDPOINT is required");
}

if (accessToken === undefined || accessToken.length === 0) {
  throw new Error("STORAGEKIT_ACCESS_TOKEN is required");
}

if (!["write", "read", "write-read"].includes(mode)) {
  throw new Error("STORAGEKIT_PERSISTENCE_MODE must be write, read, or write-read");
}

if (mode === "read" && process.env.STORAGEKIT_PERSISTENCE_TOKEN === undefined) {
  throw new Error("STORAGEKIT_PERSISTENCE_TOKEN is required for read mode");
}

if (mode === "write" || mode === "write-read") {
  await writeToken();
}

if (mode === "read" || mode === "write-read") {
  await readToken();
}

console.log(JSON.stringify({
  endpoint,
  mode,
  runID,
  tokenHex,
}, null, 2));

async function writeToken() {
  const response = expectOk(await send({
    operation: operation.commit,
    scope,
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: keyBytes, value: hexBytes(tokenHex) },
    ],
    readConflictRanges: [],
  }));
  assert.equal(typeof response.committedVersion, "bigint");
}

async function readToken() {
  const response = expectOk(await send({
    operation: operation.read,
    scope,
    key: keyBytes,
    snapshot: false,
    expectedReadVersion: null,
  }));
  assert.deepEqual([...response.value], [...hexBytes(tokenHex)]);
}

async function send(request) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/octet-stream",
      accept: "application/octet-stream",
      authorization: `Bearer ${accessToken}`,
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

function hexBytes(value) {
  if (value.length % 2 !== 0) {
    throw new Error("Hex value must have an even byte length");
  }
  return Uint8Array.from(value.match(/.{2}/g).map((byte) => Number.parseInt(byte, 16)));
}
