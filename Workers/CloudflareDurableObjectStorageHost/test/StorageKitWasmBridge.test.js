import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";
import { StorageKitDurableObjectHost } from "../src/StorageKitDurableObjectHost.js";
import { StorageKitWasmBridge } from "../src/StorageKitWasmBridge.js";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import { mutationType, operation, statusCode } from "../src/StorageKitWireConstants.js";
import { NodeSqlStorage } from "./NodeSqlStorage.js";

const wasmURL = new URL(
  "../../../.build/wasm32-unknown-wasip1/release/CloudflareDurableObjectStorageWasm.wasm",
  import.meta.url
);

test("WASM bridge dispatches a binary request through the Swift kernel", { skip: !existsSync(wasmURL) }, async () => {
  const sql = new NodeSqlStorage();
  const host = new StorageKitDurableObjectHost(sql, (callback) => sql.transactionSync(callback));
  const module = await WebAssembly.compile(readFileSync(wasmURL));
  const bridge = await StorageKitWasmBridge.instantiate(module, host);

  const responseBytes = bridge.dispatch(StorageKitWireCodec.encodeRequest({
    operation: operation.readiness,
    scope: {
      databaseID: "main",
      tenantID: null,
      workspaceID: null,
    },
  }));
  const response = StorageKitWireCodec.decodeResponse(responseBytes);

  assert.equal(response.status, statusCode.ok);
  assert.equal(response.operation, operation.readiness);
  assert.equal(response.schemaVersion, 1);
  assert.equal(response.commitVersion, 0n);
});

test("WASM bridge applies atomic mutations through the Swift export", { skip: !existsSync(wasmURL) }, async () => {
  const sql = new NodeSqlStorage();
  const host = new StorageKitDurableObjectHost(sql, (callback) => sql.transactionSync(callback));
  const module = await WebAssembly.compile(readFileSync(wasmURL));
  const bridge = await StorageKitWasmBridge.instantiate(module, host);
  host.setMutationApplier(bridge);

  decodeOk(bridge.dispatch(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope: {
      databaseID: "main",
      tenantID: null,
      workspaceID: null,
    },
    observedReadVersion: null,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(10) },
      { tag: 4, key: bytes(0x01), param: bytes(5), mutationType: mutationType.add },
    ],
  })));

  const response = decodeOk(bridge.dispatch(StorageKitWireCodec.encodeRequest({
    operation: operation.read,
    scope: {
      databaseID: "main",
      tenantID: null,
      workspaceID: null,
    },
    key: bytes(0x01),
    snapshot: false,
    expectedReadVersion: 1n,
  })));

  assert.deepEqual([...response.value], [15]);
});

function decodeOk(responseBytes) {
  const response = StorageKitWireCodec.decodeResponse(responseBytes);
  assert.equal(response.status, statusCode.ok);
  return response;
}

function bytes(...values) {
  return new Uint8Array(values);
}
