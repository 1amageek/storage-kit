import assert from "node:assert/strict";
import test from "node:test";
import worker from "../src/CloudflareDurableObjectStorageHost.js";
import { nameForScope } from "../src/StorageKitScope.js";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import { operation, statusCode } from "../src/StorageKitWireConstants.js";

test("worker routes binary requests to the Durable Object name derived from scope", async () => {
  const scope = {
    databaseID: "main",
    tenantID: "tenant-a",
    workspaceID: "workspace-a",
  };
  const requestBytes = StorageKitWireCodec.encodeRequest({
    operation: operation.readiness,
    scope,
  });

  let observedName = null;
  let observedBody = null;
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: requestBytes,
  }), {
    STORAGEKIT_DURABLE_OBJECT: {
      idFromName(name) {
        observedName = name;
        return { name };
      },
      get() {
        return {
          async fetch(request) {
            observedBody = new Uint8Array(await request.arrayBuffer());
            return new Response(StorageKitWireCodec.encodeResponse({
              status: statusCode.ok,
              operation: operation.readiness,
              schemaVersion: 1,
              commitVersion: 0n,
              metadataInitialized: false,
            }), {
              headers: {
                "content-type": "application/octet-stream",
              },
            });
          },
        };
      },
    },
  });

  assert.equal(observedName, nameForScope(scope));
  assert.deepEqual([...observedBody], [...requestBytes]);
  assert.equal(response.headers.get("content-type"), "application/octet-stream");

  const decodedResponse = StorageKitWireCodec.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.ok);
  assert.equal(decodedResponse.operation, operation.readiness);
});

test("worker returns a typed failure when routing cannot decode scope", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: new Uint8Array([0xff]),
  }), {
    STORAGEKIT_DURABLE_OBJECT: {
      idFromName() {
        throw new Error("unexpected routing");
      },
      get() {
        throw new Error("unexpected routing");
      },
    },
  });

  const decodedResponse = StorageKitWireCodec.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.invalidOperation);
});

test("worker returns a typed failure when the Durable Object binding is absent", async () => {
  const requestBytes = StorageKitWireCodec.encodeRequest({
    operation: operation.readiness,
    scope: {
      databaseID: "main",
      tenantID: null,
      workspaceID: null,
    },
  });

  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: requestBytes,
  }), {});

  const decodedResponse = StorageKitWireCodec.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.resourceUnavailable);
});
