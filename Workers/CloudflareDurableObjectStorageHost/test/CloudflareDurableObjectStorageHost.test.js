import assert from "node:assert/strict";
import test from "node:test";
import worker from "../src/CloudflareDurableObjectStorageHost.js";
import { nameForScope } from "../src/StorageKitScope.js";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import { operation, statusCode } from "../src/StorageKitWireConstants.js";

const accessToken = "storage-kit-test-token";

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
    headers: authorizedHeaders(),
    body: requestBytes,
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
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
    headers: authorizedHeaders(),
    body: new Uint8Array([0xff]),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
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
    headers: authorizedHeaders(),
    body: requestBytes,
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
  });

  const decodedResponse = StorageKitWireCodec.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.resourceUnavailable);
});

test("worker fails closed without a configured access token", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: new Uint8Array(),
  }), {});

  assert.equal(response.status, 503);
});

test("worker rejects missing or mismatched bearer token", async () => {
  const missing = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: new Uint8Array(),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
  });
  assert.equal(missing.status, 401);

  const mismatched = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: {
      authorization: "Bearer wrong-token",
    },
    body: new Uint8Array(),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
  });
  assert.equal(mismatched.status, 401);
});

test("worker rejects oversized payloads before routing", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: authorizedHeaders({
      "content-length": "3",
    }),
    body: new Uint8Array([0x01, 0x02, 0x03]),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
    STORAGEKIT_MAX_REQUEST_BYTES: "2",
    STORAGEKIT_DURABLE_OBJECT: {
      idFromName() {
        throw new Error("unexpected routing");
      },
      get() {
        throw new Error("unexpected routing");
      },
    },
  });

  assert.equal(response.status, 413);
});

function authorizedHeaders(extra = {}) {
  return {
    authorization: `Bearer ${accessToken}`,
    "content-type": "application/octet-stream",
    ...extra,
  };
}
