import assert from "node:assert/strict";
import test from "node:test";
import worker from "./fixtures/TestStorageGateway.js";
import { nameForScope } from "../src/StorageKitScope.js";
import { StorageKitWire } from "../src/StorageKitWire.js";
import { operation, statusCode } from "../src/StorageKitWireConstants.js";

const accessToken = "storage-kit-test-token";

test("fixture routes StorageKit Wire requests to the Durable Object name derived from scope", async () => {
  const scope = {
    databaseID: "main",
    tenantID: "tenant-a",
    workspaceID: "workspace-a",
  };
  const requestBytes = StorageKitWire.encodeRequest({
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
          execute(requestBytes) {
            observedBody = requestBytes;
            return StorageKitWire.encodeResponse({
              status: statusCode.ok,
              operation: operation.readiness,
              schemaVersion: 1,
              commitVersion: 0n,
              metadataInitialized: false,
            });
          },
        };
      },
    },
  });

  assert.equal(observedName, nameForScope(scope));
  assert.deepEqual([...observedBody], [...requestBytes]);
  assert.equal(response.headers.get("content-type"), "application/octet-stream");

  const decodedResponse = StorageKitWire.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.ok);
  assert.equal(decodedResponse.operation, operation.readiness);
});

test("fixture returns a typed failure when routing cannot decode scope", async () => {
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

  const decodedResponse = StorageKitWire.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.invalidOperation);
});

test("fixture rejects removed operation 7 before Durable Object routing", async () => {
  let routed = false;
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: authorizedHeaders(),
    body: new Uint8Array([1, 0x07]),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
    STORAGEKIT_DURABLE_OBJECT: {
      idFromName() {
        routed = true;
        throw new Error("unexpected routing");
      },
      get() {
        routed = true;
        throw new Error("unexpected routing");
      },
    },
  });

  const decodedResponse = StorageKitWire.decodeResponse(
    new Uint8Array(await response.arrayBuffer())
  );
  assert.equal(decodedResponse.status, statusCode.invalidOperation);
  assert.match(decodedResponse.message, /Unknown operation: 7/);
  assert.equal(routed, false);
});

test("fixture decodes only routing scope before Durable Object dispatch", async () => {
  const validPrefix = StorageKitWire.encodeRequest({
    operation: operation.readiness,
    scope: {
      databaseID: "main",
      tenantID: null,
      workspaceID: null,
    },
  });
  const requestBytes = new Uint8Array(validPrefix.length + 1);
  requestBytes.set(validPrefix);
  requestBytes[requestBytes.length - 1] = 0xff;
  let forwarded = false;

  await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: authorizedHeaders(),
    body: requestBytes,
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
    STORAGEKIT_DURABLE_OBJECT: {
      idFromName() {
        return { name: "main" };
      },
      get() {
        return {
          execute() {
            forwarded = true;
            return StorageKitWire.encodeFailure(
              statusCode.invalidOperation,
              "Trailing bytes"
            );
          },
        };
      },
    },
  });

  assert.equal(forwarded, true);
});

test("fixture returns a typed failure when the Durable Object binding is absent", async () => {
  const requestBytes = StorageKitWire.encodeRequest({
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

  const decodedResponse = StorageKitWire.decodeResponse(new Uint8Array(await response.arrayBuffer()));
  assert.equal(decodedResponse.status, statusCode.resourceUnavailable);
});

test("fixture fails closed without a configured access token", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    body: new Uint8Array(),
  }), {});

  assert.equal(response.status, 503);
});

test("fixture rejects missing or mismatched bearer token", async () => {
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

test("fixture rejects oversized payloads before routing", async () => {
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

test("fixture requires the StorageKit Wire media type", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: {
      authorization: `Bearer ${accessToken}`,
      "content-type": "application/json",
    },
    body: new Uint8Array(),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
  });

  assert.equal(response.status, 415);
});

test("fixture fails closed for an invalid request-size configuration", async () => {
  const response = await worker.fetch(new Request("https://storage-kit.example.test/", {
    method: "POST",
    headers: authorizedHeaders(),
    body: new Uint8Array(),
  }), {
    STORAGEKIT_ACCESS_TOKEN: accessToken,
    STORAGEKIT_MAX_REQUEST_BYTES: "0",
  });

  assert.equal(response.status, 500);
});

function authorizedHeaders(extra = {}) {
  return {
    authorization: `Bearer ${accessToken}`,
    "content-type": "application/octet-stream",
    ...extra,
  };
}
