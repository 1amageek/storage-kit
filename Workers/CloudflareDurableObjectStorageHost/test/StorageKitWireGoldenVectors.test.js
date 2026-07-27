import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { StorageKitWireCodec } from "../src/StorageKitWireCodec.js";
import {
  mutationType,
  operation,
  statusCode,
} from "../src/StorageKitWireConstants.js";

const fixtureURL = new URL(
  "../../../Tests/CloudflareDurableObjectStorageWireTests/GoldenVectors/StorageKitWireV1.json",
  import.meta.url
);
const vectors = JSON.parse(readFileSync(fileURLToPath(fixtureURL), "utf8"));
const scope = Object.freeze({ databaseID: "main", tenantID: null, workspaceID: null });

test("JavaScript encoder matches canonical StorageKit Wire v1 vectors", () => {
  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.readiness,
    scope,
  })), vectors.readinessRequest);

  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.range,
    scope,
    begin: null,
    end: { key: bytes(0x20), orEqual: true, offset: 1n },
    limit: 2,
    reverse: false,
    snapshot: false,
    expectedReadVersion: 7n,
    cursorKey: bytes(0xFF),
  })), vectors.rangeRequest);

  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: 7n,
    mutations: [
      { tag: 1, key: bytes(0x01), value: bytes(0x0A, 0x0B) },
      { tag: 3, begin: bytes(0x02), end: bytes(0x04) },
      { tag: 4, key: bytes(0x05), param: bytes(0x01), mutationType: mutationType.add },
    ],
    readConflictRanges: [
      { begin: null, end: bytes(0x09) },
    ],
    writeConflictRanges: [
      { begin: bytes(0x05), end: bytes(0x05, 0x00) },
    ],
  })), vectors.commitRequest);

  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: 8n,
    mutations: [
      {
        tag: 4,
        key: bytes(0x20),
        param: bytes(
          0xAA,
          0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
          0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
          0xBB,
          0x01, 0x00, 0x00, 0x00
        ),
        mutationType: mutationType.setVersionstampedValue,
      },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  })), vectors.versionstampedCommitRequest);

  assert.equal(hex(StorageKitWireCodec.encodeResponse({
    status: statusCode.ok,
    operation: operation.range,
    rows: [
      { key: bytes(0x01), value: bytes(0x0A) },
      { key: bytes(0x02), value: bytes(0x0B, 0x0C) },
    ],
    hasMore: true,
    currentCommitVersion: 8n,
    readConflictRanges: [
      { begin: bytes(0x01), end: bytes(0x02) },
      { begin: bytes(0x05), end: null },
    ],
  })), vectors.rangeResponse);

  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.rangeSize,
    scope,
    begin: bytes(0x01),
    end: bytes(0x04),
    expectedReadVersion: 7n,
  })), vectors.rangeSizeRequest);

  assert.equal(hex(StorageKitWireCodec.encodeResponse({
    status: statusCode.ok,
    operation: operation.rangeSize,
    byteCount: 11n,
    currentCommitVersion: 8n,
  })), vectors.rangeSizeResponse);

  assert.equal(hex(StorageKitWireCodec.encodeRequest({
    operation: operation.rangeSplitPoints,
    scope,
    begin: bytes(0x01),
    end: bytes(0x04),
    chunkSize: 6n,
    expectedReadVersion: 7n,
  })), vectors.rangeSplitPointsRequest);

  assert.equal(hex(StorageKitWireCodec.encodeResponse({
    status: statusCode.ok,
    operation: operation.rangeSplitPoints,
    splitPoints: [bytes(0x01), bytes(0x03), bytes(0x04)],
    currentCommitVersion: 8n,
  })), vectors.rangeSplitPointsResponse);

  assert.equal(
    hex(StorageKitWireCodec.encodeFailure(statusCode.transactionConflict, "conflict")),
    vectors.failureResponse
  );
  assert.equal(
    hex(StorageKitWireCodec.encodeFailure(
      statusCode.backendContractViolation,
      "sqlite cursor contract"
    )),
    vectors.backendContractFailureResponse
  );
});

test("JavaScript decoder reencodes every canonical vector", () => {
  for (const name of [
    "readinessRequest",
    "rangeRequest",
    "commitRequest",
    "versionstampedCommitRequest",
    "rangeSizeRequest",
    "rangeSplitPointsRequest",
  ]) {
    const encoded = fromHex(vectors[name]);
    assert.deepEqual(
      StorageKitWireCodec.encodeRequest(StorageKitWireCodec.decodeRequest(encoded)),
      encoded
    );
  }
  for (const name of [
    "rangeResponse",
    "rangeSizeResponse",
    "rangeSplitPointsResponse",
    "failureResponse",
    "backendContractFailureResponse",
  ]) {
    const encoded = fromHex(vectors[name]);
    assert.deepEqual(
      StorageKitWireCodec.encodeResponse(StorageKitWireCodec.decodeResponse(encoded)),
      encoded
    );
  }
});

test("shared invalid success frame is rejected", () => {
  for (const name of [
    "invalidSuccessWithoutOperation",
    "invalidNegativeReadinessVersion",
    "invalidEmptyRangeContinuation",
  ]) {
    assert.throws(
      () => StorageKitWireCodec.decodeResponse(fromHex(vectors[name]))
    );
  }
  assert.throws(
    () => StorageKitWireCodec.decodeRequest(
      fromHex(vectors.invalidUnknownAtomicMutation)
    )
  );
});

test("range cursor decoding borrows the canonical request buffer", () => {
  const encoded = fromHex(vectors.rangeRequest);
  const backing = new Uint8Array(encoded.byteLength + 2);
  backing.set(encoded, 1);
  const frame = backing.subarray(1, backing.byteLength - 1);

  const request = StorageKitWireCodec.decodeRequest(frame);

  assert.deepEqual([...request.cursorKey], [0xFF]);
  assert.strictEqual(request.cursorKey.buffer, backing.buffer);
});

test("range response encoder rejects an empty continuation page", () => {
  assert.throws(
    () => StorageKitWireCodec.encodeResponse({
      status: statusCode.ok,
      operation: operation.range,
      rows: [],
      hasMore: true,
      currentCommitVersion: 0n,
      readConflictRanges: [],
    }),
    /cannot continue without returning a row/
  );
});

test("JavaScript encoder rejects unknown atomic mutation types", () => {
  assert.throws(() => StorageKitWireCodec.encodeRequest({
    operation: operation.commit,
    scope,
    observedReadVersion: 0n,
    mutations: [
      {
        tag: 4,
        key: bytes(0x01),
        param: bytes(0x01),
        mutationType: 255,
      },
    ],
    readConflictRanges: [],
    writeConflictRanges: [],
  }));
});

function bytes(...values) {
  return new Uint8Array(values);
}

function hex(value) {
  return Buffer.from(value).toString("hex");
}

function fromHex(value) {
  return new Uint8Array(Buffer.from(value, "hex"));
}
