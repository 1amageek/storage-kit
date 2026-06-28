import assert from "node:assert/strict";
import test from "node:test";
import {
  readBoundedRequestBytes,
  StorageKitPayloadTooLargeError,
} from "../src/StorageKitHostLimits.js";

test("bounded request reader cancels the stream after exceeding the limit", async () => {
  let canceled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([0x01, 0x02]));
      controller.enqueue(new Uint8Array([0x03, 0x04]));
    },
    cancel() {
      canceled = true;
    },
  });

  await assert.rejects(
    readBoundedRequestBytes({
      headers: new Headers(),
      body: stream,
    }, 3),
    StorageKitPayloadTooLargeError
  );
  assert.equal(canceled, true);
});
