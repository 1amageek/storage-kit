import assert from "node:assert/strict";
import test from "node:test";
import {
  readBoundedRequestBytes,
  TestStoragePayloadTooLargeError,
} from "./fixtures/TestStorageRequestLimits.js";

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
    TestStoragePayloadTooLargeError
  );
  assert.equal(canceled, true);
});

test("bounded request reader returns one offset chunk by identity", async () => {
  const backing = new Uint8Array([0x00, 0x01, 0x02, 0x03]);
  const chunk = backing.subarray(1, 3);
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
      controller.close();
    },
  });

  const result = await readBoundedRequestBytes({
    headers: new Headers(),
    body: stream,
  }, 4);

  assert.strictEqual(result, chunk);
  assert.equal(result.byteOffset, 1);
  assert.deepEqual([...result], [0x01, 0x02]);
});

test("bounded request reader consolidates multiple chunks once", async () => {
  const first = new Uint8Array([0x01, 0x02]);
  const second = new Uint8Array([0x03, 0x04]);
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(first);
      controller.enqueue(second);
      controller.close();
    },
  });

  const result = await readBoundedRequestBytes({
    headers: new Headers(),
    body: stream,
  }, 4);

  assert.notStrictEqual(result, first);
  assert.notStrictEqual(result, second);
  assert.deepEqual([...result], [0x01, 0x02, 0x03, 0x04]);
});
