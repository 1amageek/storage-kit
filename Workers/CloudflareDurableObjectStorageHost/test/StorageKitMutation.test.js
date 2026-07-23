import assert from "node:assert/strict";
import test from "node:test";
import { applyMutation } from "../src/StorageKitMutation.js";
import { mutationType } from "../src/StorageKitWireConstants.js";

test("max and min return an unchanged same-length winner without copying", () => {
  const existing = new Uint8Array([0x00, 0x02]);
  const lower = new Uint8Array([0xff, 0x01]);
  const higher = new Uint8Array([0x01, 0x02]);

  const maximum = applyMutation(existing, lower, mutationType.max);
  const minimum = applyMutation(existing, higher, mutationType.min);

  assert.strictEqual(maximum.value, existing);
  assert.strictEqual(minimum.value, existing);
});

test("max min and missing bitAnd borrow the parameter when it wins", () => {
  const param = new Uint8Array([0xff, 0x01]);

  assert.strictEqual(
    applyMutation(new Uint8Array([0x00, 0x01]), param, mutationType.max).value,
    param
  );
  assert.strictEqual(applyMutation(null, param, mutationType.min).value, param);
  assert.strictEqual(applyMutation(null, param, mutationType.bitAnd).value, param);
});

test("a truncated existing winner remains a view of its original buffer", () => {
  const backing = new Uint8Array([0x00, 0x02, 0xaa, 0xbb]);
  const existing = backing.subarray(0, 4);
  const param = new Uint8Array([0xff, 0x01]);

  const result = applyMutation(existing, param, mutationType.max).value;

  assert.strictEqual(result.buffer, existing.buffer);
  assert.equal(result.byteOffset, existing.byteOffset);
  assert.equal(result.byteLength, param.byteLength);
  assert.deepEqual([...result], [0x00, 0x02]);
});

test("zero extension allocates only when the shorter existing value wins", () => {
  const existing = new Uint8Array([0x02]);
  const param = new Uint8Array([0x01, 0x00]);

  const result = applyMutation(existing, param, mutationType.max).value;

  assert.notStrictEqual(result, existing);
  assert.deepEqual([...result], [0x02, 0x00]);
});
