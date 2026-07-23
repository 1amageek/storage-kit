import assert from "node:assert/strict";
import test from "node:test";
import { StorageKitSQLiteStore } from "../src/StorageKitSQLiteStore.js";

const unsupportedCursorSQL = Object.freeze({
  exec() {
    return null;
  },
});

test("all rejects an unsupported SQLite cursor", () => {
  const store = new StorageKitSQLiteStore(unsupportedCursorSQL, (operation) => operation());

  assert.throws(
    () => store.all("SELECT 1"),
    (error) => error.code === "backendContractViolation"
  );
});

test("iterate rejects an unsupported SQLite cursor", () => {
  const store = new StorageKitSQLiteStore(unsupportedCursorSQL, (operation) => operation());

  assert.throws(
    () => store.iterate("SELECT 1"),
    (error) => error.code === "backendContractViolation"
  );
});
