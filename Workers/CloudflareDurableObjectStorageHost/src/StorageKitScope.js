import { encodeBase64URL } from "./StorageKitBase64URL.js";
import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

const utf8Encoder = new TextEncoder();

export function validateScope(scope) {
  validateComponent(scope.databaseID);
  if (scope.tenantID !== null) {
    validateComponent(scope.tenantID);
  }
  if (scope.workspaceID !== null) {
    validateComponent(scope.workspaceID);
  }
  return scope;
}

export function nameForScope(scope) {
  validateScope(scope);
  const database = encodeBase64URL(utf8Encoder.encode(scope.databaseID));
  const tenant = scope.tenantID === null ? "_" : encodeBase64URL(utf8Encoder.encode(scope.tenantID));
  const workspace = scope.workspaceID === null ? "_" : encodeBase64URL(utf8Encoder.encode(scope.workspaceID));
  const name = `storage-kit/cfdo/v1/database/${database}/tenant/${tenant}/workspace/${workspace}`;
  if (utf8Encoder.encode(name).byteLength > storageKitWireLimits.maxCanonicalScopeNameBytes) {
    throw StorageKitWireError.limitExceeded(
      "Canonical storage scope name bytes",
      storageKitWireLimits.maxCanonicalScopeNameBytes
    );
  }
  return name;
}

function validateComponent(value) {
  if (typeof value !== "string") {
    throw StorageKitWireError.invalidScope();
  }
  if (utf8Encoder.encode(value).byteLength > storageKitWireLimits.maxScopeComponentBytes) {
    throw StorageKitWireError.limitExceeded(
      "Storage scope component bytes",
      storageKitWireLimits.maxScopeComponentBytes
    );
  }
  if (isASCIIBlank(value)) {
    throw StorageKitWireError.invalidScope();
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x20 || code === 0x7f) {
      throw StorageKitWireError.invalidScope();
    }
  }
}

function isASCIIBlank(value) {
  if (value.length === 0) {
    return true;
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (![0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x20].includes(code)) {
      return false;
    }
  }
  return true;
}
