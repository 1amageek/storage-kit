import { encodeBase64URL } from "./StorageKitBase64URL.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

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
  const encoder = new TextEncoder();
  const database = encodeBase64URL(encoder.encode(scope.databaseID));
  const tenant = scope.tenantID === null ? "_" : encodeBase64URL(encoder.encode(scope.tenantID));
  const workspace = scope.workspaceID === null ? "_" : encodeBase64URL(encoder.encode(scope.workspaceID));
  return `storage-kit/cfdo/v1/database/${database}/tenant/${tenant}/workspace/${workspace}`;
}

function validateComponent(value) {
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
