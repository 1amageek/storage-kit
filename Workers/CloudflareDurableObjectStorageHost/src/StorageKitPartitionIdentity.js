import { encodeBase64URL } from "./StorageKitBase64URL.js";
import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

const utf8Encoder = new TextEncoder();

export function validatePartitionIdentity(partitionIdentity) {
  if (typeof partitionIdentity !== "object" || partitionIdentity === null || Array.isArray(partitionIdentity)) {
    throw StorageKitWireError.invalidPartitionIdentity();
  }
  validateComponent(partitionIdentity.databaseID);
  if (partitionIdentity.tenantID !== null) {
    validateComponent(partitionIdentity.tenantID);
  }
  if (partitionIdentity.workspaceID !== null) {
    validateComponent(partitionIdentity.workspaceID);
  }
  return partitionIdentity;
}

export function nameForPartitionIdentity(partitionIdentity) {
  validatePartitionIdentity(partitionIdentity);
  const database = encodeBase64URL(utf8Encoder.encode(partitionIdentity.databaseID));
  const tenant = partitionIdentity.tenantID === null ? "_" : encodeBase64URL(utf8Encoder.encode(partitionIdentity.tenantID));
  const workspace = partitionIdentity.workspaceID === null ? "_" : encodeBase64URL(utf8Encoder.encode(partitionIdentity.workspaceID));
  const name = `storage-kit/cfdo/v1/database/${database}/tenant/${tenant}/workspace/${workspace}`;
  if (utf8Encoder.encode(name).byteLength > storageKitWireLimits.maxCanonicalPartitionIdentityNameBytes) {
    throw StorageKitWireError.limitExceeded(
      "Canonical storage partition identity name bytes",
      storageKitWireLimits.maxCanonicalPartitionIdentityNameBytes
    );
  }
  return name;
}

function validateComponent(value) {
  if (typeof value !== "string") {
    throw StorageKitWireError.invalidPartitionIdentity();
  }
  if (hasUnpairedSurrogate(value)) {
    throw StorageKitWireError.invalidPartitionIdentity();
  }
  if (utf8Encoder.encode(value).byteLength > storageKitWireLimits.maxPartitionIdentityComponentBytes) {
    throw StorageKitWireError.limitExceeded(
      "Storage partition identity component bytes",
      storageKitWireLimits.maxPartitionIdentityComponentBytes
    );
  }
  if (isASCIIBlank(value)) {
    throw StorageKitWireError.invalidPartitionIdentity();
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x20 || code === 0x7f) {
      throw StorageKitWireError.invalidPartitionIdentity();
    }
  }
}

function hasUnpairedSurrogate(value) {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      if (index + 1 >= value.length) {
        return true;
      }
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) {
        return true;
      }
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
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
