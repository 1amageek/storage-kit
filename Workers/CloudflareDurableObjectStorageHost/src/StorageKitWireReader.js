import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

export class StorageKitWireReader {
  constructor(bytes) {
    this.bytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    if (this.bytes.byteLength > storageKitWireLimits.maxFrameBytes) {
      throw StorageKitWireError.limitExceeded(
        "Wire frame bytes",
        storageKitWireLimits.maxFrameBytes
      );
    }
    this.offset = 0;
    this.decoder = new TextDecoder("utf-8", { fatal: true });
  }

  ensureFullyRead() {
    if (this.offset !== this.bytes.length) {
      throw StorageKitWireError.trailingBytes();
    }
  }

  readUInt8() {
    if (this.offset >= this.bytes.length) {
      throw StorageKitWireError.truncated();
    }
    const value = this.bytes[this.offset];
    this.offset += 1;
    return value;
  }

  readBool() {
    const value = this.readUInt8();
    if (value === 0) {
      return false;
    }
    if (value === 1) {
      return true;
    }
    throw StorageKitWireError.invalidBool(value);
  }

  readUInt32() {
    this.require(4);
    const value = (this.bytes[this.offset]
      | (this.bytes[this.offset + 1] << 8)
      | (this.bytes[this.offset + 2] << 16)
      | (this.bytes[this.offset + 3] << 24)) >>> 0;
    this.offset += 4;
    return value;
  }

  readInt32() {
    return this.readUInt32() | 0;
  }

  readInt64() {
    this.require(8);
    let value = 0n;
    let shift = 0n;
    for (let index = 0; index < 8; index += 1) {
      value |= BigInt(this.bytes[this.offset + index]) << shift;
      shift += 8n;
    }
    this.offset += 8;
    return BigInt.asIntN(64, value);
  }

  readBytes(maximum = storageKitWireLimits.maxFrameBytes, field = "Byte string") {
    const count = this.readUInt32();
    if (count > maximum) {
      throw StorageKitWireError.limitExceeded(field, maximum);
    }
    if (count > this.bytes.length - this.offset) {
      throw StorageKitWireError.truncated();
    }
    const value = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return value;
  }

  readString(
    maximum = storageKitWireLimits.maxScopeComponentBytes,
    field = "UTF-8 string"
  ) {
    const bytes = this.readBytes(maximum, field);
    try {
      return this.decoder.decode(bytes);
    } catch {
      throw StorageKitWireError.invalidUTF8();
    }
  }

  require(byteCount) {
    if (byteCount < 0 || byteCount > this.bytes.length - this.offset) {
      throw StorageKitWireError.truncated();
    }
  }
}
