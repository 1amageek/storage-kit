import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

export class StorageKitWireWriter {
  constructor(
    maximumBytes = storageKitWireLimits.maxFrameBytes,
    initialCapacity = Math.min(256, maximumBytes),
    measuring = false
  ) {
    this.maximumBytes = maximumBytes;
    this.measuring = measuring;
    this.buffer = measuring
      ? null
      : new Uint8Array(Math.min(initialCapacity, maximumBytes));
    this.length = 0;
    this.encoder = new TextEncoder();
  }

  static measuring(maximumBytes = storageKitWireLimits.maxFrameBytes) {
    return new StorageKitWireWriter(maximumBytes, 0, true);
  }

  static encodeExact(
    encode,
    maximumBytes = storageKitWireLimits.maxFrameBytes
  ) {
    const measuringWriter = StorageKitWireWriter.measuring(maximumBytes);
    encode(measuringWriter);
    const writer = new StorageKitWireWriter(
      maximumBytes,
      measuringWriter.length
    );
    encode(writer);
    if (writer.length !== measuringWriter.length) {
      throw new Error("Wire encoding changed between sizing and writing");
    }
    return writer.toBytes();
  }

  writeUInt8(value) {
    this.ensureCapacity(1);
    if (!this.measuring) {
      this.buffer[this.length] = value & 0xff;
    }
    this.length += 1;
  }

  writeBool(value) {
    this.writeUInt8(value ? 1 : 0);
  }

  writeUInt32(value) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw StorageKitWireError.byteCountOverflow();
    }
    this.ensureCapacity(4);
    if (!this.measuring) {
      this.buffer[this.length] = value & 0xff;
      this.buffer[this.length + 1] = (value >>> 8) & 0xff;
      this.buffer[this.length + 2] = (value >>> 16) & 0xff;
      this.buffer[this.length + 3] = (value >>> 24) & 0xff;
    }
    this.length += 4;
  }

  writeInt32(value) {
    this.writeUInt32(value >>> 0);
  }

  writeInt64(value) {
    this.ensureCapacity(8);
    let unsigned = BigInt.asUintN(64, BigInt(value));
    if (!this.measuring) {
      for (let index = 0; index < 8; index += 1) {
        this.buffer[this.length + index] = Number(unsigned & 0xffn);
        unsigned >>= 8n;
      }
    }
    this.length += 8;
  }

  writeBytes(value) {
    const byteLength = sourceByteLength(value);
    this.writeUInt32(byteLength);
    this.ensureCapacity(byteLength);
    if (!this.measuring) {
      this.buffer.set(wireBytes(value), this.length);
    }
    this.length += byteLength;
  }

  writeString(value) {
    const byteLength = utf8ByteLength(value);
    this.writeUInt32(byteLength);
    this.ensureCapacity(byteLength);
    if (!this.measuring) {
      const destination = this.buffer.subarray(
        this.length,
        this.length + byteLength
      );
      const result = this.encoder.encodeInto(value, destination);
      if (result.read !== value.length || result.written !== byteLength) {
        throw new Error("UTF-8 encoding length mismatch");
      }
    }
    this.length += byteLength;
  }

  toBytes() {
    if (this.measuring) {
      throw new Error("A measuring writer does not own encoded bytes");
    }
    return this.buffer.subarray(0, this.length);
  }

  ensureCapacity(additionalBytes) {
    const required = this.length + additionalBytes;
    if (!Number.isSafeInteger(required) || required > this.maximumBytes) {
      throw StorageKitWireError.limitExceeded("Wire frame bytes", this.maximumBytes);
    }
    if (this.measuring) {
      return;
    }
    if (required <= this.buffer.byteLength) {
      return;
    }
    let capacity = Math.max(1, this.buffer.byteLength);
    while (capacity < required) {
      capacity = Math.min(this.maximumBytes, capacity * 2);
      if (capacity < required && capacity === this.maximumBytes) {
        throw StorageKitWireError.limitExceeded("Wire frame bytes", this.maximumBytes);
      }
    }
    const replacement = new Uint8Array(capacity);
    replacement.set(this.buffer.subarray(0, this.length));
    this.buffer = replacement;
  }
}

function sourceByteLength(value) {
  if (value instanceof Uint8Array) {
    return value.byteLength;
  }
  if (ArrayBuffer.isView(value)) {
    return value.byteLength;
  }
  if (value instanceof ArrayBuffer) {
    return value.byteLength;
  }
  return value.length;
}

function wireBytes(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  return Uint8Array.from(value);
}

function utf8ByteLength(value) {
  let byteLength = 0;
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit <= 0x7f) {
      byteLength += 1;
      continue;
    }
    if (codeUnit <= 0x7ff) {
      byteLength += 2;
      continue;
    }
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        byteLength += 4;
        index += 1;
        continue;
      }
    }
    byteLength += 3;
  }
  return byteLength;
}
