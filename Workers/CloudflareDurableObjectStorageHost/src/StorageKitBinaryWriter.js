import { StorageKitWireError } from "./StorageKitWireError.js";

export class StorageKitBinaryWriter {
  constructor() {
    this.bytes = [];
    this.encoder = new TextEncoder();
  }

  writeUInt8(value) {
    this.bytes.push(value & 0xff);
  }

  writeBool(value) {
    this.writeUInt8(value ? 1 : 0);
  }

  writeUInt32(value) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw StorageKitWireError.byteCountOverflow();
    }
    this.bytes.push(value & 0xff);
    this.bytes.push((value >>> 8) & 0xff);
    this.bytes.push((value >>> 16) & 0xff);
    this.bytes.push((value >>> 24) & 0xff);
  }

  writeInt32(value) {
    this.writeUInt32(value >>> 0);
  }

  writeInt64(value) {
    let unsigned = BigInt.asUintN(64, BigInt(value));
    for (let index = 0; index < 8; index += 1) {
      this.bytes.push(Number(unsigned & 0xffn));
      unsigned >>= 8n;
    }
  }

  writeBytes(value) {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
    this.writeUInt32(bytes.length);
    for (const byte of bytes) {
      this.bytes.push(byte);
    }
  }

  writeString(value) {
    this.writeBytes(this.encoder.encode(value));
  }

  toBytes() {
    return new Uint8Array(this.bytes);
  }
}
