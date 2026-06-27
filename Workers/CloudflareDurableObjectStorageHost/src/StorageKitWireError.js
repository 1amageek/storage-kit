export class StorageKitWireError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "StorageKitWireError";
    this.code = code;
  }

  static truncated() {
    return new StorageKitWireError("truncated", "Truncated binary message");
  }

  static trailingBytes() {
    return new StorageKitWireError("trailingBytes", "Trailing bytes in binary message");
  }

  static byteCountOverflow() {
    return new StorageKitWireError("byteCountOverflow", "Binary count exceeds supported bounds");
  }

  static invalidBool(value) {
    return new StorageKitWireError("invalidBool", `Invalid bool byte: ${value}`);
  }

  static invalidUTF8() {
    return new StorageKitWireError("invalidUTF8", "Invalid UTF-8 string");
  }

  static unknownOperation(value) {
    return new StorageKitWireError("unknownOperation", `Unknown operation: ${value}`);
  }

  static unknownStatus(value) {
    return new StorageKitWireError("unknownStatus", `Unknown status: ${value}`);
  }

  static unknownKeySelector(value) {
    return new StorageKitWireError("unknownKeySelector", `Unknown key selector: ${value}`);
  }

  static unknownMutationType(value) {
    return new StorageKitWireError("unknownMutationType", `Unknown mutation type: ${value}`);
  }

  static unknownWriteOperation(value) {
    return new StorageKitWireError("unknownWriteOperation", `Unknown write operation: ${value}`);
  }

  static unsupportedProtocolVersion(value) {
    return new StorageKitWireError("unsupportedProtocolVersion", `Unsupported protocol version: ${value}`);
  }

  static invalidScope() {
    return new StorageKitWireError("invalidScope", "Invalid Durable Object storage scope");
  }

  static invalidCursor() {
    return new StorageKitWireError("invalidCursor", "Invalid range cursor");
  }

  static transactionConflict() {
    return new StorageKitWireError("transactionConflict", "Observed read version does not match current committed version");
  }

  static invalidOperation(message) {
    return new StorageKitWireError("invalidOperation", message);
  }
}
