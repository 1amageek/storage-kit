export class StorageKitWireError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "StorageKitWireError";
    this.code = code;
  }

  static truncated() {
    return new StorageKitWireError("truncated", "Truncated StorageKit Wire frame");
  }

  static trailingBytes() {
    return new StorageKitWireError("trailingBytes", "Trailing bytes in StorageKit Wire frame");
  }

  static byteCountOverflow() {
    return new StorageKitWireError("byteCountOverflow", "Encoded byte count exceeds supported bounds");
  }

  static limitExceeded(field, limit) {
    return new StorageKitWireError(
      "limitExceeded",
      `${field} exceeds the configured limit of ${limit}`
    );
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

  static unknownRangeBoundary(value) {
    return new StorageKitWireError("unknownRangeBoundary", `Unknown range boundary: ${value}`);
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

  static invalidPartitionIdentity() {
    return new StorageKitWireError("invalidPartitionIdentity", "Invalid Durable Object storage partition identity");
  }

  static invalidRangeContinuation() {
    return new StorageKitWireError(
      "invalidRangeContinuation",
      "A range response cannot continue without returning a row"
    );
  }

  static transactionConflict() {
    return new StorageKitWireError("transactionConflict", "Observed read version does not match current committed version");
  }

  static partitionIdentityMismatch() {
    return new StorageKitWireError(
      "partitionIdentityMismatch",
      "Storage partition identity does not match this Durable Object"
    );
  }

  static invalidOperation(message) {
    return new StorageKitWireError("invalidOperation", message);
  }

  static resourceUnavailable(message) {
    return new StorageKitWireError("resourceUnavailable", message);
  }

  static backendContractViolation(message) {
    return new StorageKitWireError("backendContractViolation", message);
  }
}
