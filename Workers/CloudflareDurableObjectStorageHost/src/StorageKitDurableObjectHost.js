import { StorageKitSQLiteStore } from "./StorageKitSQLiteStore.js";
import { StorageKitWire } from "./StorageKitWire.js";
import { statusCode } from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

export class StorageKitDurableObjectHost {
  constructor(sql, transactionSync) {
    this.store = new StorageKitSQLiteStore(sql, transactionSync);
  }

  migrate() {
    this.store.migrate();
  }

  dispatchBytes(bytes) {
    try {
      const request = StorageKitWire.decodeRequest(bytes);
      const response = this.store.dispatch(request);
      return StorageKitWire.encodeResponse(response);
    } catch (error) {
      return StorageKitWire.encodeFailure(statusForError(error), error.message);
    }
  }
}

function statusForError(error) {
  if (error instanceof StorageKitWireError) {
    switch (error.code) {
      case "transactionConflict":
        return statusCode.transactionConflict;
      case "resourceUnavailable":
        return statusCode.resourceUnavailable;
      case "backendContractViolation":
        return statusCode.backendContractViolation;
      case "invalidOperation":
      case "invalidScope":
      case "scopeMismatch":
      case "invalidRangeContinuation":
      case "limitExceeded":
      case "unsupportedProtocolVersion":
      case "unknownOperation":
      case "unknownStatus":
      case "unknownRangeBoundary":
      case "unknownMutationType":
      case "unknownWriteOperation":
      case "invalidBool":
      case "invalidUTF8":
      case "trailingBytes":
      case "byteCountOverflow":
      case "truncated":
        return statusCode.invalidOperation;
      default:
        return statusCode.backendFailure;
    }
  }
  return statusCode.backendFailure;
}
