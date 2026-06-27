import { StorageKitSQLiteStore } from "./StorageKitSQLiteStore.js";
import { StorageKitWireCodec } from "./StorageKitWireCodec.js";
import { statusCode } from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

export class StorageKitDurableObjectHost {
  constructor(sql, transactionSync = null) {
    this.store = new StorageKitSQLiteStore(sql, transactionSync);
  }

  dispatchBytes(bytes) {
    try {
      const request = StorageKitWireCodec.decodeRequest(bytes);
      const response = this.store.dispatch(request);
      return StorageKitWireCodec.encodeResponse(response);
    } catch (error) {
      return StorageKitWireCodec.encodeFailure(statusForError(error), error.message);
    }
  }

  setMutationApplier(mutationApplier) {
    this.store.setMutationApplier(mutationApplier);
  }
}

function statusForError(error) {
  if (error instanceof StorageKitWireError) {
    switch (error.code) {
      case "transactionConflict":
        return statusCode.transactionConflict;
      case "invalidOperation":
      case "invalidScope":
      case "invalidCursor":
      case "unsupportedProtocolVersion":
      case "unknownOperation":
      case "unknownStatus":
      case "unknownKeySelector":
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
