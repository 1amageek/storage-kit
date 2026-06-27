import { StorageKitWireError } from "./StorageKitWireError.js";

export class StorageKitWasmBridge {
  static async instantiate(wasmModule, host) {
    const bridge = new StorageKitWasmBridge(host);
    const instance = await WebAssembly.instantiate(wasmModule, {
      storagekit_host: {
        dispatch: (pointer, length) => bridge.dispatchFromWasm(pointer, length),
      },
      wasi_snapshot_preview1: {
        args_get: () => 0,
        args_sizes_get: (argcPointer, argvBufferSizePointer) => {
          bridge.writeUInt32(argcPointer, 0);
          bridge.writeUInt32(argvBufferSizePointer, 0);
          return 0;
        },
        proc_exit: (code) => {
          throw new Error(`WASM proc_exit(${code})`);
        },
        random_get: (pointer, length) => {
          bridge.fillRandom(pointer, length);
          return 0;
        },
      },
    });
    bridge.instance = instance instanceof WebAssembly.Instance ? instance : instance.instance;
    return bridge;
  }

  constructor(host) {
    this.host = host;
    this.instance = null;
  }

  dispatch(bytes) {
    const exports = this.requireExports();
    const requestBytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const requestPointer = exports.storagekit_alloc(requestBytes.length);
    this.writeBytes(requestPointer, requestBytes);
    try {
      const framePointer = exports.storagekit_dispatch(requestPointer, requestBytes.length);
      return this.takeFrame(framePointer);
    } finally {
      exports.storagekit_dealloc(requestPointer, requestBytes.length);
    }
  }

  dispatchFromWasm(pointer, length) {
    const response = this.host.dispatchBytes(this.readBytes(pointer, length));
    return this.makeFrame(response);
  }

  applyMutation(existing, param, type) {
    const exports = this.requireExports();
    const existingBytes = existing === null ? null : bytesView(existing);
    const paramBytes = bytesView(param);
    const existingPointer = existingBytes === null || existingBytes.length === 0
      ? 0
      : exports.storagekit_alloc(existingBytes.length);
    const paramPointer = paramBytes.length === 0 ? 0 : exports.storagekit_alloc(paramBytes.length);
    if (existingBytes !== null) {
      this.writeBytes(existingPointer, existingBytes);
    }
    this.writeBytes(paramPointer, paramBytes);
    try {
      const framePointer = exports.storagekit_apply_mutation(
        existingBytes === null ? 0 : 1,
        existingPointer,
        existingBytes === null ? 0 : existingBytes.length,
        paramPointer,
        paramBytes.length,
        type
      );
      const payload = this.takeFrame(framePointer);
      return decodeMutationResult(payload);
    } finally {
      if (existingBytes !== null) {
        exports.storagekit_dealloc(existingPointer, existingBytes.length);
      }
      exports.storagekit_dealloc(paramPointer, paramBytes.length);
    }
  }

  makeFrame(payload) {
    const exports = this.requireExports();
    const frameLength = payload.length + 4;
    const pointer = exports.storagekit_alloc(frameLength);
    const memory = this.memory();
    const view = new Uint8Array(memory.buffer, pointer, frameLength);
    view[0] = payload.length & 0xff;
    view[1] = (payload.length >>> 8) & 0xff;
    view[2] = (payload.length >>> 16) & 0xff;
    view[3] = (payload.length >>> 24) & 0xff;
    view.set(payload, 4);
    return pointer;
  }

  takeFrame(pointer) {
    if (pointer === 0) {
      throw StorageKitWireError.invalidOperation("WASM dispatch returned no response frame");
    }
    const exports = this.requireExports();
    const memory = this.memory();
    const header = new Uint8Array(memory.buffer, pointer, 4);
    const length = (header[0]
      | (header[1] << 8)
      | (header[2] << 16)
      | (header[3] << 24)) >>> 0;
    const payload = new Uint8Array(memory.buffer, pointer + 4, length).slice();
    exports.storagekit_dealloc(pointer, length + 4);
    return payload;
  }

  readBytes(pointer, length) {
    return new Uint8Array(this.memory().buffer, pointer, length).slice();
  }

  writeBytes(pointer, bytes) {
    if (bytes.length === 0) {
      return;
    }
    new Uint8Array(this.memory().buffer, pointer, bytes.length).set(bytes);
  }

  writeUInt32(pointer, value) {
    if (this.instance === null) {
      return;
    }
    const view = new Uint8Array(this.memory().buffer, pointer, 4);
    view[0] = value & 0xff;
    view[1] = (value >>> 8) & 0xff;
    view[2] = (value >>> 16) & 0xff;
    view[3] = (value >>> 24) & 0xff;
  }

  fillRandom(pointer, length) {
    if (this.instance === null) {
      return;
    }
    const view = new Uint8Array(this.memory().buffer, pointer, length);
    if (globalThis.crypto?.getRandomValues !== undefined) {
      globalThis.crypto.getRandomValues(view);
      return;
    }
    view.fill(0);
  }

  memory() {
    const memory = this.requireExports().memory;
    if (!(memory instanceof WebAssembly.Memory)) {
      throw StorageKitWireError.invalidOperation("WASM instance does not export memory");
    }
    return memory;
  }

  requireExports() {
    if (this.instance === null) {
      throw StorageKitWireError.invalidOperation("WASM bridge is not initialized");
    }
    return this.instance.exports;
  }
}

function bytesView(value) {
  return value instanceof Uint8Array ? value : new Uint8Array(value);
}

function decodeMutationResult(payload) {
  if (payload.length < 2 || payload[0] !== 0) {
    throw StorageKitWireError.invalidOperation("WASM atomic mutation failed");
  }
  switch (payload[1]) {
    case 1:
      return { kind: "set", value: readResultBytes(payload) };
    case 2:
      return { kind: "clear" };
    case 3:
      return { kind: "unchanged" };
    default:
      throw StorageKitWireError.invalidOperation("WASM atomic mutation returned an unknown result");
  }
}

function readResultBytes(payload) {
  if (payload.length < 6) {
    throw StorageKitWireError.invalidOperation("WASM atomic mutation returned a truncated value");
  }
  const length = (payload[2]
    | (payload[3] << 8)
    | (payload[4] << 16)
    | (payload[5] << 24)) >>> 0;
  if (payload.length !== length + 6) {
    throw StorageKitWireError.invalidOperation("WASM atomic mutation returned an invalid value length");
  }
  return payload.slice(6);
}
