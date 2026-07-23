export type StorageKitTransactionSync = <Result>(
  operation: () => Result
) => Result;

/** Minimal synchronous SQL contract consumed by the StorageKit host. */
export type StorageKitSQLBinding =
  | null
  | number
  | bigint
  | string
  | ArrayBuffer
  | ArrayBufferView;

export type StorageKitSQLStorage = {
  exec(statement: string, ...bindings: StorageKitSQLBinding[]): unknown;
};

export declare class StorageKitDurableObjectHost {
  constructor(
    sql: StorageKitSQLStorage,
    transactionSync: StorageKitTransactionSync
  );

  migrate(): void;
  dispatchBytes(bytes: Uint8Array): Uint8Array;
}
