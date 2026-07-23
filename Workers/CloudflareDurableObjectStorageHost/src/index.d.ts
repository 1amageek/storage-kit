export type StorageKitTransactionSync = <Result>(
  operation: () => Result
) => Result;

export declare class StorageKitDurableObjectHost {
  constructor(
    sql: SqlStorage,
    transactionSync: StorageKitTransactionSync
  );

  migrate(): void;
  dispatchBytes(bytes: Uint8Array): Uint8Array;
}
