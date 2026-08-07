export type StorageKitTransactionSync = <Result>(
  operation: () => Result
) => Result;

export type StoragePartitionIdentity = Readonly<{
  databaseID: string;
  tenantID: string | null;
  workspaceID: string | null;
}>;

export declare function validatePartitionIdentity(
  partitionIdentity: StoragePartitionIdentity
): StoragePartitionIdentity;

export declare function nameForPartitionIdentity(
  partitionIdentity: StoragePartitionIdentity
): string;

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
