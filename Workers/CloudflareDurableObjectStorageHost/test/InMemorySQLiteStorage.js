import { DatabaseSync } from "node:sqlite";

export class InMemorySQLiteStorage {
  constructor() {
    this.database = new DatabaseSync(":memory:");
  }

  exec(statement, ...bindings) {
    const normalizedBindings = bindings.map((value) => {
      if (value instanceof Uint8Array) {
        return Buffer.from(value);
      }
      return value;
    });
    const prepared = this.database.prepare(statement);
    if (/^\s*(SELECT|PRAGMA)\b/i.test(statement)) {
      return prepared.all(...normalizedBindings);
    }
    prepared.run(...normalizedBindings);
    return [];
  }

  transactionSync(operation) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }
}
