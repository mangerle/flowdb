export declare class Transaction {
  put(store: string, value: unknown): void
  delete(store: string, key: unknown): void
  commit(): Promise<void>
  abort(): void
}
