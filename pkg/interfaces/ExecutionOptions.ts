
export interface ExecuteOptions {
    streamResult?: boolean | undefined;
    fetchAsString?: Array<"String" | "Boolean" | "Number" | "Date" | "JSON" | "Buffer"> | undefined;
    parameters?: Record<string, unknown>;
}

/** A single bind variable, matching the set of values the driver accepts. */
export type Bind = string | number | boolean | null;

/** A single result row, keyed by column name. */
export type Row = Record<string, unknown>;

export type CacheStore = 'inmemory' | 'redis' | null;

export interface CacheStoreConfigs {
    /** Redis connection string. Ignored by the in-memory store. */
    connectionString?: string;
    /** Extra options forwarded to the underlying store constructor. */
    [key: string]: unknown;
}

/**
 * The subset of ioredis / node-cache that snowmise actually calls. Keeping this
 * narrow means neither package has to be imported for type-checking. ioredis
 * resolves asynchronously while node-cache returns directly, so the return
 * types cover both shapes.
 */
export interface CacheStoreConnection {
    get(key: string): Promise<string | null> | string | undefined | null;
    set(
        key: string,
        value: string,
        modeOrTtl?: string | number,
        expiry?: number
    ): Promise<unknown> | boolean;
    ping(): Promise<string>;
    disconnect(): void;
}
