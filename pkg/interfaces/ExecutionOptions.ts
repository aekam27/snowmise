
export interface ExecuteOptions {
    streamResult?: boolean | undefined;
    fetchAsString?: Array<"String" | "Boolean" | "Number" | "Date" | "JSON" | "Buffer"> | undefined;
    parameters?: Record<string, unknown>;
}
export type Bind = string | number

export type CacheStore = 'inmemory' | 'redis' | null;