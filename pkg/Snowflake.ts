import * as SDK from 'snowflake-sdk';
import { createHash } from 'crypto';
import { Readable } from 'stream';
import { SnowflakeError } from './interfaces/SnowflakeError';
import { ConnectionOptions } from './interfaces/Connection';
import { ConfigurationOptions } from './interfaces/Configurations';
import {
    Bind,
    CacheStore,
    CacheStoreConfigs,
    CacheStoreConnection,
    Row,
} from './interfaces/ExecutionOptions';

/**
 * Bookkeeping for a single in-flight or recently completed `execute()` call.
 */
interface ExecutionRecord {
    running: boolean;
    error: boolean;
    executionPromise: Promise<Row[]>;
    rows?: Row[];
    queryExecutedAt?: number;
    ttlMs?: number;
}

const MS_PER_SECOND = 1000;

export class Snowflake {
    private readonly connection: SDK.Connection;
    private readonly cacheStoreConnection: CacheStoreConnection | null = null;
    private readonly cacheStore: CacheStore = null;

    // Per-instance, not static: two Snowflake instances may point at different
    // accounts, databases or roles, so they must never share cached rows or
    // statement handles.
    private readonly executePromiseMap = new Map<string, ExecutionRecord>();
    private readonly statementIdMap = new Map<string, SDK.RowStatement>();

    constructor(
        connectionOptions: ConnectionOptions,
        cacheStore?: CacheStore,
        configurationOptions?: ConfigurationOptions,
        cacheStoreConfigs?: CacheStoreConfigs
    ) {
        if (configurationOptions && Object.prototype.toString.call(configurationOptions) === '[object Object]') {
            SDK.configure(configurationOptions);
        }
        if (cacheStore) {
            this.cacheStore = cacheStore;
            this.cacheStoreConnection = Snowflake.createCacheStore(cacheStore, cacheStoreConfigs);
        }
        this.connection = SDK.createConnection(connectionOptions);
    }

    /**
     * Cache backends are loaded lazily so that installing snowmise without ever
     * enabling a cache does not pay the cost of requiring ioredis/node-cache.
     */
    private static createCacheStore(
        cacheStore: CacheStore,
        cacheStoreConfigs?: CacheStoreConfigs
    ): CacheStoreConnection {
        switch (cacheStore) {
            case 'redis': {
                // eslint-disable-next-line @typescript-eslint/no-require-imports
                const Redis = require('ioredis');
                return cacheStoreConfigs?.connectionString
                    ? new Redis(cacheStoreConfigs.connectionString)
                    : new Redis();
            }
            case 'inmemory': {
                // eslint-disable-next-line @typescript-eslint/no-require-imports
                const NodeCache = require('node-cache');
                return new NodeCache({ checkperiod: 120, ...cacheStoreConfigs });
            }
            default:
                throw new SnowflakeError(`Invalid cache store type '${cacheStore}'.`);
        }
    }

    get id(): string {
        return this.connection.getId();
    }

    get conn(): SDK.Connection {
        return this.connection;
    }

    get serviceName(): string {
        return this.connection.getServiceName();
    }

    public async isConnectionUp(): Promise<boolean> {
        if (!this.connection) {
            throw new SnowflakeError(
                'Snowflake is not initialized - Initialize Snowflake and call connect() to establish a connection'
            );
        }
        return this.connection.isUp();
    }

    public isValidConnection(): Promise<boolean> {
        if (!this.connection) {
            throw new SnowflakeError(
                'Snowflake is not initialized - Initialize Snowflake and call connect() to establish a connection'
            );
        }
        return this.connection.isValidAsync();
    }

    public connectAsync(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.connection.connectAsync(async (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                await this.warnIfCacheStoreUnreachable();
                resolve();
            });
        });
    }

    public connect(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.connection.connect(async (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                await this.warnIfCacheStoreUnreachable();
                resolve();
            });
        });
    }

    public destroy(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.connection.destroy(async (err) => {
                if (err) {
                    reject(err);
                    return;
                }
                await this.disconnectConn();
                resolve();
            });
        });
    }

    /**
     * Executes a query, de-duplicating concurrent identical queries and
     * optionally serving repeats from cache.
     *
     * @param sqlText the statement to run
     * @param binds optional bind variables
     * @param cacheTtlMs how long a result stays servable from cache, in
     *        milliseconds. Pass 0 to disable caching for this call.
     * @param useHash hash the SQL text to build the cache key instead of using
     *        the raw statement
     */
    public execute(
        sqlText: string,
        binds?: Bind[] | Bind[][],
        cacheTtlMs: number = 60000,
        useHash: boolean = true
    ): Promise<Row[]> {
        const uniqKey = useHash ? createHash('sha256').update(sqlText).digest('hex') : sqlText;

        const previous = this.executePromiseMap.get(uniqKey);
        if (previous?.running) {
            return this.returnExecutionPromise(uniqKey);
        }

        // The record has to be registered synchronously. If any await ran first,
        // a second caller arriving in the same tick would miss the `running`
        // check above and issue a duplicate query.
        const record: ExecutionRecord = {
            running: true,
            error: false,
            executionPromise: undefined as unknown as Promise<Row[]>,
        };
        record.executionPromise = this.resolveRows(record, previous, uniqKey, sqlText, binds, cacheTtlMs);
        this.executePromiseMap.set(uniqKey, record);
        return this.returnExecutionPromise(uniqKey);
    }

    /** Serves `uniqKey` from cache when it is still live, otherwise runs the query. */
    private async resolveRows(
        record: ExecutionRecord,
        previous: ExecutionRecord | undefined,
        uniqKey: string,
        sqlText: string,
        binds: Bind[] | Bind[][] | undefined,
        cacheTtlMs: number
    ): Promise<Row[]> {
        const cached = await this.readFromCache(previous, uniqKey);
        if (cached) {
            record.running = false;
            record.rows = cached.rows;
            record.queryExecutedAt = previous?.queryExecutedAt;
            record.ttlMs = previous?.ttlMs;
            return cached.rows;
        }

        return new Promise<Row[]>((resolve, reject) => {
            const executionOptions: SDK.StatementOption = {
                sqlText,
                complete: async (err, _stmt, rows) => {
                    record.running = false;
                    if (err) {
                        record.error = true;
                        reject(err);
                        return;
                    }
                    const resultRows = (rows ?? []) as Row[];
                    if (cacheTtlMs) {
                        record.rows = resultRows;
                        record.queryExecutedAt = Date.now();
                        record.ttlMs = cacheTtlMs;
                        await this.writeToCache(uniqKey, resultRows, cacheTtlMs);
                    }
                    resolve(resultRows);
                },
            };
            if (binds) {
                executionOptions.binds = binds as SDK.Binds;
            }
            this.connection.execute(executionOptions);
        });
    }

    public async *executeAsyncStream(sqlText: string, binds?: Bind[] | Bind[][]): AsyncGenerator<Row> {
        const executionOptions: SDK.StatementOption = {
            sqlText,
            streamResult: true,
        };
        if (binds) {
            executionOptions.binds = binds as SDK.Binds;
        }
        const stmt = this.connection.execute(executionOptions);
        const stream = stmt.streamRows();
        for await (const row of stream) {
            yield row as Row;
        }
    }

    public createStatement(
        sqlText: string,
        onComplete: (err: SDK.SnowflakeError | undefined, rows: Row[] | null) => void,
        binds?: Bind[] | Bind[][],
        streamData?: boolean,
        getStream?: boolean,
        getStreamFn?: (stream: Readable) => void
    ): string {
        const executionOptions: SDK.StatementOption = { sqlText };
        if (binds) {
            executionOptions.binds = binds as SDK.Binds;
        }
        if (streamData) {
            executionOptions.streamResult = true;
            executionOptions.complete = (err, stmt) => {
                if (err) {
                    onComplete(err, null);
                    return;
                }
                const stream = stmt.streamRows();
                if (getStream && getStreamFn) {
                    getStreamFn(stream);
                    return;
                }
                const rows: Row[] = [];
                stream
                    .on('readable', function (this: Readable) {
                        let row;
                        while ((row = this.read()) !== null) {
                            rows.push(row as Row);
                        }
                    })
                    .on('end', () => onComplete(undefined, rows))
                    .on('error', (streamErr: SDK.SnowflakeError) => onComplete(streamErr, null));
            };
        } else {
            executionOptions.complete = (err, _stmt, rows) => {
                onComplete(err, (rows ?? null) as Row[] | null);
            };
        }
        const stmt = this.connection.execute(executionOptions);
        this.statementIdMap.set(stmt.getQueryId(), stmt);
        return stmt.getQueryId();
    }

    public getStatementSQLText(stmtId: string): string {
        return this.requireStatement(stmtId).getSqlText();
    }

    public getStatementExecutionStatus(stmtId: string): SDK.StatementStatus {
        return this.requireStatement(stmtId).getStatus();
    }

    public getColumnsReturnedByStatement(stmtId: string): SDK.Column[] | undefined {
        return this.requireStatement(stmtId).getColumns();
    }

    public getColumnReturnedByStatement(stmtId: string, columnIdentifier: string | number): SDK.Column {
        return this.requireStatement(stmtId).getColumn(columnIdentifier);
    }

    public getNumRows(stmtId: string): number {
        return this.requireStatement(stmtId).getNumRows();
    }

    public getSessionState(stmtId: string): object | undefined {
        return this.requireStatement(stmtId).getSessionState();
    }

    public getRequestId(stmtId: string): string {
        return this.requireStatement(stmtId).getRequestId();
    }

    public getNumUpdatedRows(stmtId: string): number | undefined {
        return this.requireStatement(stmtId).getNumUpdatedRows();
    }

    public cancel(stmtId: string): Promise<void> {
        const stmt = this.requireStatement(stmtId);
        return new Promise<void>((resolve, reject) => {
            stmt.cancel((err) => {
                this.statementIdMap.delete(stmtId);
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    private requireStatement(stmtId: string): SDK.RowStatement {
        const stmt = this.statementIdMap.get(stmtId);
        if (!stmt) {
            throw new SnowflakeError('Either the statement id is invalid or expired.');
        }
        return stmt;
    }

    /**
     * Returns the cached rows for `uniqKey` when the previous result is still
     * within its TTL, or null when the query must be re-run.
     */
    private async readFromCache(
        record: ExecutionRecord | undefined,
        uniqKey: string
    ): Promise<{ rows: Row[] } | null> {
        if (!record || record.running || record.error || !record.ttlMs || !record.queryExecutedAt) {
            return null;
        }
        if (record.ttlMs <= Date.now() - record.queryExecutedAt) {
            return null;
        }
        if (this.cacheStore) {
            try {
                const raw = await this.getRecords(uniqKey);
                if (raw) {
                    return { rows: JSON.parse(raw).data as Row[] };
                }
            } catch {
                console.warn(
                    `snowmise: key missing from ${this.cacheStore} cache, falling back to the in-process result.`
                );
            }
        }
        return record.rows ? { rows: record.rows } : null;
    }

    private async writeToCache(uniqKey: string, rows: Row[], ttlMs: number): Promise<void> {
        if (!this.cacheStore) {
            return;
        }
        try {
            await this.setRecords(uniqKey, JSON.stringify({ data: rows }), ttlMs);
        } catch (err) {
            console.warn(`snowmise: unable to write to ${this.cacheStore} cache. Error trace: '${err}'`);
        }
    }

    private returnExecutionPromise(uniqKey: string): Promise<Row[]> {
        const record = this.executePromiseMap.get(uniqKey);
        if (!record) {
            throw new SnowflakeError('No execution is in flight for this query.');
        }
        return record.executionPromise.catch((err: Error) => {
            throw new SnowflakeError(err.message);
        });
    }

    private async getRecords(key: string): Promise<string | null> {
        const conn = this.requireCacheStoreConnection();
        switch (this.cacheStore) {
            case 'redis':
                return (await conn.get(key)) ?? null;
            case 'inmemory':
                return (conn.get(key) as string | undefined) ?? null;
            default:
                throw new SnowflakeError('Invalid store type');
        }
    }

    /**
     * Both backends take a TTL in seconds, while the public API is expressed in
     * milliseconds, so convert rather than passing the raw value through.
     */
    private async setRecords(key: string, value: string, expiryMs: number): Promise<unknown> {
        const conn = this.requireCacheStoreConnection();
        const expirySeconds = Math.max(1, Math.round(expiryMs / MS_PER_SECOND));
        switch (this.cacheStore) {
            case 'redis':
                return conn.set(key, value, 'EX', expirySeconds);
            case 'inmemory':
                return conn.set(key, value, expirySeconds);
            default:
                throw new SnowflakeError('Invalid store type');
        }
    }

    private requireCacheStoreConnection(): CacheStoreConnection {
        if (!this.cacheStoreConnection) {
            throw new SnowflakeError('Cache store is not configured.');
        }
        return this.cacheStoreConnection;
    }

    private async warnIfCacheStoreUnreachable(): Promise<void> {
        if (!this.cacheStore) {
            return;
        }
        try {
            if ((await this.pingStoreConn()) !== 'PONG') {
                console.warn('snowmise: cache store connection failed.');
            }
        } catch (err) {
            console.warn(`snowmise: cache store connection failed. Error trace: '${err}'`);
        }
    }

    private async pingStoreConn(): Promise<string> {
        switch (this.cacheStore) {
            case 'redis':
                return this.requireCacheStoreConnection().ping();
            case 'inmemory':
                return 'PONG';
            default:
                return 'DING';
        }
    }

    private async disconnectConn(): Promise<void> {
        if (this.cacheStore === 'redis') {
            this.requireCacheStoreConnection().disconnect();
        }
    }
}
