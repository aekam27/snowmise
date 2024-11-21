import * as SDK from 'snowflake-sdk';
import { SnowflakeError } from './interfaces/SnowflakeError';
import { ConnectionOptions } from './interfaces/Connection';
import { ConfigurationOptions } from './interfaces/Configurations';
import { Bind, CacheStore } from './interfaces/ExecutionOptions';
import { Readable } from "stream";
const Redis = require("ioredis");
const NodeCache = require( "node-cache" );
const crypto = require('crypto');

export class Snowflake {
    private readonly connection: SDK.Connection;
    private readonly cacheStoreConnection: any;
    private static executePromiseMap: any = {};
    private static statementIdMap: any = {};
    private static cacheStore: CacheStore = null;
    constructor(
        connectionOptions: ConnectionOptions,
        cacheStore?: CacheStore,
        configurationOptions?: ConfigurationOptions,
        cacheStoreConfigs?: any
    ) {
        if ( configurationOptions && Object.prototype.toString.call(configurationOptions) === '[object Object]' ) {
            SDK.configure(configurationOptions);
        }
        if ( cacheStore ) {
            Snowflake.cacheStore = cacheStore;
            if ( cacheStore === 'redis' ) {
                this.cacheStoreConnection = cacheStoreConfigs?.connectionString ? new Redis(cacheStoreConfigs?.connectionString): new Redis()
            } else if ( cacheStore === 'inmemory' ) {
                this.cacheStoreConnection = cacheStoreConfigs?.connectionString ? new NodeCache(cacheStoreConfigs): new NodeCache( { checkperiod: 120 } );
            }
        }
        this.connection = SDK.createConnection(connectionOptions);
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

    public isConnectionUp() {
        if (!this.connection) { throw new SnowflakeError('Snowflake is not initialized - Initialize Snowflake and call connect() to establish a connection'); }
        return new Promise<boolean>((resolve, _) => {
            const isUp = this.connection.isUp();
            if (isUp) {
                resolve(true);
            } else {
                resolve(false);
            }
        });
    }

    public isValidConnection() {
        if (!this.connection) { throw new SnowflakeError('Snowflake is not initialized - Initialize Snowflake and call connect() to establish a connection'); }
        return this.connection.isValidAsync();
    }

    public connectAsync() {
        return new Promise<void>((resolve, reject) => {
            this.connection.connectAsync(async (err, _) => {
                if (err) {
                    reject(err);
                } else {
                    const isPong = await this.pingStoreConn();
                    if (isPong !== 'PONG') {
                        console.log('Cache store connection failed.')
                    }
                    resolve();
                }
            });
        });
    }

    public async connect() {
        return new Promise<void>((resolve, reject) => {
            this.connection.connect(async (err, _) => {
                if (err) {
                    reject(err);
                } else {
                    const isPong = await this.pingStoreConn();
                    if (isPong !== 'PONG') {
                        console.log('Cache store connection failed.')
                    }
                    resolve();
                }
            });
        });
    }

    public async destroy() {
        return new Promise<void>((resolve, reject) => {
            this.connection.destroy(async (err) => {
                if (err) {
                    reject(err);
                } else {
                    await this.disconnectConn()
                    resolve();
                }
            });
        });
    }


    public async execute(sqlText: string, binds?: Bind[] | Bind[][], destroyQueryCacheResponse: number = 60000, useHash: boolean = true) {
        let uniqKey = sqlText;
        if (useHash) {
            uniqKey = crypto.createHash('md5').update(sqlText).digest("hex") 
        }
        if (Snowflake.executePromiseMap[uniqKey] && Snowflake.executePromiseMap[uniqKey]['running']) {
            return this.returnExecutionPromise(uniqKey);
        }

        if (Snowflake.executePromiseMap[uniqKey] && !Snowflake.executePromiseMap[uniqKey]['running']
            && !Snowflake.executePromiseMap[uniqKey]['error'] && Snowflake.executePromiseMap[uniqKey]['destroyQueryCacheResponse']) {
            const queryExecutedAt = Snowflake.executePromiseMap[uniqKey]['queryExecutedAt'];
            const currentTime = new Date().getTime();
            const destroyQueryCacheResponse = Snowflake.executePromiseMap[uniqKey]['destroyQueryCacheResponse'];
            if (destroyQueryCacheResponse > (currentTime - queryExecutedAt)) {
                if ( Snowflake.cacheStore ) {
                    let record;
                    try {
                        record =  await this.getRecords(Snowflake.cacheStore, uniqKey);
                        return JSON.parse(record)?.data;
                    } catch (err) {
                        console.log(`The key does not exist in ${Snowflake.cacheStore}. Retrying with in-memory map records.`);
                    } 
                }
                return Snowflake.executePromiseMap[uniqKey]['rows'];
            }
        }

        Snowflake.executePromiseMap[uniqKey] = {
            'running': true,
            'executionPromise': new Promise((resolve, reject) => {
                const executionOptions: any = {
                    'sqlText': sqlText,
                    'complete': async (err: any, _: any, rows: any) => {
                        Snowflake.executePromiseMap[uniqKey]['running'] = false;
                        if (err) {
                            Snowflake.executePromiseMap[uniqKey]['error'] = true;
                            reject(err);
                        }
                        if (destroyQueryCacheResponse) {
                            Snowflake.executePromiseMap[uniqKey]['rows'] = rows;
                            Snowflake.executePromiseMap[uniqKey]['queryExecutedAt'] = new Date().getTime();
                            Snowflake.executePromiseMap[uniqKey]['destroyQueryCacheResponse'] = destroyQueryCacheResponse;
                            if ( Snowflake.cacheStore ) {
                                try {
                                    await this.setRecords(Snowflake.cacheStore, uniqKey, JSON.stringify({data:rows}),destroyQueryCacheResponse);
                                } catch (err) {
                                    console.log(`Unable to update the key value in ${Snowflake.cacheStore}. Error trace: '${err}'`);
                                } 
                            }                
                        }
                        resolve(rows);
                    }
                };
                if (binds) {
                    executionOptions['binds'] = binds;
                }
                this.connection.execute(executionOptions);
            })
        }
        return this.returnExecutionPromise(uniqKey);
    }

    public async *executeAsyncStream(sqlText: string, binds?: Bind[] | Bind[][]) {
        const executionOptions:any = {
            'sqlText': sqlText,
            streamResult: true,
        };
        if (binds) {
            executionOptions['binds'] = binds;
        }
        const stmt = this.connection.execute(executionOptions);
        const stream = stmt.streamRows();
        for await (const row of stream) {
            yield row;
        }
    }

    public createStatement(sqlText: string, onComplete: (err: any, rows: any) => any, binds?: Bind[] | Bind[][], streamData?: boolean, getStream?: boolean, getStreamFn?: (stream: Readable) => any) {
        const executionOptions:any = {
            'sqlText': sqlText,
        };
        if (binds) {
            executionOptions['binds'] = binds;
        }
        if (streamData) {
            executionOptions['streamResult'] = streamData;
            executionOptions['complete'] = (err: any, stmt: { streamRows: () => Readable; }) => {
                if (err) {
                    onComplete(err, null);
                    return;
                }
                const stream = stmt.streamRows();
                if (getStream && getStreamFn) {
                    getStreamFn(stream);
                } else {
                    const rows: any = [];
                    let error:any = null;
                    stream.on('readable', function (this: any,row: any) {
                        while ((row = this.read()) !== null) {
                            rows.push(row)
                        }
                    }).on('end', function () {
                        onComplete(error, rows);
                    }).on('error', function (err: any) {
                        error = err;
                        onComplete(error, null);
                    });
                }
            }
        } else {
            executionOptions['complete'] = (err: any, _: any, rows: any) => {
                onComplete(err, rows);
            }
        }
        const stmt = this.connection.execute(executionOptions);
        Snowflake.statementIdMap[stmt.getQueryId()] = stmt;
        return stmt.getQueryId();
    }

    public getStatementSQLText(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getSqlText();
    }

    public getStatementExecutionStatus(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getStatus();
    }

    public getColumnsReturnedByStatement(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getColumns();
    }

    public getColumnReturnedByStatement(stmtId: string, columnIdentifier: string | number) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getColumn(columnIdentifier);
    }

    public getNumRows(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getNumRows();
    }

    public getSessionState(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getSessionState();
    }

    public getRequestId(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getRequestId()();
    }

    public getNumUpdatedRows(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return Snowflake.statementIdMap[stmtId].getNumUpdatedRows();
    }

    public cancel(stmtId: string) {
        if (!Snowflake.statementIdMap[stmtId]) { throw new SnowflakeError('Either the statement id is invalid or expired.') }
        return new Promise<void>((resolve, reject) => {
            Snowflake.statementIdMap[stmtId].cancel((err: any) => {
                delete Snowflake.statementIdMap[stmtId];
                if (err) { reject(err); }
                else { resolve(); }
            })
        });
    }

    private returnExecutionPromise(uniqKey: string) {
        return Snowflake.executePromiseMap[uniqKey]['executionPromise'].then((rows: any) => {
            return rows
        }).catch((err: any) => {
            throw new SnowflakeError(err.message)
        });
    }

    private getRecords(cacheStore: string, key: string) {
        switch(cacheStore){
            case 'redis':
                return this.cacheStoreConnection.get(key);
            case 'inmemory':
                return this.cacheStoreConnection.get(key);
            default:
                Snowflake.cacheStore = null;
                throw new SnowflakeError('Invalid store type')
        }
    }

    private setRecords(cacheStore: string, key: string, value: string, expiryTime: number) {
        switch(cacheStore){
            case 'redis':
                return this.cacheStoreConnection.set(key, value,"EX", expiryTime);
            case 'inmemory':
                return this.cacheStoreConnection.set(key, value, expiryTime);
            default:
                Snowflake.cacheStore = null;
                throw new SnowflakeError('Invalid store type')
        }
    }

    private async pingStoreConn(){
        switch (Snowflake.cacheStore) {
            case 'redis':
                return this.cacheStoreConnection.ping();
            case 'inmemory':
                return 'PONG';
            default:
                return 'DING';
        }
    }

    private disconnectConn(){
        switch (Snowflake.cacheStore) {
            case 'redis':
                return this.cacheStoreConnection.disconnect();
            default:
                return;
        } 
    }

}