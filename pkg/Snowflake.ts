import * as SDK from 'snowflake-sdk';
import { SnowflakeError } from './interfaces/SnowflakeError';
import { ConnectionOptions } from './interfaces/Connection';
import { ConfigurationOptions } from './interfaces/Configurations';
import { Bind } from './interfaces/ExecutionOptions';
import { Readable } from "stream";

export class Snowflake {
    private readonly connection: SDK.Connection;
    private static executePromiseMap: any = {};
    private static statementIdMap: any = {};
    constructor(
        connectionOptions: ConnectionOptions,
        configurationOptions?: ConfigurationOptions
    ) {
        if (configurationOptions && typeof configurationOptions === 'object') {
            SDK.configure(configurationOptions);
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

    get clientSessionKeepAlive(): boolean {
        return this.connection.getClientSessionKeepAlive();
    }

    get clientSessionKeepAliveHeartbeatFrequency(): number {
        return this.connection.getClientSessionKeepAliveHeartbeatFrequency();
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
            this.connection.connectAsync((err, _) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    public connect() {
        return new Promise<void>((resolve, reject) => {
            this.connection.connect((err, _) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    public destroy() {
        return new Promise<void>((resolve, reject) => {
            this.connection.destroy(err => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }


    public execute(sqlText: string, binds?: Bind[] | Bind[][], destroyQueryCacheResponse: number = 2000) {
        if (Snowflake.executePromiseMap[sqlText] && Snowflake.executePromiseMap[sqlText]['running']) {
            return this.returnExecutionPromise(sqlText);
        }

        if (Snowflake.executePromiseMap[sqlText] && !Snowflake.executePromiseMap[sqlText]['running']
            && !Snowflake.executePromiseMap[sqlText]['error'] && Snowflake.executePromiseMap[sqlText]['destroyQueryCacheResponse']) {
            const queryExecutedAt = Snowflake.executePromiseMap[sqlText]['queryExecutedAt'];
            const currentTime = new Date().getTime();
            const destroyQueryCacheResponse = Snowflake.executePromiseMap[sqlText]['destroyQueryCacheResponse'];
            if (destroyQueryCacheResponse > (currentTime - queryExecutedAt)) {
                return Snowflake.executePromiseMap[sqlText]['rows'];
            }
        }

        Snowflake.executePromiseMap[sqlText] = {
            'running': true,
            'executionPromise': new Promise((resolve, reject) => {
                const executionOptions: any = {
                    'sqlText': sqlText,
                    'complete': (err: any, _: any, rows: any) => {
                        Snowflake.executePromiseMap[sqlText]['running'] = false;
                        if (destroyQueryCacheResponse) {
                            Snowflake.executePromiseMap[sqlText]['rows'] = rows;
                            Snowflake.executePromiseMap[sqlText]['queryExecutedAt'] = new Date().getTime();
                            Snowflake.executePromiseMap[sqlText]['destroyQueryCacheResponse'] = destroyQueryCacheResponse;
                        }
                        if (err) {
                            Snowflake.executePromiseMap[sqlText]['error'] = true;
                            reject(err);
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
        return this.returnExecutionPromise(sqlText);
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
                const stream = stmt.streamRows();
                if (getStream && getStreamFn) {
                    getStreamFn(stream);
                } else {
                    const rows: any = [];
                    let error = null;
                    stream.on('readable', function (this: any,row: any) {
                        while ((row = this.read()) !== null) {
                            rows.push(row)
                        }
                    }).on('end', function () {
                        onComplete(err, rows);
                    }).on('error', function (err: any) {
                        error = err;
                        onComplete(err, null);
                    });
                }
            }
        } else {
            executionOptions['complete'] = (err: any, _: any, rows: any) => {
                onComplete(err, rows);
            }
        }
        const stmt = this.connection.execute(executionOptions);
        Snowflake.statementIdMap[stmt.getStatementId()] = stmt;
        return stmt.getStatementId();
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

    private returnExecutionPromise(sqlText: string) {
        return Snowflake.executePromiseMap[sqlText]['executionPromise'].then((rows: any) => {
            return rows
        }).catch((err: any) => {
            throw new SnowflakeError(err.message)
        });
    }

}