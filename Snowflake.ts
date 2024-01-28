import * as SDK from 'snowflake-sdk';
import { SnowflakeError } from './interfaces/SnowflakeError';
import { ConnectionOptions } from './interfaces/Connection';
import { ConfigurationOptions } from './interfaces/Configurations';
import { Bind } from './interfaces/ExecutionOptions';

export class Snowflake {
    private readonly connection;
    private static executePromiseMap = {};
    private static statementIdMap = {};
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

    get conn(): string {
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
        if (!this.connection) { throw new SnowflakeError('Snowflake connection is not up - call connect() to establish a connection'); }
        return new Promise<boolean>((resolve, reject) => {
            const isUp = this.connection.isUp();
            if (isUp) {
                resolve(true);
            } else {
                reject(false);
            }
        });
    }

    public isValidConnection() {
        if (!this.connection) { throw new SnowflakeError('Snowflake connection is not up - call connect() to establish a connection'); }
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


    public execute(sqlText: string, binds?: Bind[] | Bind[][], destroyQueryCacheResponse: number = 120) {
        if (Snowflake.executePromiseMap[sqlText] && Snowflake.executePromiseMap[sqlText]['running']) {
            return this.returnExecutionPromise(sqlText);
        }

        if (destroyQueryCacheResponse && Snowflake.executePromiseMap[sqlText] && !Snowflake.executePromiseMap[sqlText]['running']
            && !Snowflake.executePromiseMap[sqlText]['error']) {
            const queryExecutedAt = Snowflake.executePromiseMap[sqlText]['queryExecutedAt'];
            const currentTime = new Date().getTime();
            if (destroyQueryCacheResponse < (currentTime - queryExecutedAt)) {
                return Snowflake.executePromiseMap[sqlText]['rows'];
            }
        }

        Snowflake.executePromiseMap[sqlText] = {
            'running': true,
            'executionPromise': new Promise((resolve, reject) => {
                const executionOptions = {
                    'sqlText': sqlText,
                    'complete': (err, _, rows) => {
                        Snowflake.executePromiseMap[sqlText]['running'] = false;
                        if (destroyQueryCacheResponse) {
                            Snowflake.executePromiseMap[sqlText]['rows'] = rows;
                            Snowflake.executePromiseMap[sqlText]['queryExecutedAt'] = new Date().getTime();
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
                const stmt = this.connection.execute(executionOptions);
            })
        }
        return this.returnExecutionPromise(sqlText);
    }


    public createStatement(sqlText: string, onComplete: (err, rows) => any, binds?: Bind[] | Bind[][], streamData?: boolean, getStream?: boolean, getStreamFn?: (stream) => any) {
        const executionOptions = {
            'sqlText': sqlText,
        };
        if (binds) {
            executionOptions['binds'] = binds;
        }
        if (streamData) {
            executionOptions['streamResult'] = streamData;
            executionOptions['complete'] = (err, stmt) => {
                const stream = stmt.streamRows();
                if (getStream && getStreamFn) {
                    getStreamFn(stream);
                } else {
                    const rows: any = [];
                    let error = null;
                    stream.on('readable', function (row) {
                        while ((row = this.read()) !== null) {
                            rows.push(row)
                        }
                    }).on('end', function () {
                        onComplete(err, rows);
                    }).on('error', function (err) {
                        error = err;
                        onComplete(err, null);
                    });
                }
            }
        } else {
            executionOptions['complete'] = (err, _, rows) => {
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
            Snowflake.statementIdMap[stmtId].cancel(err => {
                delete Snowflake.statementIdMap[stmtId];
                if (err) { reject(err); }
                else { resolve(); }
            })
        });
    }

    private returnExecutionPromise(sqlText: string) {
        return Snowflake.executePromiseMap[sqlText]['executionPromise'].then((rows) => {
            return rows
        }).catch((err) => {
            throw new SnowflakeError(err.message)
        });
    }

}