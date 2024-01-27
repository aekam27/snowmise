"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Snowflake = void 0;
var SDK = require("snowflake-sdk");
var SnowflakeError_1 = require("./interfaces/SnowflakeError");
var Snowflake = /** @class */ (function () {
    function Snowflake(connectionOptions, configurationOptions) {
        if (configurationOptions && typeof configurationOptions === 'object') {
            SDK.configure(configurationOptions);
        }
        this.connection = SDK.createConnection(connectionOptions);
    }
    Object.defineProperty(Snowflake.prototype, "id", {
        get: function () {
            return this.connection.getId();
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Snowflake.prototype, "conn", {
        get: function () {
            return this.connection;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Snowflake.prototype, "serviceName", {
        get: function () {
            return this.connection.getServiceName();
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Snowflake.prototype, "clientSessionKeepAlive", {
        get: function () {
            return this.connection.getClientSessionKeepAlive();
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(Snowflake.prototype, "clientSessionKeepAliveHeartbeatFrequency", {
        get: function () {
            return this.connection.getClientSessionKeepAliveHeartbeatFrequency();
        },
        enumerable: false,
        configurable: true
    });
    Snowflake.prototype.isConnectionUp = function () {
        var _this = this;
        if (!this.connection) {
            throw new SnowflakeError_1.SnowflakeError('Snowflake connection is not up - call connect() to establish a connection');
        }
        return new Promise(function (resolve, reject) {
            var isUp = _this.connection.isUp();
            if (isUp) {
                resolve(true);
            }
            else {
                reject(false);
            }
        });
    };
    Snowflake.prototype.isValidConnection = function () {
        if (!this.connection) {
            throw new SnowflakeError_1.SnowflakeError('Snowflake connection is not up - call connect() to establish a connection');
        }
        return this.connection.isValidAsync();
    };
    Snowflake.prototype.connectAsync = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.connection.connectAsync(function (err, _) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve();
                }
            });
        });
    };
    Snowflake.prototype.connect = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.connection.connect(function (err, _) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve();
                }
            });
        });
    };
    Snowflake.prototype.destroy = function () {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.connection.destroy(function (err) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve();
                }
            });
        });
    };
    Snowflake.prototype.execute = function (sqlText, binds, destroyQueryCacheResponse) {
        var _this = this;
        if (destroyQueryCacheResponse === void 0) { destroyQueryCacheResponse = 120; }
        if (Snowflake.executePromiseMap[sqlText] && Snowflake.executePromiseMap[sqlText]['running']) {
            return this.returnExecutionPromise(sqlText);
        }
        if (destroyQueryCacheResponse && Snowflake.executePromiseMap[sqlText] && !Snowflake.executePromiseMap[sqlText]['running']
            && !Snowflake.executePromiseMap[sqlText]['error']) {
            var queryExecutedAt = Snowflake.executePromiseMap[sqlText]['queryExecutedAt'];
            var currentTime = new Date().getTime();
            if (destroyQueryCacheResponse < (currentTime - queryExecutedAt)) {
                return Snowflake.executePromiseMap[sqlText]['rows'];
            }
        }
        Snowflake.executePromiseMap[sqlText] = {
            'running': true,
            'executionPromise': new Promise(function (resolve, reject) {
                var executionOptions = {
                    'sqlText': sqlText,
                    'complete': function (err, _, rows) {
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
                var stmt = _this.connection.execute(executionOptions);
                Snowflake.executePromiseMap[sqlText]['stmt'] = stmt;
            })
        };
        return this.returnExecutionPromise(sqlText);
    };
    Snowflake.prototype.createStatement = function (sqlText, onComplete, binds, streamData, getStream, getStreamFn) {
        var executionOptions = {
            'sqlText': sqlText,
        };
        if (binds) {
            executionOptions['binds'] = binds;
        }
        if (streamData) {
            executionOptions['streamResult'] = streamData;
            executionOptions['complete'] = function (err, stmt) {
                var stream = stmt.streamRows();
                if (getStream && getStreamFn) {
                    getStreamFn(stream);
                }
                else {
                    var rows_1 = [];
                    var error_1 = null;
                    stream.on('readable', function (row) {
                        while ((row = this.read()) !== null) {
                            rows_1.push(row);
                        }
                    }).on('end', function () {
                        onComplete(err, rows_1);
                    }).on('error', function (err) {
                        error_1 = err;
                        onComplete(err, null);
                    });
                }
            };
        }
        else {
            executionOptions['complete'] = function (err, _, rows) {
                onComplete(err, rows);
            };
        }
        var stmt = this.connection.execute(executionOptions);
        Snowflake.statementIdMap[stmt.getStatementId()] = stmt;
        return stmt.getStatementId();
    };
    Snowflake.prototype.getStatementSQLText = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getSqlText();
    };
    Snowflake.prototype.getStatementExecutionStatus = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getStatus();
    };
    Snowflake.prototype.getColumnsReturnedByStatement = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getColumns();
    };
    Snowflake.prototype.getColumnReturnedByStatement = function (stmtId, columnIdentifier) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getColumn(columnIdentifier);
    };
    Snowflake.prototype.getNumRows = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getNumRows();
    };
    Snowflake.prototype.getSessionState = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getSessionState();
    };
    Snowflake.prototype.getRequestId = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getRequestId()();
    };
    Snowflake.prototype.getNumUpdatedRows = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return Snowflake.statementIdMap[stmtId].getNumUpdatedRows();
    };
    Snowflake.prototype.cancel = function (stmtId) {
        if (!Snowflake.statementIdMap[stmtId]) {
            throw new SnowflakeError_1.SnowflakeError('Either the statement id is invalid or expired.');
        }
        return new Promise(function (resolve, reject) {
            Snowflake.statementIdMap[stmtId].cancel(function (err) {
                delete Snowflake.statementIdMap[stmtId];
                if (err) {
                    reject(err);
                }
                else {
                    resolve();
                }
            });
        });
    };
    Snowflake.prototype.returnExecutionPromise = function (sqlText) {
        return Snowflake.executePromiseMap[sqlText]['executionPromise'].then(function (rows) {
            return rows;
        }).catch(function (err) {
            throw new SnowflakeError_1.SnowflakeError(err.message);
        });
    };
    Snowflake.executePromiseMap = {};
    Snowflake.statementIdMap = {};
    return Snowflake;
}());
exports.Snowflake = Snowflake;
