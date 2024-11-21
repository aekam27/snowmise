# Snowmise: A Promise based wrapper for Snowflake 

# Overview

The Snowmise Node.js package is a powerful tool for interacting with the Snowflake data warehouse using Node.js. This package provides a comprehensive set of features, including executing SQL queries, managing connections, caching query results, and handling Snowflake statements.

# Installation

To use the Snowmise Node.js package in your project, follow these installation steps:

1. Install the package using npm: <b>npm install snowmise</b>

2. Import the necessary modules in your Node.js application: <br/>
      <b>const SDK = require('snowmise');</b><br/>
      or<br/>
      <b>import {Snowflake} from 'snowmise';</b>

4. Create an instance of the Snowflake class to connect to your Snowflake account and begin executing queries.
      const snowflake = new Snowflake(connectionOptions, cacheStore, configurationOptions, cacheStoreConfigs);
      required: connectionOptions
      optional: cacheStore, configurationOptions, cacheStoreConfigs

# Usage
      
  Basic Usage
      
      Create a Snowflake Connection:
         const snowflake = new Snowflake({
                                              account: "",
                                              username: "",
                                              password: "",
                                              database: "",
                                              schema: "",
                                              warehouse: ""
                                        });
         await snowmise.connect()
         
      Execute a Query:
         const result = await snowflake.execute(sqlText);
      
      Retrieve Statement Information:
         const status = snowflake.getStatementExecutionStatus(stmtId);
         const numRows = snowflake.getNumRows(stmtId);
         const sessionState = snowflake.getSessionState(stmtId);
         // ... and more
         
      Caching Results:  
          const cacheStore = 'redis'; // or 'inmemory'
          const cacheStoreConfigs = { connectionString: 'your-redis-connection-string' };
          const snowflake = new Snowflake(connectionOptions, cacheStore, configurationOptions, cacheStoreConfigs);
          
      Destroy Connection:
           await snowflake.destroy();


# API Reference

<b>Properties</b>

      id: string 
      Returns the unique identifier for the Snowflake connection.

      conn: SDK.Connection 
      Returns the underlying Snowflake SDK Connection instance.

      serviceName: string 
      Returns the name of the Snowflake service.

<b>Connection Management</b>

      isConnectionUp(): Promise<boolean> 
      Checks if the Snowflake connection is established and returns a Promise that resolves to a boolean.
      
      isValidConnection(): Promise<boolean>
      Checks if the Snowflake connection is valid and returns a Promise that resolves to a boolean.
      
      connect(): Promise<void>
      Synchronously establishes a connection to the Snowflake account. Returns a Promise that resolves when the connection is successful.
      
      destroy(): Promise<void>
      Destroys the Snowflake connection and performs cleanup. Returns a Promise that resolves when the destruction is complete.
      
<b>Query Execution</b>

      1. execute(sqlText: string, binds?: Bind[] | Bind[][], destroyQueryCacheResponse: number = 60000, useHash: boolean = true): Promise<any>
         
         Executes a SQL query asynchronously, handling caching and result retrieval.
         
         Parameters:
           sqlText: The SQL query to execute.
           binds: Optional array of bind variables.
           destroyQueryCacheResponse: Time (in milliseconds) to cache query results.
           useHash: If true, uses a hash of the SQL text as a unique key for caching.

      2. public async *executeAsyncStream(sqlText: string, binds?: Bind[] | Bind[][]): AsyncGenerator<any, void, unknown>
         The executeAsyncStream function is an asynchronous generator designed to execute a SQL query and stream the results row-by-row. It enables efficient handling of large result sets by processing rows incrementally rather than loading the entire dataset into memory at once.
         Parameters:
           sqlText: The SQL query to execute.
           binds: Optional array of bind variables.
            
      3. createStatement(sqlText: string, onComplete: (err: any, rows: any) => any, binds?: Bind[] | Bind[][], streamData?: boolean, getStream?: boolean, getStreamFn?: (stream: Readable) => any): string
         
         Creates a Snowflake statement for executing SQL queries.
         
         Parameters:
           sqlText: The SQL query to execute.
           onComplete: Callback function to handle query results.
           Additional parameters for handling streaming and advanced options.
        
      4. getStatementSQLText(stmtId: string): string
         Retrieves the SQL text of a Snowflake statement using its identifier.
                  
      5. getStatementExecutionStatus(stmtId: string): string
         Retrieves the execution status of a Snowflake statement using its identifier.
         
      6. getColumnsReturnedByStatement(stmtId: string): any[]
         Retrieves an array of column objects returned by a Snowflake statement using its identifier.
         
      7. getColumnReturnedByStatement(stmtId: string, columnIdentifier: string | number): any
         Retrieves a specific column object returned by a Snowflake statement using its identifier and column identifier.
         
      8. getNumRows(stmtId: string): number
         Retrieves the number of rows returned by a Snowflake statement using its identifier.
         
      9. getSessionState(stmtId: string): string
         Retrieves the session state of a Snowflake statement using its identifier.
         
      10. getRequestId(stmtId: string): string
         Retrieves the request identifier associated with a Snowflake statement using its identifier.
         
      11.getNumUpdatedRows(stmtId: string): number
         Retrieves the number of updated rows by a Snowflake statement using its identifier.
         
      12.cancel(stmtId: string): Promise<void>
         Cancels the execution of a Snowflake statement using its identifier. Returns a Promise that resolves when the cancellation is complete.


# Dependencies
      snowflake-sdk: (Version: ^1.9.3)
      ioredis: (Version: ^5.3.2) (Required for Redis cache store)
      node-cache: (Version: ^5.1.2) (Required for in-memory cache store)
