# Snowmise

[![CI](https://github.com/aekam27/snowmise/actions/workflows/ci.yml/badge.svg)](https://github.com/aekam27/snowmise/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/snowmise.svg)](https://www.npmjs.com/package/snowmise)

A promise-based wrapper around the Snowflake Node.js driver.

Snowmise wraps `snowflake-sdk` so queries return promises instead of taking
callbacks, and adds two things on top: identical queries issued at the same time
share a single round trip, and results can be cached for a configurable window
in memory or in Redis.

## Upgrading from 0.0.x

0.1.0 changes two caching behaviours. Neither raises an error, so both are worth
reading before you upgrade.

**Cached results now expire when you asked them to.** The TTL argument is
milliseconds, but it was being handed unconverted to Redis `EX` and to
`node-cache`, which both take seconds. The default 60,000 ms TTL was therefore
caching for 60,000 *seconds* — about 16.7 hours. If your workload was quietly
relying on that, pass a larger `cacheTtlMs` explicitly; otherwise expect more
queries to reach Snowflake than before.

**Caches are no longer shared between instances.** Query results and statement
handles used to be `static`, so every `Snowflake` object in a process shared one
cache keyed on SQL text alone — two instances pointing at different accounts,
databases or roles could serve each other's rows. Each instance now keeps its
own state.

Also: Node.js 20 or newer is required, and `@types/snowflake-sdk` should be
removed from your project — the driver ships its own types now.

The full list is in the
[changelog](https://github.com/aekam27/snowmise/blob/main/CHANGELOG.md).

## Requirements

- Node.js 20 or newer
- `snowflake-sdk` 3.x (installed as a dependency)

## Installation

```bash
npm install snowmise
```

```ts
import { Snowflake } from 'snowmise';
// or
const { Snowflake } = require('snowmise');
```

## Quick start

```ts
import { Snowflake } from 'snowmise';

const snowflake = new Snowflake({
    account: 'my-account',
    username: 'my-user',
    password: 'my-password',
    database: 'MY_DB',
    schema: 'PUBLIC',
    warehouse: 'COMPUTE_WH',
});

await snowflake.connect();

const rows = await snowflake.execute('select * from customers where region = ?', ['EMEA']);

await snowflake.destroy();
```

## Constructor

```ts
new Snowflake(connectionOptions, cacheStore?, configurationOptions?, cacheStoreConfigs?)
```

| Parameter | Required | Description |
| --- | --- | --- |
| `connectionOptions` | yes | Passed straight to `snowflake-sdk`'s `createConnection`. |
| `cacheStore` | no | `'inmemory'`, `'redis'`, or omitted for no external cache. |
| `configurationOptions` | no | Passed to `snowflake-sdk`'s `configure` (log level, OCSP behaviour). |
| `cacheStoreConfigs` | no | `{ connectionString }` for Redis; forwarded to `node-cache` otherwise. |

Cache backends are loaded lazily, so `ioredis` and `node-cache` are only
required when you actually enable a cache store.

## Caching

```ts
const snowflake = new Snowflake(connectionOptions, 'redis', undefined, {
    connectionString: 'redis://localhost:6379',
});

// Cache this result for 30 seconds.
const rows = await snowflake.execute('select count(*) from events', undefined, 30_000);

// Opt out of caching for a single call.
const live = await snowflake.execute('select current_timestamp()', undefined, 0);
```

TTLs are given in **milliseconds** and converted to seconds for the underlying
store. Each `Snowflake` instance keeps its own cache — two instances pointing at
different databases or roles never share rows.

## API reference

### Properties

| Property | Type | Description |
| --- | --- | --- |
| `id` | `string` | The underlying connection id. |
| `conn` | `SDK.Connection` | The raw driver connection, for anything snowmise does not wrap. |
| `serviceName` | `string` | The Snowflake service name. |

### Connection management

| Method | Returns | Description |
| --- | --- | --- |
| `connect()` | `Promise<void>` | Establishes the connection. |
| `connectAsync()` | `Promise<void>` | Establishes the connection using the driver's async flow (required for browser-based SSO). |
| `isConnectionUp()` | `Promise<boolean>` | Whether the connection is currently active. |
| `isValidConnection()` | `Promise<boolean>` | Whether the connection can accept a query. |
| `destroy()` | `Promise<void>` | Tears down the connection and disconnects the cache store. |

### Query execution

#### `execute(sqlText, binds?, cacheTtlMs?, useHash?)`

Runs a query and resolves its rows.

- `sqlText` — the statement to run.
- `binds` — optional bind variables.
- `cacheTtlMs` — how long the result stays servable from cache, in
  milliseconds. Defaults to `60000`. Pass `0` to disable caching for this call.
- `useHash` — hash the SQL text to build the cache key rather than using the raw
  statement. Defaults to `true`.

Concurrent identical queries are de-duplicated onto a single round trip.

#### `executeAsyncStream(sqlText, binds?)`

An async generator that yields rows one at a time, for result sets too large to
hold in memory.

```ts
for await (const row of snowflake.executeAsyncStream('select * from big_table')) {
    process(row);
}
```

#### `createStatement(sqlText, onComplete, binds?, streamData?, getStream?, getStreamFn?)`

Creates a statement and returns its query id. Use the id with the accessors
below.

### Statement accessors

Each takes the query id returned by `createStatement` and throws a
`SnowflakeError` if the id is unknown or expired.

| Method | Returns |
| --- | --- |
| `getStatementSQLText(stmtId)` | `string` |
| `getStatementExecutionStatus(stmtId)` | `StatementStatus` |
| `getColumnsReturnedByStatement(stmtId)` | `Column[] \| undefined` |
| `getColumnReturnedByStatement(stmtId, columnIdentifier)` | `Column` |
| `getNumRows(stmtId)` | `number` |
| `getNumUpdatedRows(stmtId)` | `number \| undefined` |
| `getSessionState(stmtId)` | `object \| undefined` |
| `getRequestId(stmtId)` | `string` |
| `cancel(stmtId)` | `Promise<void>` |

## Development

```bash
npm install
npm run lint
npm run build
npm test
```

## Changelog

See [CHANGELOG.md](https://github.com/aekam27/snowmise/blob/main/CHANGELOG.md).

## License

[ISC](https://github.com/aekam27/snowmise/blob/main/LICENSE)
