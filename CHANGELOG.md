# Changelog

## 0.1.0

### Breaking

- **Node.js 20 or newer is required.** `snowflake-sdk` 3.x and `ioredis` 5.11
  both dropped older runtimes.
- **Cache TTLs are now honoured correctly.** `destroyQueryCacheResponse` (now
  documented as `cacheTtlMs`) is milliseconds, but was being passed unconverted
  to Redis `EX` and to `node-cache`, both of which take **seconds**. A default
  60,000 ms TTL was therefore caching for 60,000 seconds — roughly 16.7 hours.
  Entries now expire when they were always meant to. If you were unknowingly
  relying on the long TTL, raise `cacheTtlMs` explicitly.
- **Caches and statement handles are per-instance.** They were `static`, so
  every `Snowflake` object in a process shared one query cache keyed only by SQL
  text. Two instances pointing at different accounts, databases or roles could
  serve each other's rows. Each instance now keeps its own state.
- `@types/snowflake-sdk` is no longer a dependency; the driver ships its own
  types. Remove it from your project if you installed it for snowmise.
- `Bind` widened to `string | number | boolean | null` to match the driver.
- `getColumnsReturnedByStatement()` returns `Column[] | undefined` and
  `getStatementExecutionStatus()` returns `StatementStatus`, matching the driver.

### Fixed

- `getRequestId()` invoked its own result (`getRequestId()()`) and threw
  `TypeError: ... is not a function` on every call. It now returns the id.
- `execute()` continued past `reject()` on a driver error, then wrote the failed
  result into the cache store. It now stops at the error.
- `connect()`, `connectAsync()` and `destroy()` had the same missing `return`
  after `reject()`.
- Cache-store failures during `connect()` no longer reject the connection; they
  warn, as originally intended.

### Changed

- Cache keys are hashed with SHA-256 instead of MD5.
- `ioredis` and `node-cache` are required lazily, so enabling neither cache store
  costs nothing at startup.
- Internal maps use `Map` rather than object literals, so a query containing
  `__proto__` can no longer collide with `Object.prototype`.

### Added

- Test suite covering de-duplication, cache expiry, TTL conversion, per-instance
  isolation, and error propagation. The previous test required live Snowflake
  credentials and could not run in CI.
- GitHub Actions CI across Node 20/22/24, with lint, build, test and a
  production dependency audit.
- Dependabot configuration for npm and GitHub Actions.
- ESLint flat config.

### Security

- `snowflake-sdk` 1.15 → 3.1, clearing the transitive advisories in `axios`,
  `form-data`, `fast-xml-parser`, `jws`, `brace-expansion`, `glob`, `minimatch`,
  `js-yaml`, `bn.js`, `picomatch`, `uuid`, `@smithy/config-resolver`,
  `@tootallnate/once` and `follow-redirects`.
- `jest` 29 → 30 and `typescript` 5.6 → 5.9, clearing the `@babel/core` advisory.
- `npm audit` reports no known vulnerabilities in the resulting tree.
