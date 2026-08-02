import * as SDK from 'snowflake-sdk';
import { Snowflake } from '../pkg/Snowflake';
import { SnowflakeError } from '../pkg/interfaces/SnowflakeError';

// An explicit factory rather than an automock: loading the real driver kicks
// off its telemetry platform detection, which outlives the test environment.
jest.mock('snowflake-sdk', () => ({
    createConnection: jest.fn(),
    configure: jest.fn(),
}));
jest.mock('ioredis');

const mockedSDK = SDK as jest.Mocked<typeof SDK>;

type CompleteFn = NonNullable<SDK.StatementOption['complete']>;

/** Rows the fake driver hands back, and how it should behave. */
interface FakeDriverBehaviour {
    rows?: Array<Record<string, unknown>>;
    err?: SDK.SnowflakeError;
    /** Delay completion so concurrent calls overlap. */
    deferred?: boolean;
}

/**
 * Builds a fake SDK.Connection. `execute` records every call and completes
 * either immediately or when `flush()` is invoked.
 */
function fakeConnection(behaviour: FakeDriverBehaviour = {}) {
    const pending: Array<() => void> = [];
    const executeCalls: SDK.StatementOption[] = [];
    // Built in two steps: the mock implementations close over `statement` and
    // `connection`, so they cannot be part of the initialising literal.
    const statement = {
        getSqlText: jest.fn(() => 'select 1'),
        getStatus: jest.fn(() => 'complete' as unknown as SDK.StatementStatus),
        getColumns: jest.fn(() => undefined),
        getColumn: jest.fn(),
        getNumRows: jest.fn(() => 1),
        getNumUpdatedRows: jest.fn(() => 0),
        getSessionState: jest.fn(() => ({})),
        getRequestId: jest.fn(() => 'request-id-123'),
        getStatementId: jest.fn(() => 'query-id-abc'),
        getQueryId: jest.fn(() => 'query-id-abc'),
        cancel: jest.fn<void, [SDK.StatementCallback?]>(),
        streamRows: jest.fn(),
        fetchRows: jest.fn(),
    };

    const connection = {
        isUp: jest.fn(() => true),
        getId: jest.fn(() => 'conn-1'),
        getServiceName: jest.fn(() => 'svc'),
        isValidAsync: jest.fn(async () => true),
        connect: jest.fn<unknown, [SDK.ConnectionCallback?]>(),
        connectAsync: jest.fn<unknown, [SDK.ConnectionCallback?]>(),
        destroy: jest.fn<void, [SDK.ConnectionCallback?]>(),
        execute: jest.fn<unknown, [SDK.StatementOption]>(),
    };

    statement.cancel.mockImplementation((cb) => cb?.(undefined, statement as never, []));

    connection.connect.mockImplementation((cb) => {
        cb?.(undefined, connection as never);
        return connection;
    });
    connection.connectAsync.mockImplementation((cb) => {
        cb?.(undefined, connection as never);
        return connection;
    });
    connection.destroy.mockImplementation((cb) => {
        cb?.(undefined, connection as never);
    });
    connection.execute.mockImplementation((opts) => {
        executeCalls.push(opts);
        const complete = () =>
            (opts.complete as CompleteFn)?.(
                behaviour.err,
                statement as never,
                behaviour.rows ?? [{ N: 1 }]
            );
        if (behaviour.deferred) {
            pending.push(complete);
        } else {
            complete();
        }
        return statement;
    });

    return {
        connection,
        statement,
        executeCalls,
        flush: () => {
            pending.splice(0).forEach((fn) => fn());
        },
    };
}

const CONNECTION_OPTIONS = { account: 'acct', username: 'u', password: 'p' };

describe('Snowflake', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    describe('construction', () => {
        it('creates a driver connection and exposes its identifiers', () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);

            expect(mockedSDK.createConnection).toHaveBeenCalledWith(CONNECTION_OPTIONS);
            expect(snowflake.id).toBe('conn-1');
            expect(snowflake.serviceName).toBe('svc');
            expect(snowflake.conn).toBe(fake.connection);
        });

        it('applies configuration options only when they are an object', () => {
            mockedSDK.createConnection.mockReturnValue(fakeConnection().connection as never);

            new Snowflake(CONNECTION_OPTIONS, undefined, { logLevel: 'ERROR' });
            expect(mockedSDK.configure).toHaveBeenCalledWith({ logLevel: 'ERROR' });

            mockedSDK.configure.mockClear();
            new Snowflake(CONNECTION_OPTIONS);
            expect(mockedSDK.configure).not.toHaveBeenCalled();
        });
    });

    describe('execute', () => {
        it('resolves the rows returned by the driver', async () => {
            const fake = fakeConnection({ rows: [{ ID: 7 }] });
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await expect(snowflake.execute('select 7')).resolves.toEqual([{ ID: 7 }]);
        });

        it('passes bind variables through to the driver', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await snowflake.execute('select ?', [42]);

            expect(fake.executeCalls[0]?.binds).toEqual([42]);
        });

        it('de-duplicates identical queries that are already in flight', async () => {
            const fake = fakeConnection({ deferred: true });
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            const first = snowflake.execute('select 1');
            const second = snowflake.execute('select 1');
            // Let the cache lookup settle so the driver call is actually queued.
            await new Promise((resolve) => setImmediate(resolve));
            fake.flush();

            await expect(Promise.all([first, second])).resolves.toEqual([[{ N: 1 }], [{ N: 1 }]]);
            expect(fake.connection.execute).toHaveBeenCalledTimes(1);
        });

        it('serves a repeat query from cache while the TTL is live', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await snowflake.execute('select 1', undefined, 60000);
            await snowflake.execute('select 1', undefined, 60000);

            expect(fake.connection.execute).toHaveBeenCalledTimes(1);
        });

        it('re-runs the query once the TTL has elapsed', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);
            const now = jest.spyOn(Date, 'now');

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            now.mockReturnValue(1_000);
            await snowflake.execute('select 1', undefined, 5_000);
            now.mockReturnValue(1_000 + 5_001);
            await snowflake.execute('select 1', undefined, 5_000);

            expect(fake.connection.execute).toHaveBeenCalledTimes(2);
        });

        it('skips caching entirely when the TTL is zero', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await snowflake.execute('select 1', undefined, 0);
            await snowflake.execute('select 1', undefined, 0);

            expect(fake.connection.execute).toHaveBeenCalledTimes(2);
        });

        it('rejects with a SnowflakeError and does not cache a failed result', async () => {
            const driverError = Object.assign(new Error('boom'), { code: 1 }) as SDK.SnowflakeError;
            const fake = fakeConnection({ err: driverError });
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await expect(snowflake.execute('select bad')).rejects.toBeInstanceOf(SnowflakeError);
            await expect(snowflake.execute('select bad')).rejects.toBeInstanceOf(SnowflakeError);

            expect(fake.connection.execute).toHaveBeenCalledTimes(2);
        });

        it('uses the raw SQL as the cache key when hashing is disabled', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await snowflake.execute('select 1', undefined, 60000, false);
            await snowflake.execute('select 1', undefined, 60000, false);

            expect(fake.connection.execute).toHaveBeenCalledTimes(1);
        });
    });

    describe('per-instance isolation', () => {
        it('does not share cached results between two instances', async () => {
            const first = fakeConnection({ rows: [{ FROM: 'a' }] });
            const second = fakeConnection({ rows: [{ FROM: 'b' }] });

            mockedSDK.createConnection.mockReturnValueOnce(first.connection as never);
            const a = new Snowflake({ ...CONNECTION_OPTIONS, database: 'A' });
            mockedSDK.createConnection.mockReturnValueOnce(second.connection as never);
            const b = new Snowflake({ ...CONNECTION_OPTIONS, database: 'B' });

            await expect(a.execute('select 1')).resolves.toEqual([{ FROM: 'a' }]);
            await expect(b.execute('select 1')).resolves.toEqual([{ FROM: 'b' }]);

            expect(first.connection.execute).toHaveBeenCalledTimes(1);
            expect(second.connection.execute).toHaveBeenCalledTimes(1);
        });

        it('does not expose one instance statement handles to another', () => {
            const first = fakeConnection();
            const second = fakeConnection();

            mockedSDK.createConnection.mockReturnValueOnce(first.connection as never);
            const a = new Snowflake(CONNECTION_OPTIONS);
            mockedSDK.createConnection.mockReturnValueOnce(second.connection as never);
            const b = new Snowflake(CONNECTION_OPTIONS);

            const stmtId = a.createStatement('select 1', () => undefined);

            expect(a.getStatementSQLText(stmtId)).toBe('select 1');
            expect(() => b.getStatementSQLText(stmtId)).toThrow(SnowflakeError);
        });
    });

    describe('statement accessors', () => {
        it('returns the request id rather than invoking the result', () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            const stmtId = snowflake.createStatement('select 1', () => undefined);

            expect(snowflake.getRequestId(stmtId)).toBe('request-id-123');
        });

        it('throws for an unknown statement id', () => {
            mockedSDK.createConnection.mockReturnValue(fakeConnection().connection as never);
            const snowflake = new Snowflake(CONNECTION_OPTIONS);

            expect(() => snowflake.getNumRows('nope')).toThrow(SnowflakeError);
        });

        it('forgets a statement after it is cancelled', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            const stmtId = snowflake.createStatement('select 1', () => undefined);
            await snowflake.cancel(stmtId);

            expect(() => snowflake.getNumRows(stmtId)).toThrow(SnowflakeError);
        });

        it('hands non-streaming rows to the completion callback', () => {
            const fake = fakeConnection({ rows: [{ A: 1 }] });
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const onComplete = jest.fn();
            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            snowflake.createStatement('select 1', onComplete);

            expect(onComplete).toHaveBeenCalledWith(undefined, [{ A: 1 }]);
        });
    });

    describe('cache store', () => {
        it('converts the millisecond TTL to seconds before writing to redis', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS, 'redis');
            await snowflake.execute('select 1', undefined, 60000);

            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const Redis = require('ioredis');
            const redisInstance = Redis.mock.instances[0];
            expect(redisInstance.set).toHaveBeenCalledWith(
                expect.any(String),
                JSON.stringify({ data: [{ N: 1 }] }),
                'EX',
                60
            );
        });

        it('never writes a sub-second TTL of zero', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS, 'redis');
            await snowflake.execute('select 1', undefined, 100);

            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const Redis = require('ioredis');
            const redisInstance = Redis.mock.instances[0];
            expect(redisInstance.set).toHaveBeenCalledWith(
                expect.any(String),
                expect.any(String),
                'EX',
                1
            );
        });

        it('rejects an unsupported cache store', () => {
            mockedSDK.createConnection.mockReturnValue(fakeConnection().connection as never);

            expect(
                () => new Snowflake(CONNECTION_OPTIONS, 'memcached' as never)
            ).toThrow(SnowflakeError);
        });
    });

    describe('connection lifecycle', () => {
        it('resolves connect and reports the connection as up', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await expect(snowflake.connect()).resolves.toBeUndefined();
            await expect(snowflake.isConnectionUp()).resolves.toBe(true);
            await expect(snowflake.isValidConnection()).resolves.toBe(true);
        });

        it('rejects connect when the driver reports an error', async () => {
            const fake = fakeConnection();
            const driverError = Object.assign(new Error('nope'), { code: 1 }) as SDK.SnowflakeError;
            fake.connection.connect.mockImplementation((cb?: SDK.ConnectionCallback) => {
                cb?.(driverError, fake.connection as never);
                return fake.connection;
            });
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await expect(snowflake.connect()).rejects.toBe(driverError);
        });

        it('resolves destroy', async () => {
            const fake = fakeConnection();
            mockedSDK.createConnection.mockReturnValue(fake.connection as never);

            const snowflake = new Snowflake(CONNECTION_OPTIONS);
            await expect(snowflake.destroy()).resolves.toBeUndefined();
            expect(fake.connection.destroy).toHaveBeenCalled();
        });
    });
});
