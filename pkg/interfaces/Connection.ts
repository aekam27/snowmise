
export interface ConnectionOptions {
    account: string;
    /** @deprecated */
    region?: string | undefined;

    application?: string | undefined;
    authenticator?: "SNOWFLAKE" | "EXTERNALBROWSER" | "OAUTH" | "SNOWFLAKE_JWT";
    username?: string | undefined;
    password?: string | undefined;
    token?: string | undefined;
    privateKey?: string;
    privateKeyPath?: string | undefined;
    privateKeyPass?: string | undefined;

    accessUrl?: string | undefined;
    browserActionTimeout?: number | undefined;
    clientSessionKeepAlive?: boolean | undefined;
    clientSessionKeepAliveHeartbeatFrequency?: number | undefined;

    database?: string | undefined;
    host?: string | undefined;
    keepAlive?: boolean | undefined;

    noProxy?: string | undefined;
    proxyHost?: string | undefined;
    proxyPassword?: string | undefined;
    proxyPort?: number | undefined;
    proxyProtocol?: string | undefined;
    proxyUser?: string | undefined;
    role?: string | undefined;
    schema?: string | undefined;
    timeout?: number | undefined;
    warehouse?: string | undefined;
    jsTreatIntegerAsBigInt?: boolean | undefined;
}