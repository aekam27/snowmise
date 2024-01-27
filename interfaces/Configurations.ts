export interface ConfigurationOptions {
    logLevel?: "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" | undefined;
    insecureConnect?: boolean | undefined;
    /**
     * ### Related Docs
     * - {@link https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#choosing-fail-open-or-fail-close-mode Choosing `Fail-Open` or `Fail-Close` Mode}
     */
    ocspFailOpen?: boolean | undefined;
}
