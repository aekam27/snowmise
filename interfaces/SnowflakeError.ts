export class SnowflakeError extends Error {
    constructor(errmsg:string){super(errmsg || 'Oops! Snowmise got caught in an inescapable trap.')}
}
