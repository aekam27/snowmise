# snowmise
# beta
# Supports node-cache, redis cache
 
const snowflake = require('snowmise');
const {Snowflake} = snowflake;
const snowmise = new Snowflake({
      account: "",
      username: "",
      password: "",
      database: "",
      schema: "",
      warehouse: ""
})
await snowmise.connect()

or

import {Snowflake} from 'snowmise';
const snowmise = new Snowflake({
      account: "",
      username: "",
      password: "",
      database: "",
      schema: "",
      warehouse: ""
})
await snowmise.connect()