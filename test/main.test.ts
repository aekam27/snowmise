const snowflake = require('../dist/pkg/index.js');
describe('index.ts', () => {
  test('Package is initialize and connected', async () => {
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
    expect(snowmise.id).not.toEqual(null);
    const isUp = await snowmise.isConnectionUp()
    expect(isUp).toEqual(true);
    const isVailidAsync = await snowmise.isValidConnection()
    expect(isVailidAsync).toEqual(true);
    await snowmise.destroy();
    const isVailidAsyncAfterDestroy = await snowmise.isValidConnection()
    expect(isVailidAsyncAfterDestroy).toEqual(false);
    const isUpAfterDestroy = await snowmise.isConnectionUp();
    expect(isUpAfterDestroy).toEqual(false);
  });
});