const Ioredis = require("ioredis");
const redis = new Ioredis(6380, '192.168.2.120', { db: 0 });


async function main() {
  const results = await redis.multi()
    .set('foo', 'bar')
    .get('foo')
    .exec();

  // const results = await redis.multi(
  //   [
  //     ['SET', 'foo', 'bar'],
  //     ['GET', 'foo']
  //   ]
  // ).exec();

  console.log(results, typeof results);

}

main()
.catch(err => console.error(err));
