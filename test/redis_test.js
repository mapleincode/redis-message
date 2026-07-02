/**
 * RedisMethod 底层 Redis 操作测试
 * 测试 key 命名、消息序列化/反序列化、队列操作等功能
 */

const Redis = require('../src/redis-method').default;
// const IORedis = require('ioredis');
// const _redis = new IORedis();
const should = require('should');

let redis;
let _redis;

let topic;
let keyHeader;


describe('redis method', function() {
    before(function() {
        _redis = {
            kv: {},
            list: {},
            queue: {}
        };
        _redis.init = function() {
            this.kv = {};
            this.list = {};
            this.queue = {};
        }

        topic = parseInt(Math.random() * 1000 + 666).toString();
        keyHeader = parseInt(Math.random() * 1000 + 666).toString();

        redis = new Redis(_redis, { topic, keyHeader });
    });

    beforeEach(function() {
        _redis.init();
    });

    it('mq default key header', function() {
        let __redis = new Redis(_redis, { topic });
        __redis.MQ_NAME.should.equal(`msg_-${topic}-redis-mq`);
    });

    it('mq name', function() {
        const mqName = redis.MQ_NAME;
        mqName.should.equal(`${keyHeader}-${topic}-redis-mq`);
    });

    it('mq hash name', function() {
        const mqHashName = redis.MQ_HASH_NAME;
        mqHashName.should.equal(`${keyHeader}-${topic}-redis-hash`);
    });

    it('mq hash retry key', function() {
        const mqHashRetryKey = redis.MQ_HASH_RETRY_TIMES;
        mqHashRetryKey.should.equal(`${keyHeader}-${topic}-redis-retry-hash`);
    });

    it('lock pull key', function() {
        redis.LOCK_PULL_KEY.should.equal(`${keyHeader}-${topic}-pull-lock`);
    });

    it('lock check key', function() {
        redis.LOCK_CHECK_KEY.should.equal(`${keyHeader}-${topic}-check-lock`)
    });

    it('pack message with string', function() {
        const data = 'hello';
        const msgType = 'foo1';

        const result = redis.packMessage(data, msgType);
        result.should.equal('{"data":"hello","msgType":"foo1"}');
    });

    it('pack message with json', function() {
        const data = '{"foo":"bar"}';
        const msgType = 'foo2';

        const result = redis.packMessage(data, msgType);
        result.should.equal('{"data":{"foo":"bar"},"msgType":"foo2"}');
    });

    it('pack message with object', function() {
        const data = {
            foo: {
                bar: 'qaq'
            }
        };
        const msgType = 'foo3';
        const result = redis.packMessage(data, msgType);
        result.should.equal('{"data":{"foo":{"bar":"qaq"}},"msgType":"foo3"}');
    });

    it('unpack message', function() {
        const str1 = null;
        const str2 = undefined;
        const str3 = 'foo';
        const str4 = '{"data":{"foo":"bar"},"msgType":"foo2"}';

        // null/undefined 输入应返回 null
        should(redis.unpackMessage(str1)).be.Null();
        should(redis.unpackMessage(str2)).be.Null();
        // 非 JSON 字符串应返回 unknown 类型
        should(redis.unpackMessage(str3)).deepEqual({ msgType: 'unknown', data: 'foo' });
        // 正常 JSON 应正确解析
        should(redis.unpackMessage(str4)).deepEqual({
            data: {
                foo: 'bar'
            },
            msgType: 'foo2'
        });
    });

    it('redis expire', async function() {
        _redis.expire = function(key, time) {
            return { key, time };
        };

        const { key, time } = await redis.expire('key', 'timestamp');
        key.should.equal('key');
        time.should.equal('timestamp');

        delete _redis.expire;
    });

    it('message count', async function() {
        _redis.llen = await function(mqName) {
            return mqName;
        };

        const len = await redis.messageCount();
        len.should.equal(redis.MQ_NAME);
        
        delete _redis.llen;
    });

    it('set pull lock', async function() {
        // 注意: 当前实现使用 RedisLock 库，以下测试基于旧的 incr 实现逻辑
        // 保留测试以验证基本功能
        let lockKey = null;
        _redis.incr = function(key) {
            lockKey = key;
            return 2;
        }

        _redis.ttl = function(key) {
            return -2;
        }

        _redis.expire = function() {

        }

        let lock = await redis.setPullLock();

        lockKey.should.be.equal(redis.LOCK_PULL_KEY);

        delete _redis.incr;
    });

    it('clean pull lock', async function() {
        _redis.del = async function(key) {
            return key;
        };

        const delKey = await redis.cleanPullLock();
        should(delKey).equal(undefined);
    });

    it('set check lock', async function() {
        // 注意: 当前实现使用 RedisLock 库，以下测试基于旧的 incr 实现逻辑
        // 保留测试以验证基本功能
        let lockKey = null;
        _redis.incr = function(key) {
            lockKey = key;
            return 2;
        }

        _redis.ttl = function(key) {
            return -2;
        }

        _redis.expire = function() {

        }

        let lock = await redis.setCheckLock();

        lockKey.should.be.equal(redis.LOCK_CHECK_KEY);
    });

    it('clean check lock', async function() {
        _redis.del = async function(key) {
            return key;
        };

        const delKey = await redis.cleanCheckLock();
        should(delKey).equal(undefined);
    });
});