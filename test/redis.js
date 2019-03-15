const Redis = require('../lib/redis-method');
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

        const result = redis._packMessage(data, msgType);
        result.should.equal('{"data":"hello","msgType":"foo1"}');
    });

    it('pack message with json', function() {
        const data = '{"foo":"bar"}';
        const msgType = 'foo2';

        const result = redis._packMessage(data, msgType);
        result.should.equal('{"data":{"foo":"bar"},"msgType":"foo2"}');
    });

    it('pack message with object', function() {
        const data = {
            foo: {
                bar: 'qaq'
            }
        };
        const msgType = 'foo3';
        const result = redis._packMessage(data, msgType);
        result.should.equal('{"data":{"foo":{"bar":"qaq"}},"msgType":"foo3"}');
    });

    it('unpack message', function() {
        const str1 = null;
        const str2 = undefined;
        const str3 = 'foo';
        const str4 = '{"data":{"foo":"bar"},"msgType":"foo2"}';

        should(redis._unpackMessage(str1)).be.Null;
        should(redis._unpackMessage(str2)).be.Null;
        should(redis._unpackMessage(str3)).equal('foo');
        should(redis._unpackMessage(str4)).deepEqual({
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

        const { key, time } = await redis._expire('key', 'timestamp');
        key.should.equal('key');
        time.should.equal('timestamp');

        delete _redis.expire;
    });

    it('get message id', function() {

    });
});