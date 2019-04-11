const should = require('should');
const ioredis = require('ioredis');
const _redis = new ioredis(6379, '192.168.2.118');
const RedisMessage = require('../index').RedisMessage;



const body = {
    name: 'Bob'
};
let indexId = 0;
let fetchMessageLengthLimit = 0;
let afterFetchMessageData = null;

function sleep(time) {
    return new Promise(function(resolve) {
        setTimeout(resolve, time);
    });
}

const fetchMessage = function(options) {
    return async function() {
        const list = [];
        while(fetchMessageLengthLimit-- > 0) {
            list.push({
                id: indexId++,
                data: body,
                msgType: 'msgType'
            });
        }
        await sleep(0);
        return list;
    };
};

const afterFetchMessage = function(options) {
    return async function(data = {}) {
        afterFetchMessageData = data;
    };
}

const dealFailedMessage = function(options) {
    return async function(messageId, detail) {

    };
};



const redisMessage = new RedisMessage({
    redis: _redis,
    topic: 'topic',
    fetchMessage: fetchMessage,
    afterFetchMessage: afterFetchMessage,
    dealFailedMessage: dealFailedMessage
});

const redis = redisMessage.redis;

describe('redis message', function() {
    beforeEach(async function() {
        await _redis.flushdb(); // 清理 redis 数据库
        indexId = 0;
        fetchMessageLengthLimit = 0; // 可 pull 数据为 0
        afterFetchMessageData = null; // 初始化 afterFetchMessageData 为 null
        await sleep(500);
    });

    after(async function() {
        this.timeout(0);
        await _redis.flushdb();
        await sleep(1000);
        await _redis.quit();
    });

    it('fetch message basic', async function() {
        fetchMessageLengthLimit = 5;
        const msgs = await redisMessage.fetchMessage();
        msgs.length.should.equal(5);
        const msg = msgs[0];
        msg.should.be.an.Object();
        msg.should.have.keys('id', 'data', 'msgType');
    });

    it('pull message basic', async function() {
        fetchMessageLengthLimit = 5;
        await redisMessage._pull();
        afterFetchMessageData.should.be.an.Object();
        afterFetchMessageData.should.deepEqual({ offset: 5, noChange: false });
        const msgLength = await redis.messageCount();
        msgLength.should.equal(5);
    });

    it('pull message basic with no message', async function() {
        await redisMessage._pull();
        afterFetchMessageData.should.be.an.Object();
        afterFetchMessageData.should.deepEqual({ offset: undefined, noChange: true });
        const msgLength = await redis.messageCount();
        msgLength.should.equal(0);
    });

    it('pull message', async function() {
        fetchMessageLengthLimit = 5;

        const result = await redisMessage.pullMessage(0);
        result.should.equal(true);
        const msgLength = await redis.messageCount();
        msgLength.should.equal(5);
    });

    it('pull message async', async function() {
        this.timeout(5000);
        fetchMessageLengthLimit = 5;

        const result = await redisMessage.pullMessage(100);
        result.should.equal(true);
        let msgLength = await redis.messageCount();
        msgLength.should.equal(0);
        await sleep(2000);
        msgLength = await redis.messageCount();
        msgLength.should.equal(5);
    });

    it('pull message lock', async function() {
        const b1 = await redisMessage.pullMessage(100); // 模拟异步执行
        const b2 = await redisMessage.pullMessage(100); // 模拟异步执行

        (b1 && b2).should.be.False;
        (b1 || b2).should.be.True;
    });

    it('fetch one message basic', async function() {
        fetchMessageLengthLimit = 1;
        let data = await redisMessage.getOneMessage();
        should(data).equal(null);

        await redisMessage.pullMessage(0);

        data = await redisMessage.getOneMessage();

        data.should.be.an.Object();
        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });
    });

    it('fetch multi message', async function() {
        fetchMessageLengthLimit = 10;
        const datas = await redisMessage.getMessages(5);
        datas.should.be.an.Array();
        datas.length.should.equal(5);
        const data = datas[0];
        data.should.be.an.Object();
        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });
    });

    it('fetch no message', async function() {
        const datas = await redisMessage.getMessages(5);
        datas.should.be.an.Array();
        datas.length.should.equal(0);
    });

    it('ack success message', async function() {
        fetchMessageLengthLimit = 10;
        const datas = await redisMessage.getMessages(1);
        datas.should.be.an.Array();
        datas.length.should.equal(1);
        const data = datas[0];
        data.should.be.an.Object();
        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });

        const messageId = data.messageId;
        await redisMessage.ackMessages(messageId, true);

        const b = await redis.checkTimeExists(messageId);
        b.should.be.False;
        const detail = await redis.getDetail(messageId);
        should(detail).be.Null;
    });

    it('ack failed message', async function() {
        fetchMessageLengthLimit = 1;
        const datas = await redisMessage.getMessages(1);
        datas.should.be.an.Array();
        datas.length.should.equal(1);
        const data = datas[0];
        data.should.be.an.Object();
        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });

        const messageId = data.messageId;
        await redisMessage.ackMessages(messageId, false);

        const b = await redis.checkTimeExists(messageId);
        b.should.equal(1);
        const detail = await redis.getDetail(messageId);
        should(detail).be.Object;

        const msgLength = await redis.messageCount();
        msgLength.should.equal(1);

        const failedTimes = await redis.incrFailedTimes(messageId);
        failedTimes.should.equal(1 + 1);
    });

    it('ack multi message', async function() {
        fetchMessageLengthLimit = 10;
        const datas = await redisMessage.getMessages(10);
        datas.should.be.an.Array();
        datas.length.should.equal(10);

        const ids = datas.map(d => d.messageId);
        await redisMessage.ackMessages(ids, true);

        const msgLength = await redis.messageCount();
        msgLength.should.equal(0);
    });

    it('ack message timeout', async function() {
        // 超时没法测试啊!
    });

    it('ack message multi with not fetch message', async function() {
        fetchMessageLengthLimit = 1;
        const datas = await redisMessage.getMessages(1);
        const data = datas[0];
        const messageId = data.messageId;
        let time = 10;
        while(time --) {
            await redisMessage.ackMessages(messageId, false);
            const msgLength = await redis.messageCount();
            msgLength.should.equal(1);
        }
    });

    it('ack message multi failed', async function() {
        this.timeout(3000);
        fetchMessageLengthLimit = 1;
        const datas = await redisMessage.getMessages(1);
        const data = datas[0];
        const messageId = data.messageId;
        let time = 6;
        while(time --) {
            if (time !== 5) {
                await redisMessage.getMessages(1);
            }
            await redisMessage.ackMessages(messageId, false);
            const msgLength = await redis.messageCount();
            msgLength.should.equal(time === 0 ? 0 : 1);
        }
    });

    it('message unconsumed', async function() {
        fetchMessageLengthLimit = 2;
        await redisMessage.pullMessage(0);

        const result = await redisMessage.__messageUnconsumed();
        result.should.be.an.Object();
        result.should.have.keys('itemsLength', 'items');

        const { itemsLength, items } = result;
        itemsLength.should.equal(2);
        items.should.be.an.Array();
        items.should.have.length(2);
    });

    it('message consuming', async function() {
        fetchMessageLengthLimit = 4;
        await redisMessage.pullMessage(0);
        await redisMessage.getMessages(2);

        const result = await redisMessage.__messageUnconsumed();
        result.should.be.an.Object();
        result.should.have.keys('itemsLength', 'items');

        const { itemsLength, items } = result;
        itemsLength.should.equal(2);
        items.should.be.an.Array();
        items.should.have.length(2);

        const map = await redisMessage.__messageConsuming();
        map.should.be.an.Object();
        map.should.have.size(4);
    });
});