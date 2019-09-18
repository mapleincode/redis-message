const should = require('should');
const ioredis = require('ioredis');
const RedisMessage = require('../index').RedisMessage;
const debug = require('debug')('redis-message:test:message');

const body = {
    name: 'Bob'
};

let indexId = 0;
let fetchMessageLengthLimit = 0;
let afterFetchMessageData = null;

let writeToDB = false;

function sleep(time) {
    return new Promise(function (resolve) {
        setTimeout(resolve, time);
    });
}

const fetchMessage = function (options) {
    return async function () {
        const list = [];
        while (fetchMessageLengthLimit-- > 0) {
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

const afterFetchMessage = function (options) {
    return async function (data = {}) {
        afterFetchMessageData = data;
    };
}

const dealFailedMessage = function (options) {
    return async function (messageId, detail) {
        writeToDB = true;
    };
};

let _redis;

let redisMessage;

let redis;

describe('order consume', function() {
    before(async function() {
        _redis = new ioredis(6380, '192.168.2.120');

        redisMessage = new RedisMessage({
            redis: _redis,
            topic: 'topic',
            fetchMessage: fetchMessage,
            afterFetchMessage: afterFetchMessage,
            dealFailedMessage: dealFailedMessage,
            orderConsumption: true
        });

        redis = redisMessage.redis;
    });

    beforeEach(async function () {
        await _redis.flushdb(); // 清理 redis 数据库
        indexId = 0;
        fetchMessageLengthLimit = 0; // 可 pull 数据为 0
        afterFetchMessageData = null; // 初始化 afterFetchMessageData 为 null

        debug('begin test unit');
    });

    afterEach(async function() {
        await sleep(1500); // 存在异步操作可能要 1 s 后才执行,所以需要上一步先执行完
    })

    after(async function () {
        this.timeout(0);
        await _redis.flushdb();
        await sleep(2000); // 存在异步操作可能要 1 s 后才执行
        await _redis.quit();
    });

    it('fetch order & ack false', async function() {
        fetchMessageLengthLimit = 4;
        
        let datas = await redisMessage.getMessages(2);

        datas.should.be.an.Array();

        let data = datas[0];

        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });

        await redisMessage.ackMessages(false);

        const keyLength = await redis.messageCount();

        keyLength.should.equal(4);

        datas = await redisMessage.getMessages(2);

        datas.should.be.an.Array();

        data = datas[0];

        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });
    })


    it('fetch order & ack true', async function() {
        fetchMessageLengthLimit = 4;
        
        let datas = await redisMessage.getMessages(2);

        datas.should.be.an.Array();

        let data = datas[0];

        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-0',
            msgType: 'msgType'
        });

        await redisMessage.ackMessages(true);

        const keyLength = await redis.messageCount();

        keyLength.should.equal(2);

        datas = await redisMessage.getMessages(2);

        datas.should.be.an.Array();

        data = datas[0];

        data.should.deepEqual({
            data: {
                name: 'Bob'
            },
            messageId: 'topic-2',
            msgType: 'msgType'
        });
    })

    it('fetch order without ack', async function() {
        fetchMessageLengthLimit = 4;
        let datas = await redisMessage.getMessages(1);

        datas.should.be.an.Array();
        datas.should.have.length(1);

        datas = await redisMessage.getMessages(1);

        datas.should.be.an.Array();
        datas.should.have.length(0);

        await redisMessage.ackMessages(true);

        datas = await redisMessage.getMessages(1);

        datas.should.be.an.Array();
        datas.should.have.length(1);
    });

    it('check without error', async function() {
        fetchMessageLengthLimit = 4;
        let datas = await redisMessage.getMessages(1);

        datas.should.be.an.Array();
        datas.should.have.length(1);
        await redisMessage.ackMessages(true);

        const { missingList, timeoutList } = await redisMessage.checkExpireMessage(false);
        missingList.length.should.equal(0);
        timeoutList.length.should.equal(0);
    });

    it('check with no ack', async function() {
        it('check without error', async function() {
            fetchMessageLengthLimit = 4;
            let datas = await redisMessage.getMessages(1);
    
            datas.should.be.an.Array();
            datas.should.have.length(1);
    
            const { missingList, timeoutList } = await redisMessage.checkExpireMessage(false);
            missingList.length.should.equal(0);
            timeoutList.length.should.equal(0);
        });
    });
    
});