/**
 * RedisMessage 类
 * create by maple 2018-11-16 11:06:59
 */
'use strict';

const RedisMethod = require('./redis-method');
const sleep = require('./utils').sleep;
const now = require('./utils').now;
const debug = require('debug')('redis-message');

class RedisMessage {
    constructor(options) {
        const {
            topic, // topic
            messageType, // messageType
            redis, // redis
            keyHeader = 'msg_', // redis key header
            maxAckTimeout = 60 * 1000, // 消费超时时间 
            eachMessageCount = 200, // 每次 Message 获取数量
            minRedisMessageCount = 200, // Redis 最少的 count 数量
            maxRetryTimes = 5, // 消息消费失败重新消费次数
            lockExpireTime = 60, // Lock timeout

            fetchMessage = function (options) {
                // const { topic, messageType } = options;
                return async function () {
                    return [];
                }
            },
            afterFetchMessage = function (options) {
                // const { topic, messageType } = options;
                return async function (data = {}) {
                    const {
                        offset
                    } = data;
                    debug(`message has been offset to :${offset}`);
                }
            },
            dealFailedMessage = function (options) {
                // const { topic, messageType } = options;
                return async function (messageId, detail) {
                    debug(`messageId: ${messageId} has been error acks`);
                    console.log(`message failed!! id: ${messageId} data: ${JSON.stringify(detail)}`);
                    return;
                }
            }
        } = options;

        this.options = {
            topic,
            messageType,
            redis,
            keyHeader,
            lockExpireTime
        };

        this.redis = new RedisMethod(redis, options);

        this.fetchMessage = fetchMessage(options);
        this.afterFetchMessage = afterFetchMessage(options);
        this.dealFailedMessage = dealFailedMessage(options);

        this.maxAckTimeout = maxAckTimeout;
        this.maxAckTimeoutSecords = parseInt(maxAckTimeout / 1000);
        this.eachMessageCount = eachMessageCount;
        this.minRedisMessageCount = minRedisMessageCount;
        this.maxRetryTimes = maxRetryTimes;
    }

    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    async _dealFailedMessage(messageId) {
        // 获取失败次数
        const failedTimes = await this.redis.incrFailedTimes(messageId);

        if (failedTimes > this.maxRetryTimes) {
            let detail;
            try {
                detail = await this.redis.cleanFailedMsg(messageId);
            } catch(err) {
                console.error(err);
            }
            // 最后调用 deal 函数
            await this.dealFailedMessage(messageId, detail);

            return;
        }
        // 初始化调用时间
        // 重新 push 到队列中
        await this.redis.initTimeAndRpush(messageId);
        return;
    };
    async _pull() {
        const list = await this.fetchMessage();

        let offset;

        try {
            for (const msg of list) {
                const {
                    id, // 唯一 id
                    data, // data
                    msgType // 消息类型
                } = msg;
                await this.redis.pushMessage(id, data, msgType);
                // 更新 offset
                offset = id + 1;
            }
        } catch (ex) {
            console.error(ex);
        }

        await this.afterFetchMessage({
            offset: offset,
            noChange: !offset
        });
        await this.redis.cleanPullLock();
    }
    /**
     * 获取 messgae 数据
     * @return {boolean} 是否成功 pull
     */
    async pullMessage(mqCount) {
        // 先 lock
        const lockStatus = await this.redis.setPullLock();
        if (!lockStatus) return false;
        // fetch message list
        const self = this;
        // Async
        if(mqCount === 0) {
            // 如果不 pull 数据可能为 0,所以还是等 pull message 完成再返回消息
            await self._pull(); // 直接 pull
        } else {
            // 如果数据总不是不是 0 ，那就异步 pull message。先返回消息。保证消息的延迟。
            setTimeout(function() {
                self._pull();
            }, 1000);
        }

        return true;
    }

    /**
     * 获取单个消息
     * @return {object} 消息  { messageId, data }
     */
    async getOneMessage() {
        const messageId = await this.redis.lpopMessage();
        if (!messageId) {
            return null;
        }
        const data = await this.redis.fetchMessageAndSetTime(messageId);
        return {
            messageId: messageId,
            data: data.data,
            msgType: data.msgType
        };
    }

    /**
     * 根据数量返回消息 list
     * @param {integer} size 需要获得的消息数量
     * @return {array}  返回消息的数组
     */
    async getMessages(size) {
        const self = this;
        if (!size || size < 1) {
            size = 1;
        }

        const mqCount = await this.redis.messageCount();

        if (mqCount < this.minRedisMessageCount) {
            const status = await this.pullMessage(mqCount); // async
            if (status === false) {
                // 没有更新
                await sleep(500);
            }
        }
        const list = await this.redis.fetchMultiMessage(size);
        return list;
    }
    /**
     * 成功消费消息或者失败消费消息
     * @param {string|array} messageIds 消息 id 或消息 id 数组
     * @param {boolean} success boolean 是否是成功
     */
    async ackMessages(messageIds, success) {
        messageIds = [].concat(messageIds);
        debug(`获得消息的数量为: ${messageIds.length}`);
        for (const messageId of messageIds) {
            let _messageId;
            let _success;
            if(typeof messageId === 'object') {
                _messageId = messageId.messageId || messageId.id;
                _success = messageId.success;
            }
            _messageId = _messageId || messageId;
            _success = _success || success;

            const time = await this.redis.getTime(_messageId);
            if (!time) {
                // 已超时被处理
                const existsStatus = await this.redis.checkTimeExists(_messageId);
                if(existsStatus) {
                    debug(`messageId: ${_messageId} 超时被置为  null`);
                    continue;
                }
                debug(`messageId: ${_messageId} 没有 time`);
                _success = true;
            }

            if (_success) {
                debug(`messageId: ${_messageId} 设置成功`);
                await this.redis.cleanMsg(_messageId);
            } else {
                debug(`messageId: ${_messageId} 没有 time`);
                debug(`messageId: ${_messageId} 设置失败`);
                await this._dealFailedMessage(_messageId);
            }
        }
    }
    /**
     * 1. 检查消息是否有异常
     * 2. 检查消息消费是否超时
     */
    async checkExpireMessage() {
        const status = await this.redis.setCheckLock();
        if (!status) return;
        const mqCount = await this.redis.messageCount();
        const mqMessages = await this.redis.getMessageList(0, mqCount + this.eachMessageCount);
        const hashMap = await this.redis.getTimeMap();

        const missingList = [];
        const timeoutList = [];

        const keys = Object.keys(hashMap);

        for (const key of keys) {
            await sleep(0);
            const index = mqMessages.indexOf(key)
            if (index < 0 && !hashMap[key]) {
                // 数据缺失
                missingList.push(key);
            } else if (index < 0 && hashMap[key] && (now() - hashMap[key]) > this.maxAckTimeoutSecords) {
                // 数据 ack 超时
                timeoutList.push(key);
            }
        }

        for (const messageId of missingList) {
            const existsStatus = await this.redis.checkTimeExists(messageId);
            if (!existsStatus) { // 数据延迟问题
                continue;
            }
            await this.redis.rpushMessage(messageId);
        }

        for (const messageId of timeoutList) {
            const time = await this.redis.getTime(messageId);
            if (!time) { // 数据延迟 null or 不存在
                continue;
            }
            await this._dealFailedMessage(messageId);
        }
        // 清理锁
        await this.redis.cleanCheckLock();
        return {
            timeoutList: timeoutList,
            missingList: missingList
        }
    }
}

module.exports = RedisMessage;
