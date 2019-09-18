/**
 * RedisMessage 类
 * create by maple 2018-11-16 11:06:59
 */
'use strict';

const RedisMethod = require('./redis-method');
const sleep = require('./utils').sleep;
const now = require('./utils').now;
const defaultLogger = require('./logger');
const debug = require('debug')('redis-message');

class RedisMessage {
    constructor(options) {
        const {
            topic, // topic 用于作为标识，在 redis key 进行区分
            messageType, // messageType

            // ============== 以下是推荐默认参数 ================
            keyHeader = 'msg_', // redis key header 默认 msg_
            lockExpireTime = 60, // redis lock 默认时间
            maxAckTimeout = 60 * 1000, // 消费超时时间 默认 60s. 
            eachMessageCount = 200, // 每次 Message 获取数量
            minRedisMessageCount = 200, // Redis 最少的 count 数量
            maxRetryTimes = 5, // 消息消费失败重新消费次数
            recordFailedMessage = true, // 记录失败的数据

            // ================= Mode 功能选项 ===================
            orderConsumption = false, // 是否支持顺序消费
            autoAck = false, // 是否支持 auto-ack 模式
            // 如果在待消费时间内为收到错误，则自动进入消费模式并且不计入数据库


            // 三个 Main 函数
            // 1. fetch
            // 2. after fetch
            // 3. failed msg deal
            fetchMessage = function (/* { topic, messageType } */) {
                return async function () {
                    return [];
                };
            },
            afterFetchMessage = function (/* { topic, messageType } */) {
                // const  = options;
                return async function (data = {}) {
                    const {
                        offset
                    } = data;
                    debug(`message has been offset to :${offset}`);
                };
            },
            dealFailedMessage = function (/* { topic, messageType } */) {
                return async function (messageId, detail) {
                    debug(`messageId: ${messageId} has been error acks`);
                    logger.warn(`message failed!! id: ${messageId} data: ${JSON.stringify(detail)}`);
                    return;
                };
            },
            handleFailedMessage,

            // ================== other =======================
            logger = defaultLogger, // logger
            redis // ioredis 实例
        } = options;

        this.options = {
            topic, // topic 用于作为标识，在 redis key 进行区分
            messageType, // messageType 数据源
            redis, // ioredis 实例
            logger,

            // ============== 以下是推荐默认参数 ================
            keyHeader, // redis key header 默认 msg_
            lockExpireTime, // redis lock 默认时间
            maxAckTimeout: maxAckTimeout, // 消费超时时间 默认 60s. 
            maxAckTimeoutSecords: parseInt(maxAckTimeout / 1000), // 转换成单位秒(s)
            eachMessageCount: eachMessageCount, // 每次 Message 获取数量
            minRedisMessageCount: minRedisMessageCount, // Redis 最少的 count 数量
            maxRetryTimes: maxRetryTimes, // 消息消费失败重新消费次数
            recordFailedMessage, // 记录失败的数据 默认 true

            // ================= Mode 功能选项 ===================
            orderConsumption, // 是否支持顺序消费 默认 false
            autoAck, // 是否支持 auto-ack 模式 默认 false
            // 如果在待消费时间内为收到错误，则自动进入消费模式并且不计入数据库

            // 三个 Main 函数
            // 1. fetch
            // 2. after fetch
            // 3. failed msg deal
            fetchMessage: fetchMessage(options),
            afterFetchMessage: afterFetchMessage(options),
            handleFailedMessage: handleFailedMessage ? handleFailedMessage(options) : dealFailedMessage(options)
        };

        // 顺序消费模式
        if (this.options.orderConsumption) {
            this.options.recordFailedMessage = true; // 必须记录消息
            
            if (this.options.maxRetryTimes < 2) {
                this.options.maxRetryTimes = 2; // 至少保证一次重试
            }
        // 自动 ack 模式
        } else if (this.options.autoAck) {
            this.options.maxRetryTimes = 0;
            this.options.recordFailedMessage = false;
        }


        this.fetchMessage = this.options.fetchMessage;
        this.afterFetchMessage = this.options.afterFetchMessage;
        this.handleFailedMessage = this.options.handleFailedMessage;

        // ioredis 实例
        this.redis = new RedisMethod(this.options.redis, options);

        // logger
        this.logger = this.options.logger;
    }

    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    async _handleFailedMessage(messageId) {
        // 获取失败次数
        const failedTimes = await this.redis.incrFailedTimes(messageId);

        if (failedTimes > this.options.maxRetryTimes) {
            let detail;
            try {
                detail = await this.redis.cleanFailedMsg(messageId);
            } catch (err) {
                this.logger.error(err);
            }
            // 最后调用 deal 函数
            await this.handleFailedMessage(messageId, detail);
            return;
        }

        // 初始化调用时间
        // 重新 push 到队列中
        await this.redis.initTimeAndRpush(messageId);
    }

    async _pullMessage() {
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
        } catch (err) {
            this.logger.error(err);
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
        if (mqCount === 0) {
            // 如果不 pull 数据可能为 0,所以还是等 pull message 完成再返回消息
            await self._pullMessage(); // 直接 pull
        } else {
            // 如果数据总不是不是 0 ，那就异步 pull message。先返回消息。保证消息的延迟。
            setTimeout(function () {
                self._pullMessage();
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
        if (!size || size < 1) {
            size = 1;
        }

        const mqCount = await this.redis.messageCount();

        if (mqCount < this.options.minRedisMessageCount) {
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
            if (typeof messageId === 'object') {
                _messageId = messageId.messageId || messageId.id;
                _success = messageId.success;
            }
            _messageId = _messageId || messageId;
            _success = _success || success;

            const time = await this.redis.getTime(_messageId);

            if (!time) {
                // 已超时被处理
                const existsStatus = await this.redis.checkTimeExists(_messageId);
                if (existsStatus) {
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
                await this._handleFailedMessage(_messageId);
            }
        }
    }
    /**
     * 1. 检查消息是否有异常
     * 2. 检查消息消费是否超时
     */
    async checkExpireMessage() {
        // 关门打狗
        const status = await this.redis.setCheckLock();
        if (!status) return;

        // 已有的消息的数量
        const mqCount = await this.redis.messageCount();

        // mq 里所有的 messageId s
        const mqMessages = await this.redis.getMessageList(0, mqCount + this.options.eachMessageCount);

        // hash map
        const hashMap = await this.redis.getTimeMap();

        const missingList = [];
        const timeoutList = [];

        const keys = Object.keys(hashMap);

        for (const key of keys) {
            await sleep(0);
            const index = mqMessages.indexOf(key);

            
            if (index < 0 && !hashMap[key]) {
                // 数据缺失

                // queue 不存在， 但是 hashMap 上却被初始化 null
                missingList.push(key);
            } else if (index < 0 && hashMap[key] && (now() - hashMap[key]) > this.options.maxAckTimeoutSecords) {
                // 数据 ack 超时
                // queue 不存在，且超时
                timeoutList.push(key);
            }
        }

        await sleep(1000);

        for (const messageId of missingList) {
            // 这边检查是 HASH 表是否存在
            // 虽然唯一的可能性是，hashMap 为 null，queue 丢失，那也不排除是因为延迟问题，所以重新检查下 HASHMAP 是否正常
            const existsStatus = await this.redis.checkTimeExists(messageId);
            if (!existsStatus) { // 数据延迟问题
                continue;
            }

            const time = await this.redis.getTime(messageId);

            if (time === null) {
                // 数据延迟问题，time 已经被删除
                // 不需要做操作 
                continue;
            } else if (time) {
                // time 存在并且不为空
                // 因为考虑到拉取延迟，数据可能刚好在 queue 中被取了数据，但是还没设置 time
                // 但是实际已经设置 time 了
                // 如果已经超时，就会在下个周期对这个数据进行修复
                continue;
            }
            // 需要处理的场景是 time 初始化成空字符串的场景
            // 此时 queue 不存在该 messageId，但 queue 依然为空
            await this.redis.rpushMessage(messageId);
        }

        for (const messageId of timeoutList) {
            const time = await this.redis.getTime(messageId);
            if (!time) { // 数据延迟 -> 非消费中状态
                continue;
            }
            await this._handleFailedMessage(messageId);
        }
        // 清理锁
        await this.redis.cleanCheckLock();
        return {
            timeoutList: timeoutList,
            missingList: missingList
        };
    }

    async __messageUnconsumed() {
        const length = await this.redis.messageCount();
        let items = [];
        if (length > 0) {
            items = await this.redis.getMessageList(0, length);
        }
        return {
            itemsLength: length,
            items: items
        };
    }

    async __messageConsuming() {
        const map = await this.redis.getTimeMap();
        return map;
    }
}

module.exports = RedisMessage;
