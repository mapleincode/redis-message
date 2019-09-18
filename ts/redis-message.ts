/**
 * RedisMessage 类
 * create by maple 2018-11-16 11:06:59
 */
'use strict';

import _ from 'lodash';
import RedisMethod, { RedisMethodOptions, messageData } from './redis-method';
import { now, sleep } from './utils';
import defaultLogger from './logger';
import Debug from 'debug';
import { Redis } from 'ioredis';

const debug = Debug('redis-message');

type subFuncOptions = {
    topic: string; // topic
    messageType: string; // messageType
};

type originalMessage = {
    id: number; // message 在数据库里的 id ，自增数字
    data: messageData; // 实际数据
    msgType: string; // msgType 标识数据所属类别
};

interface subFunc<func> {
    (options: subFuncOptions): func;
}


type fetchMessageFunc = {
    (): Promise<originalMessage[]>
}

type afterFetchMessageFunc = {
    (data: { offset: number|undefined, noChange: boolean }): Promise<void>;
}

type handleFailedMessageFunc = {
    (messageId: string, data: messageData|string): Promise<void>;
}

type loggerFunc = {
    warn: Function,
    error: Function,
    info: Function
};

interface baseMessageOptions {
    topic: string; // topic 用于作为标识，在 redis key 进行区分
    messageType: string; // messageType
    redis: Redis
}

interface extraMessageOptions {
    keyHeader: string; // redis key header 默认 msg_
    lockExpireTime: number; // redis lock 默认时间
    maxAckTimeout: number; // 消费超时时间 默认 60s. 
    eachMessageCount: number; // 每次 Message 获取数量
    minRedisMessageCount: number; // Redis 最少的 count 数量
    maxRetryTimes: number; // 消息消费失败重新消费次数
    recordFailedMessage: boolean; // 记录失败的数据
    orderConsumption: boolean; // 是否支持顺序消费
    autoAck: boolean; // 是否支持 auto-ack 模式
    logger: loggerFunc; // logger
}

export interface redisMessageOptions extends baseMessageOptions, Partial<extraMessageOptions> {
    fetchMessage?: subFunc<fetchMessageFunc>;
    afterFetchMessage?: subFunc<afterFetchMessageFunc>;
    dealFailedMessage?: subFunc<handleFailedMessageFunc>;
    handleFailedMessage?: subFunc<handleFailedMessageFunc>;
};

interface redisMessagePrivateOptions extends baseMessageOptions, extraMessageOptions {
    fetchMessage: fetchMessageFunc;
    afterFetchMessage: afterFetchMessageFunc;
    handleFailedMessage: handleFailedMessageFunc;
    maxAckTimeoutSecords: number;
};

interface ackItem {
    messageId?: string;
    id?: string;
    success?: boolean;
}



export default class RedisMessage {
    private options: redisMessagePrivateOptions;
    private fetchMessage: fetchMessageFunc;
    private afterFetchMessage: afterFetchMessageFunc;
    private handleFailedMessage: handleFailedMessageFunc;
    private redis: RedisMethod;
    private logger: loggerFunc;

    constructor(options: redisMessageOptions) {
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
                const func: afterFetchMessageFunc = async function (data) {
                    const {
                        offset
                    } = data;
                    debug(`message has been offset to :${offset}`);
                };
                return func;
            },
            dealFailedMessage = function (/* { topic, messageType } */) {
                return async function (messageId: string, detail: string|messageData) {
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

        const subFuncOptions = {
            topic: topic,
            messageType: messageType
        };

        this.options = {
            topic, // topic 用于作为标识，在 redis key 进行区分
            messageType, // messageType 数据源
            redis, // ioredis 实例
            logger,

            // ============== 以下是推荐默认参数 ================
            keyHeader, // redis key header 默认 msg_
            lockExpireTime, // redis lock 默认时间
            maxAckTimeout: maxAckTimeout, // 消费超时时间 默认 60s. 
            maxAckTimeoutSecords: parseInt((maxAckTimeout/1000).toString()), // 转换成单位秒(s)
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
            fetchMessage: fetchMessage(subFuncOptions),
            afterFetchMessage: afterFetchMessage(subFuncOptions),
            handleFailedMessage: handleFailedMessage ? handleFailedMessage(subFuncOptions) : dealFailedMessage(subFuncOptions)
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
        this.redis = new RedisMethod(this.options.redis, this.options);

        // logger
        this.logger = this.options.logger;
    }

    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    async _handleFailedMessage(messageId: string) {
        debug(`处理失败的 Message ${messageId}`);
        // 获取失败次数
        const failedTimes = await this.redis.incrFailedTimes(messageId);

        debug(`获取失败次数为 ${failedTimes} 当前 max ${this.options.maxRetryTimes}`);

        if (failedTimes > this.options.maxRetryTimes) {
            let detail;
            try {
                detail = await this.redis.cleanFailedMsg(messageId);
            } catch (err) {
                this.logger.error(err);
            }

            if (this.options.recordFailedMessage) {
                // 最后调用 deal 函数
                await this.handleFailedMessage(messageId, detail || '');
                return;
            }
        }

        // 初始化调用时间
        // 重新 push 到队列中
        // 如果是顺序消费，就是从左边进行 push

        debug(`该条消息重新进入消费 & 是顺序消费: ${this.options.orderConsumption}`);

        await this.redis.initTimeAndRpush(messageId, this.options.orderConsumption);

        debug('重新初始化消费成功');
    }

    async _pullMessage() {
        debug('pull message by fetch message func');
        const list = await this.fetchMessage();

        debug('pull message success');

        let offset: number|undefined;
        let successItems = 0;

        try {
            for (const msg of list) {
                const {
                    id, // 唯一 id
                    data, // data
                    msgType // 消息类型
                } = msg;

                debug(`push message id: ${id}`);
                await this.redis.pushMessage(id, data, msgType);
                debug(`push message id: ${id} success`);

                // 更新 offset
                offset = id + 1;
                successItems ++;
            }
        } catch (err) {
            this.logger.error(err);
        }

        debug('after fetch message');
        await this.afterFetchMessage({
            offset: offset,
            noChange: !offset
        });

        debug('clean pull lock');

        await this.redis.cleanPullLock();
        return successItems;
    }
    /**
     * 获取 messgae 数据
     * @return {boolean} 是否成功 pull
     */
    async pullMessage(existedMQCount: number) {
        debug('set pull lock');
        // 先 lock
        const lockStatus = await this.redis.setPullLock();

        debug('set lock close end have value: ' + lockStatus);
        if (!lockStatus) {
            debug('获取 lock 失败，返回 false');
            return false;
        }
        // fetch message list
        const self = this;
        // Async or sync
        if (existedMQCount === 0) {
            debug('已有数据为空，同步返回数据，直接 pull message');

            // 如果不 pull 数据可能为 0,所以还是等 pull message 完成再返回消息
            const successItems = await self._pullMessage(); // 直接 pull
            return { successItems: successItems };
        } else {
            debug('旧数据存在，异步 pull 数据，直接返回 true');
            // 如果数据总不是不是 0 ，那就异步 pull message。先返回消息。保证消息的延迟。
            setTimeout(function () {
                debug('执行 1000ms 之后的异步任务');
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
    async getMessages(size: number) {
        if (!size || size < 1) {
            size = 1;
        }

        debug(`get messages: 需要获取 ${size} 条数据`);

        if (this.options.orderConsumption) {
            const status = await this.orderConsumeLock();
            if (!status) {
                debug(`顺序消费:set lock fail:status: ${status}`);
                return [];
            } else {
                debug('顺序消费:set lock success');
            }
        }


        const mqCount = await this.redis.messageCount();

        let fetchStatus;
        // 如果实际拥有的 count 数小于实际的数量
        if (mqCount < this.options.minRedisMessageCount) {
            fetchStatus = await this.pullMessage(mqCount); // async
            if (!fetchStatus && mqCount === 0) {
                // 说明有个进程可能已经在获取了
                // 这时候应该等待
                // 如果 mqCount 不为 0 就不需要等待了
                await sleep(500);
            }
        }

        // 如果返回的是 object 说明是同步请求
        // 可以获取最后更新的值，根据更新的数量来决定最后的请求
        // 可以变相减少对 redis 的请求压力
        if (typeof fetchStatus === 'object') {
            const { successItems } = fetchStatus;

            // 更新数量为 0 直接返回空数组
            if (successItems === 0) {
                return [];
            }

            // 更新数量 < 实际请求的数量
            if (successItems > 0 && successItems < size) {
                size = successItems;
            }
        }

        let list;

        try {
            // 从 redis 获取
            list = await this.redis.fetchMultiMessage(size);

            if (this.options.orderConsumption) {
                // 顺序请求
                // 需要记录本次获取的 ids
                try {
                    await this.setOrderConsumeIds(list);
                } catch(err) {
                    this.logger.error(err);
                    await this.setOrderConsumeIds(list);
                }
                
            }
        } catch(err) {
            if (this.options.orderConsumption) {
                await this.checkExpireMessage(false); // 清理数据
                await this.cleanOrderConsume(); // 解锁
            } else {
                throw err;
            }
        }

        return list;
    }

    private async ackNormalMessages(messageIds: string[] | ackItem[] | string, allSuccess: boolean = true) {
        if (typeof messageIds === 'string') {
            messageIds = [ messageIds ];
        } 

        debug(`获得消息的数量为: ${messageIds && messageIds.length}`);

        const processdItems: { messageId: string, success: boolean }[] = [];

        for (const item of messageIds) {
            let messageId: string|undefined;
            let success: boolean|undefined;
            if (typeof item === 'object') {
                messageId = item.messageId || item.id;
                success = item.success || allSuccess;
            }

            if (!messageId && typeof item === 'string') {
                messageId = item;
                success = allSuccess;
            }

            if (messageId && success !== undefined) {
                processdItems.push({
                    success: success,
                    messageId: messageId
                });
            }
        }

        for(const item of processdItems) {
            let { messageId, success } = item;

            const time = await this.redis.getTime(messageId);
            if (!time) {
                // 已超时被处理
                const existsStatus = await this.redis.checkTimeExists(messageId);
                if (existsStatus) {
                    debug(`messageId: ${messageId} 超时被置为空字符串`);
                    // 已经被初始化了
                    continue;
                }
                debug(`messageId: ${messageId} 没有 time`);
                // 如果 existsStatus 说明已经被消费掉了
                // 可以不做处理
                // 这里选择的是再删一次
                success = true;
            }

            if (success) {
                debug(`清理 messageId: ${messageId}`);
                await this.redis.cleanMsg(messageId);
                debug('清理 message 成功');
            } else {
                debug(`messageId: ${messageId} 没有 time 设置失败`);
                await this._handleFailedMessage(messageId);
            }
        }
    }

    /**
     * 成功消费消息或者失败消费消息
     * @param {string|array} messageIds 消息 id 或消息 id 数组
     * @param {boolean} success boolean 是否是成功
     */
    async ackMessages(messageIds: string[] | ackItem[] | string | boolean, allSuccess: boolean = true) {
        if (typeof messageIds === 'boolean') {
            allSuccess = messageIds;
            return await this.ackOrderMessages(allSuccess);
        }

        return await this.ackNormalMessages(messageIds, allSuccess);
    }

    // ======================= 检查脚本 ================================
    
    async fixNormalMessage(missingList: string[], timeoutList: string[]) {
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

    async fixOrderMessage(missingList: string[], timeoutList: string[]) {
        const ids = await this.redis.getSelectedIds();

        if (ids && ids.length) {
            const errorMissings = missingList.filter(id => ids.indexOf(id) < 0); // 非本次请求，之前遗留的脏数据
            const errorTimeouts = timeoutList.filter(id => ids.indexOf(id) < 0); // 非本次请求，之前遗留的脏数据

            for (const id of errorMissings) {
                await this.redis.cleanMsg(id); // queue 处理 id 比较麻烦，直接删除 detail 之后请求直接会自动剔除
            }

            for (const id of errorTimeouts) {
                await this.redis.cleanMsg(id); // 清理数据
            }

            missingList = missingList.filter(id => ids.indexOf(id) > -1); // 过滤 id
            timeoutList = timeoutList.filter(id => ids.indexOf(id) > -1); // 过滤 id
        }

        for (const messageId of missingList.reverse()) {
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
            await this.redis.lpushMessage(messageId);
        }

        for (const messageId of timeoutList.reverse()) {
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
    /**
     * 1. 检查消息是否有异常
     * 2. 检查消息消费是否超时
     */
    async checkExpireMessage(sleepStatus: boolean = true) {
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
        if (sleepStatus) {
            await sleep(1000);
        }

        if (this.options.orderConsumption) {
            return await this.fixOrderMessage(missingList, timeoutList);
        } else {
            return await this.fixNormalMessage(missingList, timeoutList);
        }
    }

    // =============== 顺序消费接口 =======================

    /**
     * 顺序消费
     */
    private async orderConsumeLock() {
        const status = await this.redis.orderConsumeLock();
        return status;
    }

    /**
     * 对获取的数据的 id 进行保存
     * @param ids 消费的 ids
     */
    private async setOrderConsumeIds(items: { messageId: string }[]) {
        debug('设置顺序消费 ids');
        const ids = items.map(item => item.messageId);
        await this.redis.initSelectedIds(ids);
    }

    private async cleanOrderConsume() {
        debug('清理顺序消费相关的 lock');
        await this.redis.cleanOrderConsumer();
    }

    private async ackOrderMessages(successAll: boolean) {
        debug('ack order message');

        const ids = await this.redis.getSelectedIds();

        // ack 成功的 messages
        if(successAll) {
            try {
                // 事务，要死一起死，要是没处理成功统一 gg
                await this.redis.cleanMuliMsg(ids);
            } catch(err) {
                await this.checkExpireMessage(); // 失败就调用修复服务
                this.logger.error('ORDER_CONSUME_ACK_FAILED', { err: err, messageIds: ids, successAll });
            }
        } else {
            // ack 失败的 message

            for(const messageId of ids.reverse()) {
                const time = await this.redis.getTime(messageId);
    
                if(time === '') {
                    // 已经被重新初始化
                    continue;
                } else if (time === null) {
                    // 已被删除
                    continue;
                }
    
                try {
                    // 作为失败的 messageId 重新加入队列
                    await this._handleFailedMessage(messageId);
                } catch(err) {
                    await this.checkExpireMessage(); // 失败就调用修复服务
                    this.logger.error('ORDER_CONSUME_ACK_FAILED', { err: err, messageId: messageId, successAll });
                }
            }
        }

        // 清理 lock
        await this.cleanOrderConsume();
    }

    // ========== 管理接口 =============

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
