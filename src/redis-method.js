"use strict";
/**
 * RedisMethod 类
 * 封装与 Redis 交互的底层方法，包括消息队列的增删改查、分布式锁、事务操作等
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const wm_redis_locks_1 = __importDefault(require("wm-redis-locks"));
const utils_1 = require("./utils");
class RedisMethod {
    constructor(redis, options) {
        this.redis = redis;
        this.options = options;
        const { keyHeader, topic, lockExpireTime } = options;
        this.topic = topic;
        this.keyHeader = keyHeader || 'msg_';
        this.lockExpireTime = lockExpireTime || 60;
        this.MQ_NAME = `${this.keyHeader}-${topic}-redis-mq`;
        this.MQ_HASH_NAME = `${this.keyHeader}-${topic}-redis-hash`;
        this.MQ_HASH_RETRY_TIMES = `${this.keyHeader}-${topic}-redis-retry-hash`;
        this.LOCK_PULL_KEY = `${this.keyHeader}-${topic}-pull-lock`;
        this.LOCK_CHECK_KEY = `${this.keyHeader}-${topic}-check-lock`;
        this.LOCK_ORDER_KEY = `${this.keyHeader}-${topic}-order-lock`;
        this.ORDER_CONSUME_SELECTED = `${this.keyHeader}-${topic}-order-consume-selected`;
        this.pullLock = new wm_redis_locks_1.default(this.redis, this.LOCK_PULL_KEY, this.lockExpireTime);
        this.checkLock = new wm_redis_locks_1.default(this.redis, this.LOCK_CHECK_KEY, this.lockExpireTime);
        this.orderLock = new wm_redis_locks_1.default(this.redis, this.LOCK_ORDER_KEY, this.lockExpireTime);
    }
    getPullLock() {
        return this.pullLock;
    }
    getCheckLock() {
        return this.checkLock;
    }
    getOrderLock() {
        return this.orderLock;
    }
    /**
     * 序列化消息数据为 JSON 字符串
     * 1. 如果 data 是 JSON 字符串，尝试解析为对象
     * 2. 如果 data 有 toJSON 方法，调用 toJSON 转换
     * 3. 最终序列化为 { data, msgType } 格式的 JSON
     * @param data 消息数据
     * @param msgType 消息类型标识
     * @returns JSON 字符串
     */
    packMessage(data, msgType) {
        if (typeof data === 'string') {
            try {
                data = JSON.parse(data);
            }
            catch (err) {
                // 非 JSON 字符串，保持原样
            }
        }
        if (typeof data !== 'string' && data.toJSON) {
            data = data.toJSON();
        }
        return JSON.stringify({ data, msgType });
    }
    /**
     * 反序列化 JSON 字符串为消息数据对象
     * 如果解析失败，返回 { msgType: 'unknown', data: jsonStr } 作为兜底
     * @returns 解析后的消息数据对象，输入为 null/undefined 时返回 null
     * @param json
     */
    unpackMessage(json) {
        if (json == null) {
            return null;
        }
        let data;
        try {
            const jsonStr = json.toString();
            data = JSON.parse(jsonStr);
        }
        catch (ex) {
            data = {
                msgType: 'unknown',
                data: json
            };
        }
        return data;
    }
    /**
     * 设置 redis key 过期时间戳
     * @param {string} key key
     * @param {integer} timestamp 时间戳
     */
    expire(key, timestamp) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.expire(key, timestamp);
        });
    }
    /**
     * 返回消息队列的总数
     * @return {integer} count
     */
    messageCount() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.llen(this.MQ_NAME);
        });
    }
    /**
     * 设置 pull 的锁
     * @return {boolean} status 是否 lock 成功
     */
    setPullLock() {
        return __awaiter(this, void 0, void 0, function* () {
            // const value = await this.redis.incr(this.LOCK_PULL_KEY);
            // if(value === 1) {
            //     await this.expire(this.LOCK_PULL_KEY, this.lockExpireTime);
            //     return true;
            // }
            // return false;
            return yield this.pullLock.lock();
        });
    }
    /**
     * 清理 pull 的锁
     */
    cleanPullLock() {
        return __awaiter(this, void 0, void 0, function* () {
            // return await this.redis.del(this.LOCK_PULL_KEY);
            return yield this.pullLock.cleanLock();
        });
    }
    setCheckLock() {
        return __awaiter(this, void 0, void 0, function* () {
            // const value = await this.redis.incr(this.LOCK_CHECK_KEY);
            // if (value < 5) {
            //     await this.expire(this.LOCK_CHECK_KEY, this.lockExpireTime);
            // }
            // if (value > 100) {
            //     await this.redis.del(this.LOCK_CHECK_KEY);
            // }
            // if(value === 1) {
            //     return true;
            // }
            // return false;
            return this.checkLock.lock();
        });
    }
    cleanCheckLock() {
        return __awaiter(this, void 0, void 0, function* () {
            // return await this.redis.del(this.LOCK_CHECK_KEY);
            return yield this.checkLock.cleanLock();
        });
    }
    /**
     * 从队列左侧弹出一个 messageId
     * @returns messageId 或 null（队列为空时）
     */
    lpopMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.lpop(this.MQ_NAME);
        });
    }
    /**
     * 从队列左侧推入一个 messageId（用于顺序消费重试）
     * @param messageId 消息 ID
     */
    lpushMessage(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.lpush(this.MQ_NAME, messageId);
        });
    }
    /**
     * 从队列右侧弹出一个 messageId
     * @returns messageId 或 null（队列为空时）
     */
    rpopMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.rpop(this.MQ_NAME);
        });
    }
    /**
     * 从队列右侧推入一个 messageId（普通消息入队）
     * @param messageId 消息 ID
     */
    rpushMessage(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.rpush(this.MQ_NAME, messageId);
        });
    }
    /**
     * 获取队列中指定范围的 messageId 列表
     * @param offset 起始偏移量，默认 0
     * @param size 结束偏移量，默认 10
     * @returns messageId 数组
     */
    getMessageList() {
        return __awaiter(this, arguments, void 0, function* (offset = 0, size = 10) {
            return yield this.redis.lrange(this.MQ_NAME, offset, size);
        });
    }
    setTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hset(this.MQ_HASH_NAME, messageId, (0, utils_1.now)());
        });
    }
    getTimeMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hgetall(this.MQ_HASH_NAME);
        });
    }
    checkTimeExists(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hexists(this.MQ_HASH_NAME, messageId);
        });
    }
    getTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hget(this.MQ_HASH_NAME, messageId);
        });
    }
    cleanTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hdel(this.MQ_HASH_NAME, messageId);
        });
    }
    initTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hset(this.MQ_HASH_NAME, messageId, '');
        });
    }
    /**
     * 根据消息 ID 生成唯一标识
     * @param id 消息自增 ID
     * @returns 格式为 '{topic}-{id}' 的消息 ID
     */
    getMessageId(id) {
        return `${this.topic}-${id}`;
    }
    /**
     * 获取消息详情
     * @param messageId 消息 ID
     * @returns 解析后的消息数据
     */
    getDetail(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const key = `${this.keyHeader}-${messageId}`;
            const data = (yield this.redis.get(key)) || '{}';
            return this.unpackMessage(data);
        });
    }
    /**
     * 设置消息详情（序列化为 JSON 存储）
     * @param messageId 消息 ID
     * @param data 消息数据
     * @param msgType 消息类型
     */
    setDetail(messageId, data, msgType) {
        return __awaiter(this, void 0, void 0, function* () {
            const str = this.packMessage(data, msgType);
            const key = `${this.keyHeader}-${messageId}`;
            return yield this.redis.set(key, str);
        });
    }
    /**
     * 删除消息详情
     * @param messageId 消息 ID
     */
    delDetail(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const key = `${this.keyHeader}-${messageId}`;
            return yield this.redis.del(key);
        });
    }
    /**
     * 递增消息的失败次数
     * @param messageId 消息 ID
     * @returns 递增后的失败次数
     */
    incrFailedTimes(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hincrby(this.MQ_HASH_RETRY_TIMES, messageId, 1);
        });
    }
    /**
     * 删除消息的失败次数记录
     * @param messageId 消息 ID
     */
    delFailedTimes(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hdel(this.MQ_HASH_RETRY_TIMES, messageId);
        });
    }
    /**
     * 执行 Redis 事务（multi/exec）
     * @param options 命令数组，每个命令为 [command, ...args] 格式
     * @returns 事务执行结果
     */
    multi(options) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.multi(options).exec();
        });
    }
    /**
     * 清理失败消息的所有相关数据（原子操作）
     * 包括：获取详情、删除失败次数、清理时间记录、删除消息详情
     * @param messageId 消息 ID
     * @returns 消息详情数据
     */
    cleanFailedMsg(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const results = yield this.redis.multi()
                .get(`${this.keyHeader}-${messageId}`) // 获取消息详情
                .hdel(this.MQ_HASH_RETRY_TIMES, messageId) // 删除失败次数
                .hdel(this.MQ_HASH_NAME, messageId) // 删除消息详情
                .del(`${this.keyHeader}-${messageId}`) // 删除消息详情
                .exec();
            if (results == null) {
                return null;
            }
            for (const result of results) {
                if (result[0] != null) {
                    throw result[0];
                }
            }
            return results.map(result => this.unpackMessage(result[1]));
        });
    }
    /**
     * 批量清理多个消息的所有相关数据（原子操作）
     * @param messageIds 消息 ID 数组
     */
    cleanMultiMsg(messageIds) {
        return __awaiter(this, void 0, void 0, function* () {
            const cmds = [];
            for (const messageId of messageIds) {
                cmds.push(['hdel', this.MQ_HASH_RETRY_TIMES, messageId]); // 删除失败次数
                cmds.push(['hdel', this.MQ_HASH_NAME, messageId]); // 清理时间记录
                cmds.push(['del', `${this.keyHeader}-${messageId}`]); // 删除消息详情
            }
            yield this.redis.multi(cmds).exec();
        });
    }
    /**
     * 清理单个消息的所有相关数据（原子操作）
     * @param messageId 消息 ID
     */
    cleanMsg(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.redis.multi([
                ['hdel', this.MQ_HASH_RETRY_TIMES, messageId], // 删除失败次数
                ['hdel', this.MQ_HASH_NAME, messageId], // 清理时间记录
                ['del', `${this.keyHeader}-${messageId}`], // 删除消息详情
            ]).exec();
        });
    }
    /**
     * 将消息推入队列并存储详情（原子操作）
     * 同时执行：存储消息详情、初始化时间记录、推入队列
     * @param id 消息自增 ID
     * @param data 消息数据
     * @param msgType 消息类型
     */
    pushMessage(id, data, msgType) {
        return __awaiter(this, void 0, void 0, function* () {
            const messageId = this.getMessageId(id);
            const str = this.packMessage(data, msgType);
            const key = `${this.keyHeader}-${messageId}`;
            yield this.redis.multi([
                ['set', key, str],
                ['hset', this.MQ_HASH_NAME, messageId, ''],
                ['rpush', this.MQ_NAME, messageId]
            ]).exec();
        });
    }
    /**
     * 弹出消息并设置消费时间戳（原子操作）
     * 用于单条消息获取时，同时记录消费开始时间
     * @param messageId 消息 ID
     * @returns 解析后的消息数据
     */
    fetchMessageAndSetTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            // const cmds = [
            //     [ 'hset', this.MQ_HASH_NAME, messageId, now().toString() ],
            //     [ 'get', `${this.keyHeader}-${messageId}` ]
            // ];
            const results = yield this.redis
                .multi()
                .hset(this.MQ_HASH_NAME, messageId, (0, utils_1.now)().toString())
                .get(`${this.keyHeader}-${messageId}`)
                .exec();
            if (results == null) {
                throw new Error('Message not found');
            }
            for (const result of results) {
                if (result[0] != null) {
                    throw result[0];
                }
            }
            const detail = results[1][1];
            return this.unpackMessage(detail);
        });
    }
    /**
     * 批量获取消息（原子操作）
     * 分两步执行：
     * 1. 从队列左侧依次弹出 size 条 messageId
     * 2. 批量设置消费时间戳并获取消息详情
     * @param size 需要获取的消息数量
     * @returns 消息数据数组
     */
    fetchMultiMessage(size) {
        return __awaiter(this, void 0, void 0, function* () {
            if (size <= 0) {
                return [];
            }
            const commander = this.redis.multi();
            while (size--) {
                commander.lpop(this.MQ_NAME);
            }
            // 因为必须先获取 messageId 之后再获取消息体，如果数据丢失，就需要等数据修复才能恢复数据了。
            const results = yield commander.exec();
            if (results == null) {
                throw new Error('Message not found');
            }
            // 过滤掉 null 结果（队列中可能已被其他进程消费的消息）
            const realResults = results.map(r => r[1]).filter(r => !!r);
            if (!realResults.length) {
                return [];
            }
            const timeNow = (0, utils_1.now)().toString();
            const newCommander = this.redis.multi();
            // 组装设置 time 和获取 detail 的 cmds
            for (const messageId of realResults) {
                if (typeof messageId !== 'string') {
                    continue;
                }
                newCommander
                    .hset(this.MQ_HASH_NAME, messageId, timeNow)
                    .get(`${this.keyHeader}-${messageId}`);
            }
            // 事务请求
            const dataResults = yield newCommander.exec();
            const list = [];
            if (dataResults == null) {
                throw new Error('Message not found');
            }
            for (let i = 1; i < dataResults.length; i += 2) {
                const data = this.unpackMessage(dataResults[i][1]);
                const messageId = dataResults[(i - 1) / 2][1];
                if (data && messageId) { // 目前存在 BUG 导致可能 message data 为空
                    data.messageId = messageId;
                    list.push(data);
                }
            }
            return list;
        });
    }
    /**
     * 重新初始化消息的消费时间并推入队列
     * 用于消费失败后重试的场景
     * @param messageId 消息 ID
     * @param pushLeft 是否从左侧推入（顺序消费为 true，普通消费为 false）
     */
    initTimeAndRpush(messageId_1) {
        return __awaiter(this, arguments, void 0, function* (messageId, pushLeft = false) {
            yield this.redis.hset(this.MQ_HASH_NAME, messageId, '');
            if (pushLeft) {
                yield this.redis.lpush(this.MQ_NAME, messageId);
            }
            else {
                yield this.redis.rpush(this.MQ_NAME, messageId);
            }
        });
    }
    /**
     * 获取顺序消费的分布式锁
     * @returns 是否获取锁成功
     */
    orderConsumeLock() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.orderLock.lock();
        });
    }
    /**
     * 释放顺序消费的分布式锁
     */
    orderConsumeUnlock() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.orderLock.cleanLock();
        });
    }
    /**
     * 保存顺序消费中选中的消息 ID 列表
     * @param ids 消息 ID 数组，以 '|' 分隔存储
     */
    initSelectedIds(ids) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!ids.length) {
                return;
            }
            yield this.redis.set(this.ORDER_CONSUME_SELECTED, ids.join('|'));
            yield this.expire(this.ORDER_CONSUME_SELECTED, this.lockExpireTime);
            return;
        });
    }
    /**
     * 获取顺序消费中选中的消息 ID 列表
     * @returns 消息 ID 数组
     */
    getSelectedIds() {
        return __awaiter(this, void 0, void 0, function* () {
            const idString = (yield this.redis.get(this.ORDER_CONSUME_SELECTED)) || '';
            const selectIds = idString.trim().split('|').filter(id => !!id);
            return selectIds;
        });
    }
    /**
     * 清理顺序消费相关的所有数据（选中 ID 和锁）
     */
    cleanOrderConsumer() {
        return __awaiter(this, void 0, void 0, function* () {
            const cmds = [
                ['del', this.ORDER_CONSUME_SELECTED],
                ['del', this.LOCK_ORDER_KEY]
            ];
            yield this.redis.multi(cmds).exec();
        });
    }
}
exports.default = RedisMethod;
