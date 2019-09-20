"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
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
    }
    packMessage(data, msgType) {
        if (typeof data === 'string') {
            try {
                data = JSON.parse(data);
            }
            catch (err) {
            }
        }
        if (typeof data !== 'string' && data.toJSON) {
            data = data.toJSON();
        }
        return JSON.stringify({ data, msgType });
    }
    unpackMessage(jsonStr) {
        let data;
        try {
            data = JSON.parse(jsonStr);
        }
        catch (ex) {
            data = {
                msgType: 'unknown',
                data: jsonStr
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
            const value = yield this.redis.incr(this.LOCK_PULL_KEY);
            if (value === 1) {
                yield this.expire(this.LOCK_PULL_KEY, this.lockExpireTime);
                return true;
            }
            return false;
        });
    }
    /**
     * 清理 pull 的锁
     */
    cleanPullLock() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.del(this.LOCK_PULL_KEY);
        });
    }
    setCheckLock() {
        return __awaiter(this, void 0, void 0, function* () {
            const value = yield this.redis.incr(this.LOCK_CHECK_KEY);
            if (value < 5) {
                yield this.expire(this.LOCK_CHECK_KEY, this.lockExpireTime);
            }
            if (value > 100) {
                yield this.redis.del(this.LOCK_CHECK_KEY);
            }
            if (value === 1) {
                return true;
            }
            return false;
        });
    }
    cleanCheckLock() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.del(this.LOCK_CHECK_KEY);
        });
    }
    lpopMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.lpop(this.MQ_NAME);
        });
    }
    lpushMessage(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.lpush(this.MQ_NAME, messageId);
        });
    }
    rpopMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.rpop(this.MQ_NAME);
        });
    }
    rpushMessage(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.rpush(this.MQ_NAME, messageId);
        });
    }
    getMessageList(offset = 0, size = 10) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.lrange(this.MQ_NAME, offset, size);
        });
    }
    setTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hset(this.MQ_HASH_NAME, messageId, utils_1.now());
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
    getMessageId(id) {
        return `${this.topic}-${id}`;
    }
    getDetail(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const key = `${this.keyHeader}-${messageId}`;
            const data = (yield this.redis.get(key)) || '{}';
            return this.unpackMessage(data);
        });
    }
    setDetail(messageId, data, msgType) {
        return __awaiter(this, void 0, void 0, function* () {
            const str = this.packMessage(data, msgType);
            const key = `${this.keyHeader}-${messageId}`;
            return yield this.redis.set(key, str);
        });
    }
    delDetail(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const key = `${this.keyHeader}-${messageId}`;
            return yield this.redis.del(key);
        });
    }
    incrFailedTimes(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hincrby(this.MQ_HASH_RETRY_TIMES, messageId, 1);
        });
    }
    delFailedTimes(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.hdel(this.MQ_HASH_RETRY_TIMES, messageId);
        });
    }
    multi(options) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.redis.multi(options).exec();
        });
    }
    cleanFailedMsg(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const results = yield this.redis.multi([
                ['get', `${this.keyHeader}-${messageId}`],
                ['hdel', this.MQ_HASH_RETRY_TIMES, messageId],
                ['hdel', this.MQ_HASH_NAME, messageId],
                ['del', `${this.keyHeader}-${messageId}`],
            ]).exec();
            if (Array.isArray(results)) {
                const detail = this.unpackMessage(results[0]);
                return detail;
            }
            else {
                throw new Error(JSON.stringify(results || ''));
            }
        });
    }
    cleanMuliMsg(messageIds) {
        return __awaiter(this, void 0, void 0, function* () {
            const cmds = [];
            for (const messageId of messageIds) {
                cmds.push(['hdel', this.MQ_HASH_RETRY_TIMES, messageId]); // 删除失败次数 // await this.redis.delFailedTimes(messageId);
                cmds.push(['hdel', this.MQ_HASH_NAME, messageId]); // 清理时间 key // await this.redis.cleanTime(messageId);
                cmds.push(['del', `${this.keyHeader}-${messageId}`]); // 删除 message 详情 // await this.redis.delDetail(messageId);)
            }
            yield this.redis.multi(cmds).exec();
        });
    }
    cleanMsg(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.redis.multi([
                ['hdel', this.MQ_HASH_RETRY_TIMES, messageId],
                ['hdel', this.MQ_HASH_NAME, messageId],
                ['del', `${this.keyHeader}-${messageId}`],
            ]).exec();
        });
    }
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
    fetchMessageAndSetTime(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            const cmds = [
                ['hset', this.MQ_HASH_NAME, messageId, utils_1.now().toString()],
                ['get', `${this.keyHeader}-${messageId}`]
            ];
            const result = yield this.redis.multi(cmds).exec();
            const detail = result[1][1];
            return this.unpackMessage(detail);
        });
    }
    /**
     * 获取多个数据
     * @param size number 需要获取的消息数量
     */
    fetchMultiMessage(size) {
        return __awaiter(this, void 0, void 0, function* () {
            let cmds = [];
            while (size--) {
                cmds.push([
                    'lpop', this.MQ_NAME
                ]);
            }
            // 因为必须先获取 messageId 之后再获取消息体，如果数据丢失，就需要等数据修复才能恢复数据了。
            const results = yield this.redis.multi(cmds).exec();
            const realResults = results.map(r => r[1]).filter(r => !!r);
            if (!realResults.length)
                return [];
            cmds = [];
            const timeNow = utils_1.now().toString();
            // 组装设置 time 和获取 detail 的 cmds
            for (const messageId of realResults) {
                if (typeof messageId !== 'string') {
                    continue;
                }
                cmds.push([
                    'hset', this.MQ_HASH_NAME, messageId, timeNow
                ]);
                cmds.push([
                    'get', `${this.keyHeader}-${messageId}`
                ]);
            }
            // 事务请求
            const dataResults = yield this.redis.multi(cmds).exec();
            const list = [];
            for (let i = 1; i < dataResults.length; i += 2) {
                const data = this.unpackMessage(dataResults[i][1]);
                const messageId = realResults[(i - 1) / 2];
                if (data && messageId) { // 目前存在 BUG 导致可能 message data 为空
                    data.messageId = messageId;
                    list.push(data);
                }
            }
            return list;
        });
    }
    initTimeAndRpush(messageId, pushLeft = false) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.redis.hset(this.MQ_HASH_NAME, messageId, '');
            if (pushLeft) {
                yield this.redis.lpush(this.MQ_NAME, messageId);
            }
            else {
                yield this.redis.rpush(this.MQ_NAME, messageId);
            }
        });
    }
    orderConsumeLock() {
        return __awaiter(this, void 0, void 0, function* () {
            let status = false;
            const num = yield this.redis.incr(this.LOCK_ORDER_KEY);
            if (num === 1) {
                status = true;
            }
            yield this.expire(this.LOCK_ORDER_KEY, this.lockExpireTime * 5); // 顺序消费，如果存在错误使请求中断，需要完全修复之后才允许重新获取，所以时间设置长一点
            return status;
        });
    }
    orderConsumeUnlock() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.redis.del(this.LOCK_ORDER_KEY);
        });
    }
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
    getSelectedIds() {
        return __awaiter(this, void 0, void 0, function* () {
            const idString = (yield this.redis.get(this.ORDER_CONSUME_SELECTED)) || '';
            const selectIds = idString.trim().split('|').filter(id => !!id);
            return selectIds;
        });
    }
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
