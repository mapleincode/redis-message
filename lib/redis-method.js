'use strict';

const utils = require('./utils');

const {
    now
} = utils;

class RedisMethod {
    constructor(redis, options) {
        this.redis = redis;
        const {
            keyHeader = `msg_`,
            topic,
            lockExpireTime = 60
        } = options;
        this.options = options;

        this.topic = topic;
        this.keyHeader = keyHeader;
        this.lockExpireTime = lockExpireTime;

        this.MQ_NAME = `${keyHeader}-${topic}-redis-mq`;
        this.MQ_HASH_NAME = `${keyHeader}-${topic}-redis-hash`;
        this.MQ_HASH_RETRY_TIMES = `${keyHeader}-${topic}-redis-retry-hash`;
        this.LOCK_PULL_KEY = `${keyHeader}-${topic}-pull-lock`;
        this.LOCK_CHECK_KEY = `${keyHeader}-${topic}-check-lock`;
    }
    /**
     * 序列化数据
     * @param {object} data 数据对象
     * @return {string} str 序列化后的数据
     */
    _packMessage(data, msgType) {
        if(typeof data === 'string') {
            try {
                data = JSON.parse(data);
            } catch(ex) {
                // data 为 string
            }
        } else if(typeof data !== 'object') {
            data = {};
        }
        if(data && data.toJSON && typeof data.toJSON === 'function') {
            data = data.toJSON();
        }
        return JSON.stringify({
            data: data,
            msgType: msgType
        });
    }
    /**
     * 反序列化数据
     * @param {string} jsonStr 序列化数据
     * @return {object} data data 对象
     */
    _unpackMessage(jsonStr) {
        if(jsonStr === null || jsonStr === undefined) {
            return null;
        }
        let data;
        try {
            data = JSON.parse(jsonStr);
        } catch(ex) {
            data = jsonStr; 
        }
        return data;
    }
    /**
     * 设置 redis key 过期时间戳
     * @param {string} key key
     * @param {integer} timestamp 时间戳
     */
    async _expire(key, timestamp) {
        return await this.redis.expire(key, timestamp);
    }
    /**
     * 返回消息队列的总数
     * @return {integer} count
     */
    async messageCount() {
        return await this.redis.llen(this.MQ_NAME);
    }
    
    /**
     * 设置 pull 的锁
     * @return {boolean} status 是否 lock 成功
     */
    async setPullLock() {
        const value = await this.redis.incr(this.LOCK_PULL_KEY);
        if(value === 1) {
            await this._expire(this.LOCK_PULL_KEY, this.lockExpireTime);
            return true;
        }
        return false;
    }
    /**
     * 清理 pull 的锁
     */
    async cleanPullLock() {
        return await this.redis.del(this.LOCK_PULL_KEY);
    }

    async setCheckLock() {
        const value = await this.redis.incr(this.LOCK_CHECK_KEY);
        if(value === 1) {
            await this._expire(this.LOCK_CHECK_KEY, this.lockExpireTime);
            return true;
        }
        return false;
    }

    async cleanCheckLock() {
        return await this.redis.del(this.LOCK_CHECK_KEY);
    }
    
    async lpopMessage() {
        return await this.redis.lpop(this.MQ_NAME);
    }
    
    async lpushMessage(messageId) {
        return await this.redis.lpush(this.MQ_NAME, messageId);
    }
    
    async rpopMessage() {
        return await this.redis.rpop(this.MQ_NAME);
    }
    
    async rpushMessage(messageId) {
        return await this.redis.rpush(this.MQ_NAME, messageId);
    }

    async getMessageList(offset = 0, size = 10) {
        return await this.redis.lrange(this.MQ_NAME, offset, size);
    }

    async setTime(messageId) {
        return await this.redis.hset(this.MQ_HASH_NAME, messageId, now());
    }

    async getTimeMap() {
        return await this.redis.hgetall(this.MQ_HASH_NAME);
    }

    async checkTimeExists(messageId) {
        return await this.redis.hexists(this.MQ_HASH_NAME, messageId);
    }

    async getTime(messageId) {
        return await this.redis.hget(this.MQ_HASH_NAME, messageId);
    }

    async cleanTime(messageId) {
        return await this.redis.hdel(this.MQ_HASH_NAME, messageId);
    }

    async initTime(messageId) {
        return await this.redis.hset(this.MQ_HASH_NAME, messageId, null);
    }

    getMessageId(id) {
        return `${this.topic}-${id}`;
    }

    async getDetail(messageId) {
        const key = `${this.keyHeader}-${messageId}`;
        const data = await this.redis.get(key);
        return this._unpackMessage(data);
    }

    async setDetail(messageId, data, msgType) {
        const str = this._packMessage(data, msgType);
        const key = `${this.keyHeader}-${messageId}`;
        return await this.redis.set(key, str);
        
    }

    async delDetail(messageId) {
        const key = `${this.keyHeader}-${messageId}`;
        return await this.redis.del(key);
    }

    async incrFailedTimes(messageId) {
        return await this.redis.hincrby(this.MQ_HASH_RETRY_TIMES, messageId, 1);
    }

    async delFailedTimes(messageId) {
        return await this.redis.hdel(this.MQ_HASH_RETRY_TIMES, messageId);
    }

    async multi(options) {
        return await this.redis.multi(options).exec();
    }
}

module.exports = RedisMethod;
