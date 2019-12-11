import { Redis } from 'ioredis';
import { now } from './utils';

export type RedisMethodOptions = {
    topic: string;
    keyHeader?: string;
    lockExpireTime?: number;
};

export type objectData = {
    toJSON?: Function;
    [key: string]: any
};

export type messageData = string|objectData;

type redisMessageData = {
    messageId?: string;
    msgType: string;
    data: messageData;
};

export default class RedisMethod {
    private redis: Redis;
    private options: RedisMethodOptions;

    private topic: string;
    private keyHeader: string;
    private lockExpireTime: number;

    private MQ_NAME: string;
    private MQ_HASH_NAME: string;
    private MQ_HASH_RETRY_TIMES: string;
    private LOCK_PULL_KEY: string;
    private LOCK_CHECK_KEY: string;
    private LOCK_ORDER_KEY: string;
    private ORDER_CONSUME_SELECTED: string;

    constructor(redis: Redis, options: RedisMethodOptions) {
        this.redis = redis;
        this.options = options;

        const {
            keyHeader,
            topic,
            lockExpireTime
        } = options;

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

    packMessage(data: messageData, msgType: string) {
        if(typeof data === 'string') {
            try {
                data = JSON.parse(data);
            } catch(err) {

            }
        }

        if(typeof data !== 'string' && data.toJSON) {
            data = data.toJSON();
        }

        return JSON.stringify({ data, msgType });
    }

    unpackMessage(jsonStr: string) {
        let data: redisMessageData;

        try {
            data = JSON.parse(jsonStr);
        } catch(ex) {
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
    async expire(key: string, timestamp: number) {
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
            await this.expire(this.LOCK_PULL_KEY, this.lockExpireTime);
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

        if (value < 5) {
            await this.expire(this.LOCK_CHECK_KEY, this.lockExpireTime);
        }

        if (value > 100) {
            await this.redis.del(this.LOCK_CHECK_KEY);
        }

        if(value === 1) {
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
    
    async lpushMessage(messageId: string) {
        return await this.redis.lpush(this.MQ_NAME, messageId);
    }
    
    async rpopMessage() {
        return await this.redis.rpop(this.MQ_NAME);
    }
    
    async rpushMessage(messageId: string) {
        return await this.redis.rpush(this.MQ_NAME, messageId);
    }

    async getMessageList(offset = 0, size = 10) {
        return await this.redis.lrange(this.MQ_NAME, offset, size);
    }

    async setTime(messageId: string) {
        return await this.redis.hset(this.MQ_HASH_NAME, messageId, now());
    }

    async getTimeMap() {
        return await this.redis.hgetall(this.MQ_HASH_NAME) as { [key: string]: any };
    }

    async checkTimeExists(messageId: string) {
        return await this.redis.hexists(this.MQ_HASH_NAME, messageId);
    }

    async getTime(messageId: string) {
        return await this.redis.hget(this.MQ_HASH_NAME, messageId);
    }

    async cleanTime(messageId: string) {
        return await this.redis.hdel(this.MQ_HASH_NAME, messageId);
    }

    async initTime(messageId: string) {
        return await this.redis.hset(this.MQ_HASH_NAME, messageId, '');
    }

    getMessageId(id: number) {
        return `${this.topic}-${id}`;
    }

    async getDetail(messageId: string) {
        const key = `${this.keyHeader}-${messageId}`;
        const data = await this.redis.get(key) || '{}';
        return this.unpackMessage(data);
    }

    async setDetail(messageId: string, data: messageData, msgType: string) {
        const str = this.packMessage(data, msgType);
        const key = `${this.keyHeader}-${messageId}`;
        return await this.redis.set(key, str);
        
    }

    async delDetail(messageId: string) {
        const key = `${this.keyHeader}-${messageId}`;
        return await this.redis.del(key);
    }

    async incrFailedTimes(messageId: string) {
        return await this.redis.hincrby(this.MQ_HASH_RETRY_TIMES, messageId, 1);
    }

    async delFailedTimes(messageId: string) {
        return await this.redis.hdel(this.MQ_HASH_RETRY_TIMES, messageId);
    }

    async multi(options: (string)[][]) {
        return await this.redis.multi(options).exec();
    }
    
    async cleanFailedMsg(messageId: string) {
        const results = await this.redis.multi([
            [ 'get', `${this.keyHeader}-${messageId}` ], // 获得详情 // const detail = await this.redis.getDetail(messageId);
            [ 'hdel', this.MQ_HASH_RETRY_TIMES,  messageId ], // 删除失败次数 // await this.redis.delFailedTimes(messageId);
            [ 'hdel', this.MQ_HASH_NAME, messageId ], // 清理时间 key // await this.redis.cleanTime(messageId);
            [ 'del',  `${this.keyHeader}-${messageId}` ], // 删除 message 详情 // await this.redis.delDetail(messageId);
        ]).exec();

        if (Array.isArray(results)) {
            const detail = this.unpackMessage(results[0]);
            return detail;
        } else {
            throw new Error(JSON.stringify(results || ''));
        }
    }

    async cleanMuliMsg(messageIds: string[]) {
        const cmds = [];
        for (const messageId of messageIds) {
            cmds.push([ 'hdel', this.MQ_HASH_RETRY_TIMES,  messageId ]); // 删除失败次数 // await this.redis.delFailedTimes(messageId);
            cmds.push([ 'hdel', this.MQ_HASH_NAME, messageId ]); // 清理时间 key // await this.redis.cleanTime(messageId);
            cmds.push([ 'del',  `${this.keyHeader}-${messageId}` ]); // 删除 message 详情 // await this.redis.delDetail(messageId);)
        }
        await this.redis.multi(cmds).exec();
    }

    async cleanMsg(messageId: string) {
        await this.redis.multi([
            [ 'hdel', this.MQ_HASH_RETRY_TIMES,  messageId ], // 删除失败次数 // await this.redis.delFailedTimes(messageId);
            [ 'hdel', this.MQ_HASH_NAME, messageId ], // 清理时间 key // await this.redis.cleanTime(messageId);
            [ 'del',  `${this.keyHeader}-${messageId}` ], // 删除 message 详情 // await this.redis.delDetail(messageId);
        ]).exec();
    }

    async pushMessage(id: number, data: messageData, msgType: string) {
        const messageId = this.getMessageId(id);
        const str = this.packMessage(data, msgType);
        const key = `${this.keyHeader}-${messageId}`;

        await this.redis.multi([
            [ 'set', key, str ],
            [ 'hset', this.MQ_HASH_NAME, messageId, '' ],
            [ 'rpush', this.MQ_NAME, messageId ]
        ]).exec();
    }

    async fetchMessageAndSetTime(messageId: string) {
        const cmds = [
            [ 'hset', this.MQ_HASH_NAME, messageId, now().toString() ],
            [ 'get', `${this.keyHeader}-${messageId}` ]
        ];
        const result = await this.redis.multi(cmds).exec();

        const detail = result[1][1];
        return this.unpackMessage(detail);
    }

    /**
     * 获取多个数据
     * @param size number 需要获取的消息数量
     */
    async fetchMultiMessage(size: number) {
        let cmds = [];
        while(size --) {
            cmds.push([
                'lpop', this.MQ_NAME
            ]);
        }

        // 因为必须先获取 messageId 之后再获取消息体，如果数据丢失，就需要等数据修复才能恢复数据了。

        const results: string[][] = await this.redis.multi(cmds).exec();

        const realResults = results.map(r => r[1]).filter(r => !!r);

        if(!realResults.length) return [];

        cmds = [];

        const timeNow = now().toString();

        // 组装设置 time 和获取 detail 的 cmds
        for(const messageId of realResults) {
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
        const dataResults = await this.redis.multi(cmds).exec();
        const list = [];
        for(let i = 1; i < dataResults.length; i += 2) {
            const data = this.unpackMessage(dataResults[i][1]);
            const messageId = realResults[ (i - 1) / 2 ];
            
            if(data && messageId) { // 目前存在 BUG 导致可能 message data 为空
                data.messageId = messageId;
                list.push(data as Required<redisMessageData>);
            }
        }
        return list;
    }

    async initTimeAndRpush(messageId: string, pushLeft: boolean = false) {
        await this.redis.hset(this.MQ_HASH_NAME, messageId, '');

        if (pushLeft) {
            await this.redis.lpush(this.MQ_NAME, messageId);
        } else {
            await this.redis.rpush(this.MQ_NAME, messageId);
        }
        
    }

    async orderConsumeLock() {
        let status = false;
        const num = await this.redis.incr(this.LOCK_ORDER_KEY);
        if (num === 1) {
            status = true;
        }
        await this.expire(this.LOCK_ORDER_KEY, this.lockExpireTime * 5); // 顺序消费，如果存在错误使请求中断，需要完全修复之后才允许重新获取，所以时间设置长一点
        return status;
    }

    async orderConsumeUnlock() {
        await this.redis.del(this.LOCK_ORDER_KEY);
    }

    async initSelectedIds(ids: string[]) {
        if (!ids.length) {
            return;
        }
        await this.redis.set(this.ORDER_CONSUME_SELECTED, ids.join('|'));
        await this.expire(this.ORDER_CONSUME_SELECTED, this.lockExpireTime);
        return;
    }

    async getSelectedIds() {
        const idString = await this.redis.get(this.ORDER_CONSUME_SELECTED) || '';
        const selectIds = idString.trim().split('|').filter(id => !!id);
        return selectIds
    }

    async cleanOrderConsumer() {
        const cmds = [
            [ 'del', this.ORDER_CONSUME_SELECTED ],
            [ 'del', this.LOCK_ORDER_KEY ]
        ]
        await this.redis.multi(cmds).exec();
    }
}
