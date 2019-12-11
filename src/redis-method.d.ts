/// <reference types="node" />
import { Redis } from 'ioredis';
export declare type RedisMethodOptions = {
    topic: string;
    keyHeader?: string;
    lockExpireTime?: number;
};
export declare type objectData = {
    toJSON?: Function;
    [key: string]: any;
};
export declare type messageData = string | objectData;
declare type redisMessageData = {
    messageId?: string;
    msgType: string;
    data: messageData;
};
export default class RedisMethod {
    private redis;
    private options;
    private topic;
    private keyHeader;
    private lockExpireTime;
    private MQ_NAME;
    private MQ_HASH_NAME;
    private MQ_HASH_RETRY_TIMES;
    private LOCK_PULL_KEY;
    private LOCK_CHECK_KEY;
    private LOCK_ORDER_KEY;
    private ORDER_CONSUME_SELECTED;
    constructor(redis: Redis, options: RedisMethodOptions);
    packMessage(data: messageData, msgType: string): string;
    unpackMessage(jsonStr: string): redisMessageData;
    /**
     * 设置 redis key 过期时间戳
     * @param {string} key key
     * @param {integer} timestamp 时间戳
     */
    expire(key: string, timestamp: number): Promise<import("v8").DoesZapCodeSpaceFlag>;
    /**
     * 返回消息队列的总数
     * @return {integer} count
     */
    messageCount(): Promise<number>;
    /**
     * 设置 pull 的锁
     * @return {boolean} status 是否 lock 成功
     */
    setPullLock(): Promise<boolean>;
    /**
     * 清理 pull 的锁
     */
    cleanPullLock(): Promise<number>;
    setCheckLock(): Promise<boolean>;
    cleanCheckLock(): Promise<number>;
    lpopMessage(): Promise<string>;
    lpushMessage(messageId: string): Promise<any>;
    rpopMessage(): Promise<string>;
    rpushMessage(messageId: string): Promise<any>;
    getMessageList(offset?: number, size?: number): Promise<any>;
    setTime(messageId: string): Promise<import("v8").DoesZapCodeSpaceFlag>;
    getTimeMap(): Promise<{
        [key: string]: any;
    }>;
    checkTimeExists(messageId: string): Promise<import("v8").DoesZapCodeSpaceFlag>;
    getTime(messageId: string): Promise<string | null>;
    cleanTime(messageId: string): Promise<any>;
    initTime(messageId: string): Promise<import("v8").DoesZapCodeSpaceFlag>;
    getMessageId(id: number): string;
    getDetail(messageId: string): Promise<redisMessageData>;
    setDetail(messageId: string, data: messageData, msgType: string): Promise<string>;
    delDetail(messageId: string): Promise<number>;
    incrFailedTimes(messageId: string): Promise<number>;
    delFailedTimes(messageId: string): Promise<any>;
    multi(options: (string)[][]): Promise<any>;
    cleanFailedMsg(messageId: string): Promise<redisMessageData>;
    cleanMuliMsg(messageIds: string[]): Promise<void>;
    cleanMsg(messageId: string): Promise<void>;
    pushMessage(id: number, data: messageData, msgType: string): Promise<void>;
    fetchMessageAndSetTime(messageId: string): Promise<redisMessageData>;
    /**
     * 获取多个数据
     * @param size number 需要获取的消息数量
     */
    fetchMultiMessage(size: number): Promise<Required<redisMessageData>[]>;
    initTimeAndRpush(messageId: string, pushLeft?: boolean): Promise<void>;
    orderConsumeLock(): Promise<boolean>;
    orderConsumeUnlock(): Promise<void>;
    initSelectedIds(ids: string[]): Promise<void>;
    getSelectedIds(): Promise<string[]>;
    cleanOrderConsumer(): Promise<void>;
}
export {};
