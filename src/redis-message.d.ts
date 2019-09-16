import { messageData } from './redis-method';
import { Redis } from 'ioredis';
declare type subFuncOptions = {
    topic: string;
    messageType: string;
};
declare type originalMessage = {
    id: number;
    data: messageData;
    msgType: string;
};
interface subFunc<func> {
    (options: subFuncOptions): func;
}
declare type fetchMessageFunc = {
    (): Promise<originalMessage[]>;
};
declare type afterFetchMessageFunc = {
    (data: {
        offset: number | undefined;
        noChange: boolean;
    }): Promise<void>;
};
declare type handleFailedMessageFunc = {
    (messageId: string, data: messageData | string): Promise<void>;
};
declare type loggerFunc = {
    warn: Function;
    error: Function;
    info: Function;
};
interface baseMessageOptions {
    topic: string;
    messageType: string;
    redis: Redis;
}
interface extraMessageOptions {
    keyHeader: string;
    lockExpireTime: number;
    maxAckTimeout: number;
    eachMessageCount: number;
    minRedisMessageCount: number;
    maxRetryTimes: number;
    recordFailedMessage: boolean;
    orderConsumption: boolean;
    autoAck: boolean;
    logger: loggerFunc;
}
export interface redisMessageOptions extends baseMessageOptions, Partial<extraMessageOptions> {
    fetchMessage?: subFunc<fetchMessageFunc>;
    afterFetchMessage?: subFunc<afterFetchMessageFunc>;
    dealFailedMessage?: subFunc<handleFailedMessageFunc>;
    handleFailedMessage?: subFunc<handleFailedMessageFunc>;
}
interface ackItem {
    messageId?: string;
    id?: string;
    success?: boolean;
}
export default class RedisMessage {
    private options;
    private fetchMessage;
    private afterFetchMessage;
    private handleFailedMessage;
    private redis;
    private logger;
    constructor(options: redisMessageOptions);
    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    _handleFailedMessage(messageId: string): Promise<void>;
    _pullMessage(): Promise<number>;
    /**
     * 获取 messgae 数据
     * @return {boolean} 是否成功 pull
     */
    pullMessage(existedMQCount: number): Promise<boolean | {
        successItems: number;
    }>;
    /**
     * 获取单个消息
     * @return {object} 消息  { messageId, data }
     */
    getOneMessage(): Promise<{
        messageId: string;
        data: messageData;
        msgType: string;
    } | null>;
    /**
     * 根据数量返回消息 list
     * @param {integer} size 需要获得的消息数量
     * @return {array}  返回消息的数组
     */
    getMessages(size: number): Promise<Required<{
        messageId?: string | undefined;
        msgType: string;
        data: messageData;
    }>[]>;
    private ackNormalMessages;
    /**
     * 成功消费消息或者失败消费消息
     * @param {string|array} messageIds 消息 id 或消息 id 数组
     * @param {boolean} success boolean 是否是成功
     */
    ackMessages(messageIds: string[] | ackItem[] | string | boolean, allSuccess?: boolean): Promise<void>;
    /**
     * 1. 检查消息是否有异常
     * 2. 检查消息消费是否超时
     */
    checkExpireMessage(): Promise<{
        timeoutList: string[];
        missingList: string[];
    } | undefined>;
    /**
     * 顺序消费
     */
    private orderConsumeLock;
    /**
     * 对获取的数据的 id 进行保存
     * @param ids 消费的 ids
     */
    private setOrderConsumeIds;
    private cleanOrderConsume;
    private ackOrderMessages;
    __messageUnconsumed(): Promise<{
        itemsLength: number;
        items: any;
    }>;
    __messageConsuming(): Promise<any>;
}
export {};
