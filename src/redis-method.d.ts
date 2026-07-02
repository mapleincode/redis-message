/**
 * RedisMethod 类
 * 封装与 Redis 交互的底层方法，包括消息队列的增删改查、分布式锁、事务操作等
 */
import { Redis } from "ioredis";
import RedisLock from "wm-redis-locks";
import { MessageData, MultiCommand, RedisMessageData, RedisMethodOptions } from "./type";
export default class RedisMethod {
    private readonly redis;
    private options;
    private readonly topic;
    private readonly keyHeader;
    private readonly lockExpireTime;
    private readonly MQ_NAME;
    private readonly MQ_HASH_NAME;
    private readonly MQ_HASH_RETRY_TIMES;
    private readonly LOCK_PULL_KEY;
    private readonly LOCK_CHECK_KEY;
    private readonly LOCK_ORDER_KEY;
    private readonly ORDER_CONSUME_SELECTED;
    private readonly pullLock;
    private readonly checkLock;
    private readonly orderLock;
    constructor(redis: Redis, options: RedisMethodOptions);
    getPullLock(): RedisLock;
    getCheckLock(): RedisLock;
    getOrderLock(): RedisLock;
    /**
     * 序列化消息数据为 JSON 字符串
     * 1. 如果 data 是 JSON 字符串，尝试解析为对象
     * 2. 如果 data 有 toJSON 方法，调用 toJSON 转换
     * 3. 最终序列化为 { data, msgType } 格式的 JSON
     * @param data 消息数据
     * @param msgType 消息类型标识
     * @returns JSON 字符串
     */
    packMessage(data: MessageData, msgType: string): string;
    /**
     * 反序列化 JSON 字符串为消息数据对象
     * 如果解析失败，返回 { msgType: 'unknown', data: jsonStr } 作为兜底
     * @returns 解析后的消息数据对象，输入为 null/undefined 时返回 null
     * @param json
     */
    unpackMessage(json: unknown): RedisMessageData | null;
    /**
     * 设置 redis key 过期时间戳
     * @param {string} key key
     * @param {integer} timestamp 时间戳
     */
    expire(key: string, timestamp: number): Promise<number>;
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
    cleanPullLock(): Promise<void>;
    setCheckLock(): Promise<boolean>;
    cleanCheckLock(): Promise<void>;
    /**
     * 从队列左侧弹出一个 messageId
     * @returns messageId 或 null（队列为空时）
     */
    lpopMessage(): Promise<string | null>;
    /**
     * 从队列左侧推入一个 messageId（用于顺序消费重试）
     * @param messageId 消息 ID
     */
    lpushMessage(messageId: string): Promise<number>;
    /**
     * 从队列右侧弹出一个 messageId
     * @returns messageId 或 null（队列为空时）
     */
    rpopMessage(): Promise<string | null>;
    /**
     * 从队列右侧推入一个 messageId（普通消息入队）
     * @param messageId 消息 ID
     */
    rpushMessage(messageId: string): Promise<number>;
    /**
     * 获取队列中指定范围的 messageId 列表
     * @param offset 起始偏移量，默认 0
     * @param size 结束偏移量，默认 10
     * @returns messageId 数组
     */
    getMessageList(offset?: number, size?: number): Promise<string[]>;
    setTime(messageId: string): Promise<number>;
    getTimeMap(): Promise<{
        [key: string]: any;
    }>;
    checkTimeExists(messageId: string): Promise<number>;
    getTime(messageId: string): Promise<string | null>;
    cleanTime(messageId: string): Promise<number>;
    initTime(messageId: string): Promise<number>;
    /**
     * 根据消息 ID 生成唯一标识
     * @param id 消息自增 ID
     * @returns 格式为 '{topic}-{id}' 的消息 ID
     */
    getMessageId(id: number): string;
    /**
     * 获取消息详情
     * @param messageId 消息 ID
     * @returns 解析后的消息数据
     */
    getDetail(messageId: string): Promise<RedisMessageData | null>;
    /**
     * 设置消息详情（序列化为 JSON 存储）
     * @param messageId 消息 ID
     * @param data 消息数据
     * @param msgType 消息类型
     */
    setDetail(messageId: string, data: MessageData, msgType: string): Promise<"OK">;
    /**
     * 删除消息详情
     * @param messageId 消息 ID
     */
    delDetail(messageId: string): Promise<number>;
    /**
     * 递增消息的失败次数
     * @param messageId 消息 ID
     * @returns 递增后的失败次数
     */
    incrFailedTimes(messageId: string): Promise<number>;
    /**
     * 删除消息的失败次数记录
     * @param messageId 消息 ID
     */
    delFailedTimes(messageId: string): Promise<number>;
    /**
     * 执行 Redis 事务（multi/exec）
     * @param options 命令数组，每个命令为 [command, ...args] 格式
     * @returns 事务执行结果
     */
    multi(options: MultiCommand[]): Promise<[error: Error | null, result: unknown][] | null>;
    /**
     * 清理失败消息的所有相关数据（原子操作）
     * 包括：获取详情、删除失败次数、清理时间记录、删除消息详情
     * @param messageId 消息 ID
     * @returns 消息详情数据
     */
    cleanFailedMsg(messageId: string): Promise<(RedisMessageData | null)[] | null>;
    /**
     * 批量清理多个消息的所有相关数据（原子操作）
     * @param messageIds 消息 ID 数组
     */
    cleanMultiMsg(messageIds: string[]): Promise<void>;
    /**
     * 清理单个消息的所有相关数据（原子操作）
     * @param messageId 消息 ID
     */
    cleanMsg(messageId: string): Promise<void>;
    /**
     * 将消息推入队列并存储详情（原子操作）
     * 同时执行：存储消息详情、初始化时间记录、推入队列
     * @param id 消息自增 ID
     * @param data 消息数据
     * @param msgType 消息类型
     */
    pushMessage(id: number, data: MessageData, msgType: string): Promise<void>;
    /**
     * 弹出消息并设置消费时间戳（原子操作）
     * 用于单条消息获取时，同时记录消费开始时间
     * @param messageId 消息 ID
     * @returns 解析后的消息数据
     */
    fetchMessageAndSetTime(messageId: string): Promise<RedisMessageData | null>;
    /**
     * 批量获取消息（原子操作）
     * 分两步执行：
     * 1. 从队列左侧依次弹出 size 条 messageId
     * 2. 批量设置消费时间戳并获取消息详情
     * @param size 需要获取的消息数量
     * @returns 消息数据数组
     */
    fetchMultiMessage(size: number): Promise<Required<RedisMessageData>[]>;
    /**
     * 重新初始化消息的消费时间并推入队列
     * 用于消费失败后重试的场景
     * @param messageId 消息 ID
     * @param pushLeft 是否从左侧推入（顺序消费为 true，普通消费为 false）
     */
    initTimeAndRpush(messageId: string, pushLeft?: boolean): Promise<void>;
    /**
     * 获取顺序消费的分布式锁
     * @returns 是否获取锁成功
     */
    orderConsumeLock(): Promise<boolean>;
    /**
     * 释放顺序消费的分布式锁
     */
    orderConsumeUnlock(): Promise<void>;
    /**
     * 保存顺序消费中选中的消息 ID 列表
     * @param ids 消息 ID 数组，以 '|' 分隔存储
     */
    initSelectedIds(ids: string[]): Promise<void>;
    /**
     * 获取顺序消费中选中的消息 ID 列表
     * @returns 消息 ID 数组
     */
    getSelectedIds(): Promise<string[]>;
    /**
     * 清理顺序消费相关的所有数据（选中 ID 和锁）
     */
    cleanOrderConsumer(): Promise<void>;
}
