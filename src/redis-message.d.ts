import { messageData } from './redis-method';
import { Redis } from 'ioredis';
/** 回调函数接收的选项参数 */
declare type subFuncOptions = {
    /** 主题标识 */
    topic: string;
    /** 消息类型标识 */
    messageType: string;
};
/** 从数据源拉取的原始消息结构 */
declare type originalMessage = {
    /** 消息自增 ID */
    id: number;
    /** 消息实际数据 */
    data: messageData;
    /** 消息类型标识 */
    msgType: string;
};
/** 回调函数工厂接口：接收选项，返回具体的回调函数 */
interface subFunc<func> {
    (options: subFuncOptions): func;
}
/** 拉取消息的回调函数类型 */
declare type fetchMessageFunc = {
    (): Promise<originalMessage[]>;
};
/** 拉取消息后的回调函数类型 */
declare type afterFetchMessageFunc = {
    (data: {
        offset: number | undefined;
        noChange: boolean;
    }): Promise<void>;
};
/** 处理失败消息的回调函数类型 */
declare type handleFailedMessageFunc = {
    (messageId: string, data: messageData | string): Promise<void>;
};
/** 处理超时消息的回调函数类型 */
declare type dealTimeoutMessageFunc = {
    (timeoutList: string[]): Promise<void>;
};
/** 日志函数类型 */
declare type loggerFunc = {
    warn: Function;
    error: Function;
    info: Function;
};
/** 基础配置选项（必填） */
interface baseMessageOptions {
    /** 主题标识，用于生成 Redis key 前缀 */
    topic: string;
    /** 消息类型标识 */
    messageType: string;
    /** ioredis 实例 */
    redis: Redis;
}
/** 扩展配置选项（可选，有默认值） */
interface extraMessageOptions {
    /** Redis key 前缀，默认 'msg_' */
    keyHeader: string;
    /** 分布式锁过期时间（秒），默认 60 */
    lockExpireTime: number;
    /** 消费超时时间（毫秒），默认 60000 */
    maxAckTimeout: number;
    /** 每次拉取消息的数量，默认 200 */
    eachMessageCount: number;
    /** Redis 队列中最少保持的消息数量，默认 200 */
    minRedisMessageCount: number;
    /** 消息消费失败后的最大重试次数，默认 5 */
    maxRetryTimes: number;
    /** 是否记录失败的消息，默认 true */
    recordFailedMessage: boolean;
    /** 是否启用顺序消费模式，默认 false */
    orderConsumption: boolean;
    /** 是否启用自动 ACK 模式，默认 false */
    autoAck: boolean;
    /** 自定义 logger 实例 */
    logger: loggerFunc;
}
/** 创建 RedisMessage 实例的配置选项 */
export interface redisMessageOptions extends baseMessageOptions, Partial<extraMessageOptions> {
    /** 拉取消息的工厂函数 */
    fetchMessage?: subFunc<fetchMessageFunc>;
    /** 拉取消息后的回调工厂函数 */
    afterFetchMessage?: subFunc<afterFetchMessageFunc>;
    /** 处理失败消息的工厂函数（已废弃，请使用 handleFailedMessage） */
    dealFailedMessage?: subFunc<handleFailedMessageFunc>;
    /** 处理失败消息的工厂函数 */
    handleFailedMessage?: subFunc<handleFailedMessageFunc>;
    /** 处理超时消息的工厂函数 */
    dealTimeoutMessage?: subFunc<dealTimeoutMessageFunc>;
}
/** ACK 确认项，支持多种格式 */
interface ackItem {
    messageId?: string;
    id?: string;
    success?: boolean;
}
/**
 * RedisMessage 消息队列类
 * 提供基于 Redis 的消息队列功能，支持普通消费、顺序消费、自动 ACK 等模式
 */
export default class RedisMessage {
    private options;
    private fetchMessage;
    private afterFetchMessage;
    private handleFailedMessage;
    private redis;
    private logger;
    private dealTimeoutMessage;
    /**
     * 创建 RedisMessage 实例
     * @param options 配置选项
     */
    constructor(options: redisMessageOptions);
    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    _handleFailedMessage(messageId: string): Promise<void>;
    /**
     * 内部方法：执行消息拉取并推入 Redis 队列
     * 1. 调用 fetchMessage 从数据源拉取新消息
     * 2. 逐条推入 Redis 队列
     * 3. 调用 afterFetchMessage 回调
     * 4. 清理拉取锁
     * @returns 成功推入的消息数量
     */
    _pullMessage(): Promise<number>;
    /**
     * 拉取消息并推入 Redis 队列
     * - 当队列为空时，同步拉取并返回 { successItems }
     * - 当队列不为空时，异步拉取（延迟 1s）并立即返回 true
     * - 获取锁失败时返回 false
     * @param existedMQCount 当前队列中已有的消息数量
     * @returns false | true | { successItems: number }
     */
    pullMessage(existedMQCount: number): Promise<boolean | {
        successItems: number;
    }>;
    /**
     * 从队列左侧获取单个消息
     * @returns 消息对象 { messageId, data, msgType }，队列为空时返回 null
     */
    getOneMessage(): Promise<{
        messageId: string;
        data: messageData;
        msgType: string;
    } | null>;
    /**
     * 批量获取消息
     * 1. 检查队列数量，必要时触发拉取
     * 2. 从 Redis 获取指定数量的消息
     * 3. 顺序消费模式下记录已获取的 ID
     * @param size 需要获取的消息数量
     * @returns 消息数据数组
     */
    getMessages(size: number): Promise<Required<{
        messageId?: string | undefined;
        msgType: string;
        data: messageData;
    }>[] | undefined>;
    /**
     * 确认普通消息的消费结果
     * 支持单个/批量确认，支持成功/失败确认
     * - 成功：清理消息的所有 Redis 数据
     * - 失败：调用 _handleFailedMessage 进行重试或废弃
     * @param messageIds 消息 ID（字符串/数组/ackItem 数组）
     * @param allSuccess 默认的成功状态，默认 true
     */
    private ackNormalMessages;
    /**
     * 成功消费消息或者失败消费消息
     * @param {string|array} messageIds 消息 id 或消息 id 数组
     * @param {boolean} success boolean 是否是成功
     */
    ackMessages(messageIds: string[] | ackItem[] | string | boolean, allSuccess?: boolean): Promise<void>;
    /**
     * 修复普通消费模式下的异常消息
     * - missingList: 队列中缺失但 hash 中有记录的消息（time 被初始化为空字符串的场景）
     * - timeoutList: 消费超时的消息
     * @param missingList 缺失的消息 ID 列表
     * @param timeoutList 超时的消息 ID 列表
     */
    fixNormalMessage(missingList: string[], timeoutList: string[]): Promise<{
        timeoutList: string[];
        missingList: string[];
    }>;
    /**
     * 修复顺序消费模式下的异常消息
     * 额外处理：过滤非本次请求的脏数据
     * @param missingList 缺失的消息 ID 列表
     * @param timeoutList 超时的消息 ID 列表
     */
    fixOrderMessage(missingList: string[], timeoutList: string[]): Promise<{
        timeoutList: string[];
        missingList: string[];
    }>;
    /**
     * 检查并修复异常消息
     * 1. 获取检查锁（防止并发修复）
     * 2. 对比队列和 hash 表，找出缺失和超时的消息
     * 3. 根据消费模式调用对应的修复方法
     * @param sleepStatus 是否在执行修复前等待 1s（默认 true，给消费操作留出时间）
     * @returns 修复结果或锁失败提示
     */
    checkExpireMessage(sleepStatus?: boolean): Promise<"消费 lock 失败" | {
        timeoutList: string[];
        missingList: string[];
    }>;
    /**
     * 获取顺序消费的分布式锁
     * @returns 是否获取成功
     */
    private orderConsumeLock;
    /**
     * 保存本次顺序消费获取的消息 ID 列表
     * @param items 消息对象数组
     */
    private setOrderConsumeIds;
    /**
     * 清理顺序消费相关的锁和数据
     */
    private cleanOrderConsume;
    /**
     * 确认顺序消费的消息
     * - successAll=true: 批量清理所有已消费的消息
     * - successAll=false: 将所有消息作为失败重新推入队列
     * @param successAll 是否全部成功
     */
    private ackOrderMessages;
    /**
     * 查询当前是否处于顺序消费模式
     * @returns 是否为顺序消费模式
     */
    isOrderConsumption(): boolean;
    /**
     * 获取待消费的消息列表（管理接口）
     * @returns 待消费消息的数量和 messageId 列表
     */
    __messageUnconsumed(): Promise<{
        itemsLength: number;
        items: any;
    }>;
    /**
     * 获取正在消费中的消息映射（管理接口）
     * @returns 消费中的消息 ID 到时间戳的映射
     */
    __messageConsuming(): Promise<{
        [key: string]: any;
    }>;
}
export {};
