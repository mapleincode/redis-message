/**
 * RedisMessage 类
 * create by maple 2018-11-16 11:06:59
 */
'use strict';
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const redis_method_1 = __importDefault(require("./redis-method"));
const utils_1 = require("./utils");
const logger_1 = __importDefault(require("./logger"));
const debug_1 = __importDefault(require("debug"));
const debug = debug_1.default('redis-message');
;
class RedisMessage {
    constructor(options) {
        const { topic, // topic 用于作为标识，在 redis key 进行区分
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
        fetchMessage = function ( /* { topic, messageType } */) {
            return function () {
                return __awaiter(this, void 0, void 0, function* () {
                    return [];
                });
            };
        }, afterFetchMessage = function ( /* { topic, messageType } */) {
            // const  = options;
            const func = function (data) {
                return __awaiter(this, void 0, void 0, function* () {
                    const { offset } = data;
                    debug(`message has been offset to :${offset}`);
                });
            };
            return func;
        }, dealFailedMessage = function ( /* { topic, messageType } */) {
            return function (messageId, detail) {
                return __awaiter(this, void 0, void 0, function* () {
                    debug(`messageId: ${messageId} has been error acks`);
                    logger.warn(`message failed!! id: ${messageId} data: ${JSON.stringify(detail)}`);
                    return;
                });
            };
        }, handleFailedMessage, 
        // ================== other =======================
        logger = logger_1.default, // logger
        redis // ioredis 实例
         } = options;
        this.options = {
            topic,
            messageType,
            redis,
            logger,
            // ============== 以下是推荐默认参数 ================
            keyHeader,
            lockExpireTime,
            maxAckTimeout: maxAckTimeout,
            maxAckTimeoutSecords: parseInt((maxAckTimeout / 1000).toString()),
            eachMessageCount: eachMessageCount,
            minRedisMessageCount: minRedisMessageCount,
            maxRetryTimes: maxRetryTimes,
            recordFailedMessage,
            // ================= Mode 功能选项 ===================
            orderConsumption,
            autoAck,
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
        }
        else if (this.options.autoAck) {
            this.options.maxRetryTimes = 0;
            this.options.recordFailedMessage = false;
        }
        this.fetchMessage = this.options.fetchMessage;
        this.afterFetchMessage = this.options.afterFetchMessage;
        this.handleFailedMessage = this.options.handleFailedMessage;
        // ioredis 实例
        this.redis = new redis_method_1.default(this.options.redis, this.options);
        // logger
        this.logger = this.options.logger;
    }
    /**
     * 处理失败的 message
     * 如果小于 maxRetryTimes，重新计数放入队列。
     * 如果大于则移除队列
     * @param {string} messageId messageId
     */
    _handleFailedMessage(messageId) {
        return __awaiter(this, void 0, void 0, function* () {
            // 获取失败次数
            const failedTimes = yield this.redis.incrFailedTimes(messageId);
            if (failedTimes > this.options.maxRetryTimes) {
                let detail;
                try {
                    detail = yield this.redis.cleanFailedMsg(messageId);
                }
                catch (err) {
                    this.logger.error(err);
                }
                // 最后调用 deal 函数
                yield this.handleFailedMessage(messageId, detail || '');
                return;
            }
            // 初始化调用时间
            // 重新 push 到队列中
            yield this.redis.initTimeAndRpush(messageId);
        });
    }
    _pullMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            const list = yield this.fetchMessage();
            let offset;
            try {
                for (const msg of list) {
                    const { id, // 唯一 id
                    data, // data
                    msgType // 消息类型
                     } = msg;
                    yield this.redis.pushMessage(id, data, msgType);
                    // 更新 offset
                    offset = id + 1;
                }
            }
            catch (err) {
                this.logger.error(err);
            }
            yield this.afterFetchMessage({
                offset: offset,
                noChange: !offset
            });
            yield this.redis.cleanPullLock();
        });
    }
    /**
     * 获取 messgae 数据
     * @return {boolean} 是否成功 pull
     */
    pullMessage(mqCount) {
        return __awaiter(this, void 0, void 0, function* () {
            // 先 lock
            const lockStatus = yield this.redis.setPullLock();
            if (!lockStatus)
                return false;
            // fetch message list
            const self = this;
            // Async
            if (mqCount === 0) {
                // 如果不 pull 数据可能为 0,所以还是等 pull message 完成再返回消息
                yield self._pullMessage(); // 直接 pull
            }
            else {
                // 如果数据总不是不是 0 ，那就异步 pull message。先返回消息。保证消息的延迟。
                setTimeout(function () {
                    self._pullMessage();
                }, 1000);
            }
            return true;
        });
    }
    /**
     * 获取单个消息
     * @return {object} 消息  { messageId, data }
     */
    getOneMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            const messageId = yield this.redis.lpopMessage();
            if (!messageId) {
                return null;
            }
            const data = yield this.redis.fetchMessageAndSetTime(messageId);
            return {
                messageId: messageId,
                data: data.data,
                msgType: data.msgType
            };
        });
    }
    /**
     * 根据数量返回消息 list
     * @param {integer} size 需要获得的消息数量
     * @return {array}  返回消息的数组
     */
    getMessages(size) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!size || size < 1) {
                size = 1;
            }
            const mqCount = yield this.redis.messageCount();
            if (mqCount < this.options.minRedisMessageCount) {
                const status = yield this.pullMessage(mqCount); // async
                if (status === false) {
                    // 没有更新
                    yield utils_1.sleep(500);
                }
            }
            const list = yield this.redis.fetchMultiMessage(size);
            return list;
        });
    }
    /**
     * 成功消费消息或者失败消费消息
     * @param {string|array} messageIds 消息 id 或消息 id 数组
     * @param {boolean} success boolean 是否是成功
     */
    ackMessages(messageIds, success) {
        return __awaiter(this, void 0, void 0, function* () {
            if (typeof messageIds === 'string') {
                messageIds = [messageIds];
            }
            debug(`获得消息的数量为: ${messageIds.length}`);
            for (const messageId of messageIds) {
                let _messageId;
                let _success;
                if (typeof messageId === 'object') {
                    _messageId = messageId.messageId || messageId.id;
                    _success = messageId.success;
                }
                if (!_messageId && typeof messageId === 'string') {
                    _messageId = messageId;
                }
                _success = _success || success;
                if (_success === undefined) {
                    _success = true;
                }
                if (!_messageId) {
                    continue;
                }
                const time = yield this.redis.getTime(_messageId);
                if (!time) {
                    // 已超时被处理
                    const existsStatus = yield this.redis.checkTimeExists(_messageId);
                    if (existsStatus) {
                        debug(`messageId: ${_messageId} 超时被置为  null`);
                        continue;
                    }
                    debug(`messageId: ${_messageId} 没有 time`);
                    _success = true;
                }
                if (_success) {
                    debug(`messageId: ${_messageId} 设置成功`);
                    yield this.redis.cleanMsg(_messageId);
                }
                else {
                    debug(`messageId: ${_messageId} 没有 time`);
                    debug(`messageId: ${_messageId} 设置失败`);
                    yield this._handleFailedMessage(_messageId);
                }
            }
        });
    }
    /**
     * 1. 检查消息是否有异常
     * 2. 检查消息消费是否超时
     */
    checkExpireMessage() {
        return __awaiter(this, void 0, void 0, function* () {
            // 关门打狗
            const status = yield this.redis.setCheckLock();
            if (!status)
                return;
            // 已有的消息的数量
            const mqCount = yield this.redis.messageCount();
            // mq 里所有的 messageId s
            const mqMessages = yield this.redis.getMessageList(0, mqCount + this.options.eachMessageCount);
            // hash map
            const hashMap = yield this.redis.getTimeMap();
            const missingList = [];
            const timeoutList = [];
            const keys = Object.keys(hashMap);
            for (const key of keys) {
                yield utils_1.sleep(0);
                const index = mqMessages.indexOf(key);
                if (index < 0 && !hashMap[key]) {
                    // 数据缺失
                    missingList.push(key);
                }
                else if (index < 0 && hashMap[key] && (utils_1.now() - hashMap[key]) > this.options.maxAckTimeoutSecords) {
                    // 数据 ack 超时
                    timeoutList.push(key);
                }
            }
            for (const messageId of missingList) {
                // 这边检查是 HASH 表是否存在
                const existsStatus = yield this.redis.checkTimeExists(messageId);
                if (!existsStatus) { // 数据延迟问题
                    continue;
                }
                yield this.redis.rpushMessage(messageId);
            }
            for (const messageId of timeoutList) {
                const time = yield this.redis.getTime(messageId);
                if (!time) { // 数据延迟 null or 不存在
                    continue;
                }
                yield this._handleFailedMessage(messageId);
            }
            // 清理锁
            yield this.redis.cleanCheckLock();
            return {
                timeoutList: timeoutList,
                missingList: missingList
            };
        });
    }
    __messageUnconsumed() {
        return __awaiter(this, void 0, void 0, function* () {
            const length = yield this.redis.messageCount();
            let items = [];
            if (length > 0) {
                items = yield this.redis.getMessageList(0, length);
            }
            return {
                itemsLength: length,
                items: items
            };
        });
    }
    __messageConsuming() {
        return __awaiter(this, void 0, void 0, function* () {
            const map = yield this.redis.getTimeMap();
            return map;
        });
    }
}
exports.default = RedisMessage;
module.exports = RedisMessage;
