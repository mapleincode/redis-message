# Redis-Message

基于 Redis 实现的消息队列库，支持普通消费、顺序消费、自动 ACK 等多种消费模式。

## 特性

- 基于 Redis List 实现消息队列
- 支持普通消费和顺序消费模式
- 支持自动 ACK 模式
- 消息消费失败自动重试，可配置最大重试次数
- 消费超时自动检测与修复
- 分布式锁保证多进程安全
- 支持批量消息获取与确认

## Install

```bash
npm install wm-redis-message --save
```

## Usage

```javascript
const { RedisMessage } = require('wm-redis-message');

const redisMessage = new RedisMessage(options);

// 批量获取消息
const messages = await redisMessage.getMessages(10);

// 消费确认后 ack
const messageIds = messages.map(m => m.messageId);
await redisMessage.ackMessages(messageIds, true);
```

## Options

| 参数名                 | 说明                                       | 默认值   |
| ---------------------- | ------------------------------------------ | -------- |
| topic                  | 主题标识，用于生成 Redis key 前缀           | **必填** |
| messageType            | 消息类型标识                               | **必填** |
| redis                  | ioredis 实例                               | **必填** |
| fetchMessage           | 从数据源拉取消息的工厂函数                 | 返回空数组 |
| afterFetchMessage      | 拉取消息后的回调工厂函数                   | debug 输出 |
| dealFailedMessage      | 处理超过最大重试次数的消息的工厂函数       | debug + warn |
| handleFailedMessage    | dealFailedMessage 的别名                   | -        |
| dealTimeoutMessage     | 处理超时消息的工厂函数                     | 空函数   |
| keyHeader              | Redis key 前缀                             | `msg_`   |
| maxAckTimeout          | 消费超时时间，单位 ms                      | `60000`  |
| eachMessageCount       | 每次拉取消息的数量                         | `200`    |
| minRedisMessageCount   | Redis 队列维持的最小消息数量               | `200`    |
| maxRetryTimes          | 消息消费最大失败重试次数                   | `5`      |
| lockExpireTime         | 分布式锁超时时间，单位 s                   | `60`     |
| autoAck                | 是否启用自动 ACK 模式                      | `false`  |
| orderConsumption       | 是否启用顺序消费模式                       | `false`  |
| logger                 | 自定义 logger 函数（需包含 warn/error/info）| pino     |
| recordFailedMessage    | 是否记录失败的消息                         | `true`   |

### 回调函数说明

三个核心回调函数均为工厂函数，接收 `{ topic, messageType }` 参数，返回具体的异步函数：

```js
const {
    topic,
    messageType,
    redis,
    keyHeader = 'msg_',
    maxAckTimeout = 60 * 1000,     // 消费超时时间 (ms)
    eachMessageCount = 200,         // 每次拉取消息数量
    minRedisMessageCount = 200,     // Redis 队列最少保持数量
    maxRetryTimes = 5,              // 最大失败重试次数
    lockExpireTime = 60,            // 锁超时时间 (s)
    recordFailedMessage = true,     // 是否记录失败消息
    orderConsumption = false,       // 是否顺序消费
    autoAck = false,                // 是否自动 ACK

    // 从数据源拉取新消息
    fetchMessage = function (options) {
        // const { topic, messageType } = options;
        return async function () {
            return [];
        }
    },

    // 拉取消息后的回调
    afterFetchMessage = function (options) {
        // const { topic, messageType } = options;
        return async function (data = {}) {
            const { offset } = data;
            debug(`message has been offset to :${offset}`);
        }
    },

    // 处理超过最大重试次数的失败消息
    dealFailedMessage = function (options) {
        // const { topic, messageType } = options;
        return async function (messageId, detail) {
            debug(`messageId: ${messageId} has been error acks`);
            console.log(`message failed!! id: ${messageId} data: ${JSON.stringify(detail)}`);
        }
    }
} = options;
```

## API

### 消费相关

- `getMessages(size)` - 批量获取消息
- `getOneMessage()` - 获取单条消息
- `ackMessages(messageIds, success)` - 确认消息消费结果
- `isOrderConsumption()` - 查询是否为顺序消费模式

### 管理相关

- `checkExpireMessage(sleepStatus)` - 检查并修复异常消息
- `__messageUnconsumed()` - 获取待消费消息列表
- `__messageConsuming()` - 获取正在消费中的消息映射

## 消费模式说明

### 普通消费模式（默认）

消息按 FIFO 顺序从队列右侧推入、左侧弹出，消费失败后重新推入队列尾部。

### 顺序消费模式

设置 `orderConsumption: true` 开启。消费失败后消息推入队列头部，保证消息按原始顺序被重试消费。需要显式调用 `ackMessages(true/false)` 确认。

### 自动 ACK 模式

设置 `autoAck: true` 开启。消息获取后自动确认，不进行失败重试。

## License

ISC
# Redis-Message

基于 redis 实现简单的队列功能。

## Install

```bash
npm install wm-redis-message --save
```



## Usage

```javascript
const RedisMessage = require('wm-redis-message').RedisMessage;

const cache = new RedisMessgae(options);

console.log(await cache.getMessages(10));
```



## Opitons

| name                 | marks                           | default |
| -------------------- | ------------------------------- | ------- |
| topic                | topic                           |         |
| messageType          | messageType                     |         |
| redis                | redis object                    |         |
| fetchMessage         | fetchMessage 方法               |         |
| afterFetchMessage    | fetchMessage 之后调用的方法     |         |
| dealfailedMessage    | 处理超过 max 失败次数的 Message |         |
| maxAckTimeout        | 最长 acks 时间，单位 ms         | 60000   |
| eachMessageCount     | 每次获取 messageCount 的数量    | 200     |
| minRedisMessageCount | redis 维持的最小数量的 message  | 200     |
| maxRetryTimes        | 消息消费最多失败次数            | 5       |
| lockExpireTime       | 锁超时时间，单位 s              | 60      |
| autoAck              | 自动 ack 消费超时之后自动提交   | 0       |
| orderConsumption     | 顺序消费支持                    | 0       |
| logger               | logger 函数                     |         |
| recordFailedMessage  | 是否记录失败的 message          | 1       |
|                      |                                 |         |

```js
const {
    topic, // topic
    messageType, // messageType
    redis, // redis
    keyHeader = 'msg_', // redis key header
    maxAckTimeout = 60 * 1000, // 消费超时时间 
    eachMessageCount = 200, // 每次 Message 获取数量
    minRedisMessageCount = 200, // Redis 最少的 count 数量
    maxRetryTimes = 5, // 消息消费失败重新消费次数
    lockExpireTime = 60, // Lock timeout

    fetchMessage = function (options) {
        // const { topic, messageType } = options;
        return async function () {
            return [];
        }
    },
    afterFetchMessage = function (options) {
        // const { topic, messageType } = options;
        return async function (data = {}) {
            const {
                offset
            } = data;
            debug(`message has been offset to :${offset}`);
        }
    },
    dealfailedMessage = function (options) {
        // const { topic, messageType } = options;
        return async function (messageId, detail) {
            debug(`messageId: ${messageId} has been error acks`);
            console.log(`message failed!! id: ${messageId} data: ${JSON.stringify(detail)}`);
            return;
        }
    }
} = options;
```

