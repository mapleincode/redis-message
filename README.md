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

