# Redis-Message



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

```js

```

