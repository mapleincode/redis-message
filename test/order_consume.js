// const should = require('should');
// const ioredis = require('ioredis');
// const _redis = new ioredis(6380, '192.168.2.120');
// const RedisMessage = require('../index').RedisMessage;

// const body = {
//     name: 'Bob'
// };

// let indexId = 0;
// let fetchMessageLengthLimit = 0;
// let afterFetchMessageData = null;

// let writeToDB = false;

// function sleep(time) {
//     return new Promise(function (resolve) {
//         setTimeout(resolve, time);
//     });
// }

// const fetchMessage = function (options) {
//     return async function () {
//         const list = [];
//         while (fetchMessageLengthLimit-- > 0) {
//             list.push({
//                 id: indexId++,
//                 data: body,
//                 msgType: 'msgType'
//             });
//         }
//         await sleep(0);
//         return list;
//     };
// };

// const afterFetchMessage = function (options) {
//     return async function (data = {}) {
//         afterFetchMessageData = data;
//     };
// }

// const dealFailedMessage = function (options) {
//     return async function (messageId, detail) {
//         writeToDB = true;
//     };
// };

// const redisMessage = new RedisMessage({
//     redis: _redis,
//     topic: 'topic',
//     fetchMessage: fetchMessage,
//     afterFetchMessage: afterFetchMessage,
//     dealFailedMessage: dealFailedMessage,
//     order
// });

// const redis = redisMessage.redis;

// // describe('order consume', function() {
// //     beforeEach(async function () {
// //         await _redis.flushdb(); // 清理 redis 数据库
// //         indexId = 0;
// //         fetchMessageLengthLimit = 0; // 可 pull 数据为 0
// //         afterFetchMessageData = null; // 初始化 afterFetchMessageData 为 null
// //         await sleep(500);
// //     });

// //     after(async function () {
// //         this.timeout(0);
// //         await _redis.flushdb();
// //         await sleep(1000);
// //         await _redis.quit();
// //     });

// //     it('fetch')
// // });