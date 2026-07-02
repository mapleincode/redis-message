# Changelog

## [1.4.0] - 2026-07-02

### Bug Fixes

- **redis-method**: 修复 `cleanFailedMsg` 中 `unpackMessage(results[0])` 应为 `unpackMessage(results[0][1])` 的问题，ioredis multi 返回的是 `[error, value]` 格式
- **redis-method**: 修复 `unpackMessage` 未处理 `null`/`undefined` 输入的问题，现在正确返回 `null`
- **redis-method**: 修复 `multi` 方法参数类型 `(string)[][]` 过于宽泛，改为 `string[][]` 与 ioredis 类型一致
- **redis-message**: 修复 `getOneMessage` 中 `fetchMessageAndSetTime` 返回 null 时可能的空指针异常
- **redis-message**: 移除未使用的 `lodash` 导入
- **test/order_consume**: 修复嵌套 `it()` 调用的 bug，内层 `it` 永远不会被执行
- **test/redis**: 修复 `unpackMessage` 测试中 `should.be.Null` 缺少函数调用括号
- **test/message**: 修复空的 `ack message timeout` 测试用例，改为 `this.skip()`

### Improvements

- **全部源码**: 为所有类、接口、类型、方法添加了完整的 JSDoc 注释
- **ts/utils**: 优化 `now()` 函数，使用 `Math.floor(Date.now() / 1000)` 替代字符串转换
- **ts/utils**: 优化 `sleep()` 函数，简化 `setTimeout` 调用
- **ts/redis-method**: 重命名 `cleanMuliMsg` 为 `cleanMultiMsg`（修复拼写错误）
- **tsconfig**: 添加 `skipLibCheck` 和 `types` 配置，解决旧版 TypeScript 与新版类型定义的兼容性问题
- **package**: 固定 `@types/node@12` 和 `@types/ms@0.7.31` 以兼容 TypeScript 3.6

### Documentation

- **README**: 修复多处拼写错误（`RedisMessgae` -> `RedisMessage`，`Opitons` -> `Options`，`dealfailedMessage` -> `dealFailedMessage`）
- **README**: 重写 Options 表格，添加中文说明和默认值
- **README**: 添加特性列表、API 文档、消费模式说明
- **README**: 优化代码示例，补充完整的使用流程
- **CHANGELOG**: 新增变更日志文件
