/** Redis 方法配置选项 */
export type RedisMethodOptions = {
  /** 主题标识，用于生成 Redis key 前缀 */
  topic: string;
  /** Redis key 的自定义前缀，默认 'msg_' */
  keyHeader?: string;
  /** 分布式锁的过期时间（秒），默认 60 */
  lockExpireTime?: number;
};

/** 对象类型的数据，支持 toJSON 序列化 */
export type ObjectData = {
  toJSON?: Function;
  [key: string]: any
};

/** 消息数据类型，支持字符串或对象 */
export type MessageData = string|ObjectData;

/** Redis 中存储的消息数据结构 */
export type RedisMessageData = {
  messageId?: string;
  msgType: string;
  data: MessageData;
};

/** Redis multi 事务命令参数类型 */
export type MultiCommand = string[];
