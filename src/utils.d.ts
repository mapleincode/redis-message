/**
 * 工具函数模块
 */
/**
 * 获取当前时间的 Unix 时间戳（秒级）
 * @returns 当前时间的秒级时间戳
 */
export declare function now(): number;
/**
 * 异步等待指定时间
 * @param ms 等待的毫秒数
 */
export declare function sleep(ms: number): Promise<void>;
