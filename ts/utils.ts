/**
 * 工具函数模块
 */

/**
 * 获取当前时间的 Unix 时间戳（秒级）
 * @returns 当前时间的秒级时间戳
 */
export function now(): number {
    return Math.floor(Date.now() / 1000);
}

/**
 * 异步等待指定时间
 * @param ms 等待的毫秒数
 */
export async function sleep(ms: number): Promise<void> {
    return new Promise(function(resolve) {
        setTimeout(resolve, ms);
    });
}
