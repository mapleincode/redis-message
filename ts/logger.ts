/**
 * 默认日志模块
 * 使用 pino 作为默认日志库
 */

import pino from 'pino';

/** 默认 logger 实例 */
const logger = pino();

export default logger;