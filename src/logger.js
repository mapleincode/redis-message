"use strict";
/**
 * 默认日志模块
 * 使用 pino 作为默认日志库
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const pino_1 = __importDefault(require("pino"));
/** 默认 logger 实例 */
const logger = (0, pino_1.default)();
exports.default = logger;
