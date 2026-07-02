"use strict";
/**
 * 工具函数模块
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.now = now;
exports.sleep = sleep;
/**
 * 获取当前时间的 Unix 时间戳（秒级）
 * @returns 当前时间的秒级时间戳
 */
function now() {
    return Math.floor(Date.now() / 1000);
}
/**
 * 异步等待指定时间
 * @param ms 等待的毫秒数
 */
function sleep(ms) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise(function (resolve) {
            setTimeout(resolve, ms);
        });
    });
}
