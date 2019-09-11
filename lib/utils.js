'use strict';

/**
 * 返回当前的时间戳
 */
exports.now = function () {
    const now = new Date().getTime() / 1000;
    return parseInt(now);
};

/**
 * async sleep(second)
 */
exports.sleep = async function(second) {
    return new Promise(function (resolve) {
        setTimeout(() => {
            resolve();
        }, second);
    });
};
