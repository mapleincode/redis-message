export function now(): number {
    const now = new Date().getTime() / 1000;
    return parseInt(now.toString());
}

/**
 * sleep
 * @param second 时间
 */
export async function sleep(second: number): Promise<void> {
    return new Promise(function(resolve) {
        setTimeout(() => {
            resolve();
        }, second);
    });
}
