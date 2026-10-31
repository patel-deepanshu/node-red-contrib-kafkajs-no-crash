'use strict';

/**
 * Process-wide lock for offline-queue directories: two DiskQueue objects
 * writing the same directory would overwrite each other's records (each one
 * appends at its own idea of the segment end) and corrupt the cursor.
 *
 * A directory has at most ONE holder, also when the same node id asks again.
 * On a redeploy the new instance of a producer must wait until the old
 * instance has really closed its queue (every pending append written), not
 * merely until the old close handler stopped waiting for a slow disk. So:
 *
 * - the old instance marks its claim as closing when its close starts
 *   (`release.closing()`) and releases it only after DiskQueue.close() has
 *   finished;
 * - the new instance waits for that with whenReleased(), bounded, and then
 *   acquires. If the old one is still closing, acquire() throws: the new
 *   instance runs without the queue for a while instead of sharing it.
 *
 * A directory held by a node that is NOT closing is a configuration problem
 * (two producers with the same queue): whenReleased() does not wait for it.
 */

const path = require('path');

/** normalised dir → { owner: string, closing: boolean, released: Promise<void>, resolve: Function } */
const holders = new Map();

function keyOf(dir) {
    const resolved = path.resolve(String(dir));
    // Windows and macOS file systems are case-insensitive by default.
    return process.platform === 'win32' || process.platform === 'darwin' ? resolved.toLowerCase() : resolved;
}

/**
 * Claim `dir` for `ownerId`.
 * Throws when anyone holds it: Error('Queue directory <dir> is already used
 * by node <owner>') with code 'QUEUE_DIR_IN_USE', or, when the holder is
 * closing (see whenReleased()), '... is still being closed by node <owner>'
 * with code 'QUEUE_DIR_CLOSING'.
 *
 * @param {string} dir
 * @param {string} ownerId usually the node id
 * @returns {Function} release(): idempotent, only ever releases this claim.
 *   `release.closing()` marks the claim as closing (the holder is shutting
 *   down and will release it soon), so a successor may wait for it.
 */
function acquire(dir, ownerId) {
    const key = keyOf(dir);
    const held = holders.get(key);
    if (held) {
        const where = path.resolve(String(dir));
        const err = new Error(held.closing
            ? `Queue directory ${where} is still being closed by node ${held.owner}`
            : `Queue directory ${where} is already used by node ${held.owner}`);
        err.code = held.closing ? 'QUEUE_DIR_CLOSING' : 'QUEUE_DIR_IN_USE';
        throw err;
    }
    const entry = { owner: String(ownerId), closing: false, released: null, resolve: null };
    entry.released = new Promise((resolve) => {
        entry.resolve = resolve;
    });
    holders.set(key, entry);
    // Compared by identity: a late release by an old instance can never drop
    // the claim of a newer instance with the same owner id.
    const release = function release() {
        if (holders.get(key) === entry) holders.delete(key);
        entry.resolve();
    };
    release.closing = function closing() {
        entry.closing = true;
    };
    return release;
}

/**
 * Wait until nobody holds `dir`. Resolves true when it is free; false at once
 * when a node that is not closing holds it, or when the holder is still
 * closing after `timeoutMs`. Never rejects.
 *
 * @param {string} dir
 * @param {number} [timeoutMs] no limit when omitted
 * @returns {Promise<boolean>}
 */
function whenReleased(dir, timeoutMs) {
    const held = holders.get(keyOf(dir));
    if (!held) return Promise.resolve(true);
    if (!held.closing) return Promise.resolve(false);
    const released = held.released.then(() => !holders.has(keyOf(dir)));
    if (!(timeoutMs >= 0) || timeoutMs === Infinity) return released;
    return new Promise((resolve) => {
        const timer = setTimeout(() => resolve(false), timeoutMs);
        if (timer && typeof timer.unref === 'function') timer.unref();
        released.then((free) => {
            clearTimeout(timer);
            resolve(free);
        });
    });
}

/**
 * Whether some owner currently holds `dir` (closing or not).
 *
 * @param {string} dir
 * @returns {boolean}
 */
function isOwned(dir) {
    return holders.has(keyOf(dir));
}

/**
 * The id of the owner holding `dir`, or null.
 *
 * @param {string} dir
 * @returns {string|null}
 */
function ownerOf(dir) {
    const entry = holders.get(keyOf(dir));
    return entry ? entry.owner : null;
}

module.exports = { acquire, whenReleased, isOwned, ownerOf, _reset: () => holders.clear() };
