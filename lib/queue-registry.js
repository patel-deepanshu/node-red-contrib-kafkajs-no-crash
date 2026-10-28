'use strict';

/**
 * Process-wide lock for offline-queue directories: two producer nodes writing
 * the same queue directory would corrupt each other's segments and cursor.
 *
 * The same owner may acquire a directory several times (reference counted).
 * That happens on redeploy: the new node instance has the same id and may
 * start while the old instance is still finishing its close handler.
 */

const path = require('path');

/** normalised dir → { owner: string, count: number } */
const owners = new Map();

function keyOf(dir) {
    const resolved = path.resolve(String(dir));
    // Windows and macOS file systems are case-insensitive by default.
    return process.platform === 'win32' || process.platform === 'darwin' ? resolved.toLowerCase() : resolved;
}

/**
 * Claim `dir` for `ownerId`.
 * Throws Error('Queue directory <dir> is already used by node <owner>') when a
 * DIFFERENT owner holds it. Returns an idempotent release function.
 *
 * @param {string} dir
 * @param {string} ownerId usually the node id
 * @returns {() => void} release
 */
function acquire(dir, ownerId) {
    const key = keyOf(dir);
    const owner = String(ownerId);
    const entry = owners.get(key);
    if (entry && entry.owner !== owner) {
        throw new Error(`Queue directory ${path.resolve(String(dir))} is already used by node ${entry.owner}`);
    }
    if (entry) entry.count++;
    else owners.set(key, { owner, count: 1 });
    let released = false;
    return function release() {
        if (released) return;
        released = true;
        const current = owners.get(key);
        if (!current || current.owner !== owner) return;
        current.count--;
        if (current.count <= 0) owners.delete(key);
    };
}

/**
 * Whether some owner currently holds `dir`.
 *
 * @param {string} dir
 * @returns {boolean}
 */
function isOwned(dir) {
    return owners.has(keyOf(dir));
}

/**
 * The id of the owner holding `dir`, or null.
 *
 * @param {string} dir
 * @returns {string|null}
 */
function ownerOf(dir) {
    const entry = owners.get(keyOf(dir));
    return entry ? entry.owner : null;
}

module.exports = { acquire, isOwned, ownerOf, _reset: () => owners.clear() };
