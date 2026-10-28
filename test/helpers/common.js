'use strict';

/**
 * Small helpers shared by every test file.
 *
 * - tmpDir() / cleanupTmpDirs(): throw-away directories under os.tmpdir(),
 *   removed after the run (setup.js calls cleanupTmpDirs from its root
 *   afterAll hook; a process 'exit' fallback covers runs without setup.js).
 * - sleep(), waitFor(): polling helpers with clear timeout messages.
 * - nextStatus(), statusCalls(), statusMark(): inspect node.status() calls.
 *   With node-red-node-test-helper, node.status is a sinon spy installed on
 *   Node.prototype, i.e. shared by ALL nodes; these helpers filter the calls
 *   by `this` so each node only sees its own statuses.
 */

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const createdDirs = new Set();

/**
 * Create a fresh empty directory under os.tmpdir(). It is removed by
 * cleanupTmpDirs() at the end of the run.
 *
 * @param {string} [prefix='kafkajs-nc-test-']
 * @returns {string} absolute path
 */
function tmpDir(prefix = 'kafkajs-nc-test-') {
    const safePrefix = String(prefix).replace(/[^A-Za-z0-9._-]/g, '_') || 'kafkajs-nc-test-';
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), safePrefix));
    createdDirs.add(dir);
    return dir;
}

/**
 * Remove every directory created by tmpDir(). Never throws.
 *
 * @returns {{removed: number, failed: Array<{dir: string, error: Error}>}}
 */
function cleanupTmpDirs() {
    let removed = 0;
    const failed = [];
    for (const dir of createdDirs) {
        try {
            fs.rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
            removed++;
        } catch (error) {
            failed.push({ dir, error });
        }
        createdDirs.delete(dir);
    }
    return { removed, failed };
}

// Fallback for runs that do not load setup.js (e.g. `mocha --no-config file`).
// fs.rmSync is synchronous, so it is allowed inside an 'exit' listener.
process.once('exit', () => {
    cleanupTmpDirs();
});

/**
 * Resolve after `ms` milliseconds.
 *
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeValue(v) {
    if (v instanceof Error) return `${v.name}: ${v.message}`;
    try {
        return JSON.stringify(v);
    } catch (_) {
        return String(v);
    }
}

/**
 * Poll `fn` until it returns (or resolves to) a truthy value, and resolve with
 * that value. A throw/rejection from `fn` counts as "not yet" (so assertions
 * can be polled); the last one is reported as `cause` of the timeout error.
 * The returned promise always settles within `timeout` ms, even if `fn`
 * returns a promise that never settles.
 *
 * @template T
 * @param {() => T | Promise<T>} fn
 * @param {{timeout?: number, interval?: number, message?: string}} [options]
 * @returns {Promise<T>}
 */
function waitFor(fn, { timeout = 3000, interval = 10, message } = {}) {
    return new Promise((resolve, reject) => {
        let settled = false;
        let lastError = null;
        let attempts = 0;
        let pollTimer = null;

        const finish = (err, value) => {
            if (settled) return;
            settled = true;
            clearTimeout(deadline);
            clearTimeout(pollTimer);
            if (err) reject(err);
            else resolve(value);
        };

        const deadline = setTimeout(() => {
            let text = `waitFor timed out after ${timeout} ms (${attempts} attempts)`;
            if (message) text += `: ${message}`;
            if (lastError) text += ` — last error: ${describeValue(lastError)}`;
            const err = new Error(text);
            if (lastError) err.cause = lastError;
            finish(err);
        }, timeout);

        const schedule = () => {
            if (!settled) pollTimer = setTimeout(attempt, interval);
        };

        function attempt() {
            if (settled) return;
            attempts++;
            let result;
            try {
                result = fn();
            } catch (e) {
                lastError = e;
                schedule();
                return;
            }
            Promise.resolve(result).then(
                (value) => {
                    if (value) finish(null, value);
                    else schedule();
                },
                (e) => {
                    lastError = e;
                    schedule();
                }
            );
        }

        attempt();
    });
}

/** node.status() accepts a string/number/boolean as shorthand for {text}. */
function normaliseStatus(arg) {
    if (arg !== null && typeof arg === 'object') return arg;
    if (arg === undefined) return {};
    return { text: String(arg) };
}

const OWN_RECORDER = Symbol('kafkajs-nc-test:status-recorder');

/**
 * Install a lightweight recorder on a node whose `status` is not a spy (plain
 * objects in unit tests). Only calls made after installation are recorded.
 */
function ensureRecorder(node) {
    const current = node.status;
    if (current && (Array.isArray(current.args) || current[OWN_RECORDER])) return;
    const original = typeof current === 'function' ? current : null;
    const recorder = function (...args) {
        recorder.args.push(args);
        recorder.thisValues.push(this);
        return original ? original.apply(this, args) : undefined;
    };
    recorder.args = [];
    recorder.thisValues = [];
    recorder[OWN_RECORDER] = true;
    node.status = recorder;
}

/**
 * Every status object passed to `node.status()` so far, oldest first
 * (strings are normalised to `{text}`).
 *
 * @param {object} node a Node-RED node (or any object with a `status` method)
 * @returns {object[]}
 */
function statusCalls(node) {
    ensureRecorder(node);
    const spy = node.status;
    const args = spy.args || [];
    const thisValues = Array.isArray(spy.thisValues) ? spy.thisValues : null;
    const out = [];
    for (let i = 0; i < args.length; i++) {
        // the test helper's spy lives on Node.prototype and records calls of every node
        if (thisValues && thisValues[i] !== node) continue;
        out.push(normaliseStatus(args[i][0]));
    }
    return out;
}

/**
 * The number of status calls `node` has made so far. Pass it as `since` to
 * nextStatus() to only consider statuses set after this point.
 *
 * @param {object} node
 * @returns {number}
 */
function statusMark(node) {
    return statusCalls(node).length;
}

/**
 * Wait until the status of `node` matches `pred` and resolve with that status
 * object.
 *
 * By default the node's CURRENT status (its most recent call) and every later
 * one are considered — i.e. "wait until the status is X". Options:
 * - `since: statusMark(node)` (taken before triggering something): only
 *   statuses set after that point count (strictly "the next one").
 * - `since: 0`: the whole history counts.
 *
 * @param {object} node
 * @param {(status: object) => boolean} pred
 * @param {number | {timeout?: number, since?: number}} [options] timeout in ms (default 3000) or options
 * @returns {Promise<object>}
 */
function nextStatus(node, pred, options) {
    const opts = typeof options === 'number' ? { timeout: options } : (options || {});
    const timeout = opts.timeout === undefined ? 3000 : opts.timeout;
    const initial = statusCalls(node).length;
    const since = opts.since === undefined ? Math.max(0, initial - 1) : opts.since;

    const findMatch = () => {
        const calls = statusCalls(node);
        for (let i = since; i < calls.length; i++) {
            if (pred(calls[i])) return { status: calls[i] };
        }
        return null;
    };

    return waitFor(findMatch, { timeout, interval: 5 }).then(
        (found) => found.status,
        () => {
            const calls = statusCalls(node);
            const earlier = calls.slice(0, since).filter((s) => {
                try {
                    return pred(s);
                } catch (_) {
                    return false;
                }
            }).length;
            const seen = calls.slice(-10).map((s) => JSON.stringify(s)).join(', ');
            let text = `nextStatus: no status of node ${node && node.id} matched within ${timeout} ms ` +
                `(looked at calls #${since}..#${calls.length - 1}; last statuses: [${seen}])`;
            if (earlier > 0) {
                text += `. ${earlier} earlier status call(s) matched — pass { since: 0 } to include them`;
            }
            throw new Error(text);
        }
    );
}

module.exports = {
    tmpDir,
    cleanupTmpDirs,
    sleep,
    waitFor,
    nextStatus,
    statusCalls,
    statusMark
};
