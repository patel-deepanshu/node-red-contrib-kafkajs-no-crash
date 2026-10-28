'use strict';

/**
 * Defence in depth against process crashes caused by kafkajs.
 *
 * Node-RED installs no 'unhandledRejection' handler, so on Node >= 15 any
 * unhandled rejection terminates Node-RED. Our code handles every promise it
 * creates, but kafkajs has internal fire-and-forget promises (e.g. consumer
 * restarts, 5-kafkajs-internals.md §4). This module installs ONE process-wide,
 * ref-counted 'unhandledRejection' listener while at least one broker config
 * node wants it.
 *
 * It is deliberately narrow:
 *  - rejections caused by kafkajs (or by this palette) are logged as a warning
 *    and swallowed;
 *  - anything else is re-thrown when we are the only listener, which turns it
 *    into an uncaught exception exactly like Node's default behaviour (Node-RED
 *    then logs it and exits as it always did) - we never hide other nodes' bugs;
 *  - when other 'unhandledRejection' listeners exist, they own the decision.
 */

const path = require('path');
const { flatten, describeError, isKafkaJsError } = require('./errors');
const { once } = require('./util');

const EVENT = 'unhandledRejection';
const DEDUPE_MS = 60000;
const MAX_DEDUPE_KEYS = 200;
// Our runtime code lives in lib/ and nodes/ (tests and examples are not ours to protect).
const OWN_DIRS = ['lib', 'nodes'].map((d) => path.resolve(__dirname, '..', d) + path.sep);

let refCount = 0;
let installed = false;
let currentRED = null;
const lastLogged = new Map(); // text → timestamp

/**
 * True for errors from kafkajs, for this palette's own error classes, and for
 * errors whose stack runs through this package's files (a bug of ours must
 * not take Node-RED down either). Never throws.
 *
 * @param {*} reason
 * @returns {boolean}
 */
function isPaletteError(reason) {
    try {
        if (isKafkaJsError(reason)) return true;
        return flatten(reason).some((e) => {
            const name = e && e.name;
            if (typeof name === 'string' && name.startsWith('KafkaNc')) return true;
            const stack = e && e.stack;
            return typeof stack === 'string' && OWN_DIRS.some((dir) => stack.includes(dir));
        });
    } catch (_) {
        return false;
    }
}

function warnOnce(text, RED) {
    const now = Date.now();
    const last = lastLogged.get(text);
    if (last !== undefined && now - last < DEDUPE_MS) return;
    lastLogged.delete(text);
    if (lastLogged.size >= MAX_DEDUPE_KEYS) lastLogged.delete(lastLogged.keys().next().value);
    lastLogged.set(text, now);
    const log = RED && RED.log;
    if (log && typeof log.warn === 'function') log.warn(text);
}

/**
 * Node's --unhandled-rejections mode ('throw' when not set). Command-line flags
 * win over NODE_OPTIONS; the last occurrence wins.
 *
 * @returns {string}
 */
function unhandledRejectionsMode() {
    let mode = 'throw';
    try {
        const fromOptions = String(process.env.NODE_OPTIONS || '').split(/\s+/);
        for (const arg of [...fromOptions, ...process.execArgv]) {
            const m = /^--unhandled-rejections=(\S+)$/.exec(arg);
            if (m) mode = m[1];
        }
    } catch (_) {
        mode = 'throw';
    }
    return mode;
}

/** Modes in which Node itself terminates on an unhandled rejection. */
const CRASHING_MODES = new Set(['throw', 'strict']);

/**
 * The decision logic, separated from `process` so it can be unit tested.
 * Throws `reason` (deliberately) only when it is not ours, `listenerCount` is
 * 1 (we are the only listener) and Node would have crashed without us.
 *
 * @param {*} reason
 * @param {number} listenerCount current number of 'unhandledRejection' listeners
 * @param {object} [RED] where to log (defaults to the RED of the latest acquire())
 * @param {string} [mode] Node's --unhandled-rejections mode (default: detected)
 */
function handleRejection(reason, listenerCount, RED = currentRED, mode = unhandledRejectionsMode()) {
    let ours = false;
    try {
        ours = isPaletteError(reason);
    } catch (_) {
        ours = false;
    }
    if (ours) {
        try {
            const what = isKafkaJsError(reason) ? 'unhandled KafkaJS error' : 'unhandled error in kafkajs-nc nodes';
            warnOnce(`[kafkajs-nc] Prevented a crash: ${what}: ${describeError(reason)}`, RED);
        } catch (_) {
            // logging is best effort
        }
        return;
    }
    if (listenerCount === 1 && CRASHING_MODES.has(mode)) {
        // Only us: preserve Node's default "crash on unhandled rejection"
        // (Node-RED's uncaughtException handler logs it and exits as before).
        throw reason;
    }
    // Someone else listens too (they decide), or Node runs in a mode that
    // would only have warned (--unhandled-rejections=warn|none|...).
}

/** The listener actually installed on `process`. */
function processListener(reason) {
    let count = 1;
    try {
        count = process.listenerCount(EVENT);
    } catch (_) {
        count = 1;
    }
    handleRejection(reason, count);
}

/**
 * Take a reference on the safety net; the first reference installs the
 * process listener. Returns an idempotent release function; the last release
 * removes the listener.
 *
 * @param {object} RED used for RED.log.warn (the most recent RED is used)
 * @returns {() => void} release
 */
function acquire(RED) {
    if (RED) currentRED = RED;
    refCount++;
    if (!installed) {
        try {
            process.on(EVENT, processListener);
            installed = true;
        } catch (_) {
            installed = false;
        }
    }
    return once(() => {
        refCount = Math.max(0, refCount - 1);
        if (refCount === 0) {
            try {
                process.removeListener(EVENT, processListener);
            } catch (_) {
                // ignore
            }
            installed = false;
            currentRED = null;
        }
    });
}

/** Whether the process listener is currently installed. */
function isInstalled() {
    return installed;
}

/** Current number of unreleased acquire() calls. */
function references() {
    return refCount;
}

module.exports = {
    acquire,
    handleRejection,
    isPaletteError,
    unhandledRejectionsMode,
    isInstalled,
    references,
    _processListener: processListener,
    _resetDedupe: () => lastLogged.clear()
};
