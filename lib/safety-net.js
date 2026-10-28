'use strict';

/**
 * Defence in depth against process crashes caused by kafkajs.
 *
 * Node-RED installs no 'unhandledRejection' handler, so with Node's default
 * --unhandled-rejections=throw any unhandled promise rejection terminates
 * Node-RED. Our code handles every promise it creates, but kafkajs has internal
 * fire-and-forget promises (e.g. the restart of a crashed consumer, src/consumer/runner.js).
 * This module installs ONE process-wide, ref-counted 'unhandledRejection'
 * listener while at least one broker config node wants it, and for a grace
 * period after the last one let go (RELEASE_GRACE_MS: client nodes are still
 * closing then). Only unhandled promise REJECTIONS are covered, not uncaught
 * exceptions.
 *
 * It is deliberately narrow:
 *  - a rejection is "ours" when its error, or anything it wraps, is a kafkajs
 *    error (see errors.isKafkaJsError) or an error of this palette (name
 *    KafkaNc*, or a stack through this package's lib/ or nodes/). Ours is logged
 *    as a warning and swallowed. A kafkajs error of another palette that uses
 *    kafkajs counts too: npm may share one kafkajs copy, so they cannot be told
 *    apart;
 *  - anything else is handed back to Node when we are the only listener (see
 *    handBack()), so Node applies its own --unhandled-rejections mode exactly as
 *    if this palette were not installed: same exit, same wrapping of non-Error
 *    reasons (ERR_UNHANDLED_REJECTION), same warnings and exit code. We never
 *    hide other nodes' bugs;
 *  - when other 'unhandledRejection' listeners exist, they own the decision;
 *  - with --unhandled-rejections=strict, Node raises the uncaught exception
 *    BEFORE any listener runs, so a listener cannot prevent anything: the net
 *    is not installed at all, and acquire() logs a warning saying so.
 */

const path = require('path');
const { flatten, describeError, isKafkaJsError } = require('./errors');
const { once } = require('./util');

const EVENT = 'unhandledRejection';
const DEDUPE_MS = 60000;
const MAX_DEDUPE_KEYS = 200;
// Our runtime code lives in lib/ and nodes/ (tests and examples are not ours to protect).
const OWN_DIRS = ['lib', 'nodes'].map((d) => path.resolve(__dirname, '..', d) + path.sep);
/**
 * How long the listener stays installed after the last release. Node-RED closes config nodes in parallel
 * with the client nodes that use them, so the broker releases its reference while producers and consumers
 * are still disconnecting (up to 10 s), and kafkajs requests abandoned at that point can still fail for up
 * to a request timeout (30 s by default). A redeploy acquires again long before this runs out.
 */
const RELEASE_GRACE_MS = 30000;
/** Modes in which Node's handling of a rejection depends on whether a listener exists. */
const HAND_BACK_MODES = new Set(['throw', 'warn-with-error-code']);

let refCount = 0;
let installed = false;
let currentRED = null;
let releaseGraceMs = RELEASE_GRACE_MS;
let graceTimer = null;
let handingBack = false;
let strictWarned = false;
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
 * win over NODE_OPTIONS; the last occurrence wins. Both '--flag=value' and
 * '--flag value' are understood.
 *
 * @returns {string}
 */
function unhandledRejectionsMode() {
    let mode = 'throw';
    try {
        const fromOptions = String(process.env.NODE_OPTIONS || '').split(/\s+/);
        const args = [...fromOptions, ...process.execArgv];
        for (let i = 0; i < args.length; i++) {
            const m = /^--unhandled-rejections(?:=(\S+))?$/.exec(args[i]);
            if (!m) continue;
            if (m[1] !== undefined) mode = m[1];
            else if (i + 1 < args.length) mode = args[++i];
        }
    } catch (_) {
        mode = 'throw';
    }
    return mode;
}

/**
 * The decision logic, separated from `process` so it can be unit tested.
 *
 * @param {*} reason
 * @param {number} listenerCount current number of 'unhandledRejection' listeners
 * @param {object} [RED] where to log (defaults to the RED of the latest acquire())
 * @param {string} [mode] Node's --unhandled-rejections mode (default: detected)
 * @returns {'swallowed'|'hand-back'|'ignored'}
 *   swallowed: ours, logged as a warning;
 *   hand-back: not ours, we are the only listener, and in this mode Node would have acted without a
 *     listener (crash, or warning + exit code): the caller must give the rejection back to Node;
 *   ignored: not ours, and Node's outcome does not depend on us (other listeners decide, or the mode
 *     warns anyway / does nothing).
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
        return 'swallowed';
    }
    return listenerCount === 1 && HAND_BACK_MODES.has(mode) ? 'hand-back' : 'ignored';
}

/**
 * Give a rejection that is not ours back to Node: remove our listener and reject a new promise with the
 * same reason. Node processes it right after this listener returns (in the same tick), finds no listener
 * and does exactly what it does without this palette: in 'throw' mode it raises the uncaught exception
 * (wrapping a non-Error reason in its own ERR_UNHANDLED_REJECTION error, with origin
 * 'unhandledRejection' for uncaughtException handlers such as Node-RED's, which then exits); in
 * 'warn-with-error-code' mode it prints its warnings and sets the exit code. The listener comes back in
 * the next event loop turn. A kafkajs rejection in that tiny window gets Node's default handling, which
 * can only happen while Node is already terminating (throw) or merely warning.
 */
function handBack(reason) {
    const restore = () => {
        handingBack = false;
        sync();
    };
    handingBack = true; // keeps sync() from installing the listener again before Node saw the rejection
    try {
        process.removeListener(EVENT, processListener);
        installed = false;
        Promise.reject(reason); // deliberately unhandled
    } catch (_) {
        // removeListener never throws in practice; nothing better to do here
    }
    try {
        const immediate = setImmediate(restore);
        if (immediate && typeof immediate.unref === 'function') immediate.unref();
    } catch (_) {
        restore();
    }
}

/** The listener actually installed on `process`. */
function processListener(reason) {
    let count = 1;
    try {
        count = process.listenerCount(EVENT);
    } catch (_) {
        count = 1;
    }
    if (handleRejection(reason, count) === 'hand-back') handBack(reason);
}

/** Install or remove the listener to match the references, the grace period and the Node mode. */
function sync() {
    const wanted = (refCount > 0 || graceTimer !== null) && !handingBack && unhandledRejectionsMode() !== 'strict';
    try {
        if (wanted && !installed) {
            process.on(EVENT, processListener);
            installed = true;
        } else if (!wanted && installed) {
            process.removeListener(EVENT, processListener);
            installed = false;
        }
    } catch (_) {
        // process listeners cannot really fail; keep the flag as it was
    }
    if (refCount === 0 && graceTimer === null) currentRED = null;
}

function cancelGrace() {
    if (graceTimer !== null) {
        clearTimeout(graceTimer);
        graceTimer = null;
    }
}

/**
 * Take a reference on the safety net; the first reference installs the
 * process listener. Returns an idempotent release function; after the last
 * release the listener stays for a grace period (see RELEASE_GRACE_MS), then
 * it is removed unless a new reference was taken.
 *
 * @param {object} RED used for RED.log.warn (the most recent RED is used)
 * @returns {() => void} release
 */
function acquire(RED) {
    if (RED) currentRED = RED;
    refCount++;
    cancelGrace();
    if (unhandledRejectionsMode() === 'strict' && !strictWarned) {
        strictWarned = true;
        try {
            const log = RED && RED.log;
            if (log && typeof log.warn === 'function') {
                log.warn('[kafkajs-nc] The crash safety net is not active: with --unhandled-rejections=strict, ' +
                    'Node.js stops the process before any listener can handle an unhandled promise rejection.');
            }
        } catch (_) {
            // logging is best effort
        }
    }
    sync();
    return once(() => {
        refCount = Math.max(0, refCount - 1);
        if (refCount > 0) return;
        cancelGrace();
        if (releaseGraceMs > 0) {
            graceTimer = setTimeout(() => {
                graceTimer = null;
                sync();
            }, releaseGraceMs);
            // never keep Node-RED alive just to remove a listener
            if (typeof graceTimer.unref === 'function') graceTimer.unref();
        }
        sync();
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

/**
 * Test hook: change the grace period after the last release (ms, 0 = remove at once).
 *
 * @param {number} ms
 * @returns {number} the previous value
 */
function setReleaseGrace(ms) {
    const previous = releaseGraceMs;
    releaseGraceMs = typeof ms === 'number' && Number.isFinite(ms) && ms >= 0 ? ms : RELEASE_GRACE_MS;
    if (releaseGraceMs === 0 && graceTimer !== null) {
        cancelGrace();
        sync();
    }
    return previous;
}

module.exports = {
    acquire,
    handleRejection,
    isPaletteError,
    unhandledRejectionsMode,
    isInstalled,
    references,
    RELEASE_GRACE_MS,
    _setReleaseGrace: setReleaseGrace,
    _processListener: processListener,
    _resetDedupe: () => lastLogged.clear()
};
