'use strict';

/**
 * Small, dependency-free helpers shared by every module of the palette.
 *
 * Everything in here is written so that it can NEVER throw: these helpers are
 * used inside timers, event handlers and close handlers, where an exception
 * would crash Node-RED (docs/ARCHITECTURE.md, Design goals).
 */

const fs = require('fs');
const os = require('os');
const path = require('path');

/** Does nothing. Handy as a `.catch(noop)` target. */
function noop() {}

// Node-RED replaces `${NAME}` (and the older `$(NAME)`) only when it is the WHOLE property value, with no
// spaces around it (@node-red/runtime lib/flows/util.js EnvVarPropertyRE), and leaves the text unchanged
// when NAME is not set. These are the same expressions.
const ENV_WHOLE_RE = /^\$\{(\S+)\}$|^\$\((\S+)\)$/;
const ENV_EMBEDDED_RE = /\$\{([^}]*)\}/;

/**
 * Find a `${NAME}` reference that Node-RED left in a setting. Never throws.
 *
 * - `whole: true`: the whole value is a reference, so Node-RED would have replaced it if NAME were
 *   set: the variable is not set.
 * - `whole: false`: the reference is part of a longer value. Node-RED never replaces such a reference,
 *   whatever the environment, so it is a configuration mistake, not a missing variable.
 *
 * @param {*} value the setting as the node received it (not trimmed)
 * @returns {{name: string, whole: boolean}|null} null when there is no reference (or value is not a string)
 */
function envReference(value) {
    if (typeof value !== 'string' || !value.includes('$')) return null;
    const whole = ENV_WHOLE_RE.exec(value);
    if (whole) return { name: whole[1] !== undefined ? whole[1] : whole[2], whole: true };
    const embedded = ENV_EMBEDDED_RE.exec(value);
    if (embedded) return { name: embedded[1] || '?', whole: false };
    return null;
}

/**
 * The problem text for a `${NAME}` reference left in a setting, or null when there is none. This is the
 * one rule (and wording) every node uses:
 *  - whole value: 'environment variable NAME is not set';
 *  - inside a longer value: '${NAME} is only replaced by Node-RED when it is the whole value'.
 * Never throws.
 *
 * @param {*} value
 * @param {{embedded?: boolean}} [options] `embedded: false` ignores references inside a longer value, for
 *   settings that may legitimately contain '${' (e.g. passwords)
 * @returns {string|null}
 */
function envProblem(value, options) {
    const ref = envReference(value);
    if (!ref) return null;
    if (ref.whole) return `environment variable ${ref.name} is not set`;
    if (options && options.embedded === false) return null;
    return `\${${ref.name}} is only replaced by Node-RED when it is the whole value`;
}

/**
 * Parse a number from user configuration (the Node-RED editor saves strings).
 * Only numbers, numeric strings and bigints are accepted; everything else
 * (including '', whitespace, null, undefined, NaN, Infinity, booleans, objects)
 * yields `def`. The parsed value is clamped to [min, max]. Never throws.
 *
 * @param {*} value
 * @param {*} def value returned when `value` is not a usable number (not clamped)
 * @param {{min?: number, max?: number}} [range]
 * @returns {number|*}
 */
function toNumber(value, def, range) {
    try {
        let n;
        if (typeof value === 'number') {
            n = value;
        } else if (typeof value === 'string') {
            if (value.trim() === '') return def;
            n = Number(value);
        } else if (typeof value === 'bigint') {
            n = Number(value);
        } else {
            return def;
        }
        if (!Number.isFinite(n)) return def;
        const min = range && typeof range.min === 'number' && !Number.isNaN(range.min) ? range.min : -Infinity;
        const max = range && typeof range.max === 'number' && !Number.isNaN(range.max) ? range.max : Infinity;
        if (n < min) n = min;
        if (n > max) n = max;
        return Object.is(n, -0) ? 0 : n;
    } catch (_) {
        return def;
    }
}

/**
 * Like {@link toNumber} but truncates to an integer (before clamping).
 *
 * @param {*} value
 * @param {*} def
 * @param {{min?: number, max?: number}} [range]
 * @returns {number|*}
 */
function toInt(value, def, range) {
    const n = toNumber(value, undefined);
    if (n === undefined) return def;
    return toNumber(Math.trunc(n), def, range);
}

const TRUE_STRINGS = new Set(['true', '1', 'on', 'yes']);
const FALSE_STRINGS = new Set(['false', '0', '', 'off', 'no']);

/**
 * Parse a boolean from user configuration. Strings are compared trimmed and
 * case-insensitively. Anything unrecognised yields `def`.
 *
 * @param {*} value
 * @param {*} def
 * @returns {boolean|*}
 */
function toBool(value, def) {
    if (value === true || value === 1) return true;
    if (value === false || value === 0) return false;
    if (typeof value === 'string') {
        const s = value.trim().toLowerCase();
        if (TRUE_STRINGS.has(s)) return true;
        if (FALSE_STRINGS.has(s)) return false;
    }
    return def;
}

/**
 * Return a function that calls `fn` at most once and then keeps returning the
 * result of that first call.
 *
 * @template {Function} T
 * @param {T} fn
 * @returns {T}
 */
function once(fn) {
    let called = false;
    let result;
    return function onceWrapper(...args) {
        if (called) return result;
        called = true;
        result = fn.apply(this, args);
        return result;
    };
}

/**
 * Register an event listener that can never throw into the emitter.
 *
 * kafkajs only catches *asynchronous* rejections of instrumentation-event
 * listeners; a listener that throws synchronously is called from socket
 * handlers and timers and crashes the process (src/instrumentation/emitter.js).
 * The wrapper try/catches `fn` and attaches a rejection handler to a returned
 * thenable; both kinds of failure go to `onError`, which is itself guarded.
 *
 * @param {{on: Function}} emitter a kafkajs client or any EventEmitter
 * @param {string} event
 * @param {Function} fn
 * @param {(err: *) => void} [onError]
 * @returns {() => void} remover (safe to call any number of times)
 */
function safeOn(emitter, event, fn, onError) {
    const report = (err) => {
        try {
            if (typeof onError === 'function') onError(err);
        } catch (_) {
            // The error reporter itself failed: there is nobody left to tell.
        }
    };
    const wrapped = function safeListener(...args) {
        try {
            const result = fn.apply(this, args);
            if (result && typeof result.then === 'function') {
                result.then(undefined, report);
            }
        } catch (err) {
            report(err);
        }
    };
    let remover = null;
    try {
        remover = emitter.on(event, wrapped);
    } catch (err) {
        report(err);
        return noop;
    }
    return once(() => {
        try {
            if (typeof remover === 'function') {
                // kafkajs returns a remover function from on()
                remover();
            } else if (emitter && typeof emitter.removeListener === 'function') {
                // a plain EventEmitter returns itself from on()
                emitter.removeListener(event, wrapped);
            }
        } catch (_) {
            // ignore: removing a listener must never fail a close handler
        }
    });
}

/**
 * Wait for `promise` but never longer than `ms`. Resolves with the promise's
 * value, or with `onTimeoutValue` when the time is up OR the promise rejects.
 * It NEVER rejects, always clears its timer, and the timer is unref()'d so it
 * cannot keep the process alive during shutdown.
 *
 * @template T, U
 * @param {Promise<T>|T} promise
 * @param {number} ms
 * @param {U} [onTimeoutValue]
 * @returns {Promise<T|U>}
 */
function withTimeout(promise, ms, onTimeoutValue) {
    return new Promise((resolve) => {
        let settled = false;
        let timer = null;
        const finish = (value) => {
            if (settled) return;
            settled = true;
            if (timer) clearTimeout(timer);
            resolve(value);
        };
        try {
            timer = setTimeout(() => finish(onTimeoutValue), toInt(ms, 0, { min: 0, max: 2147483647 }));
            if (timer && typeof timer.unref === 'function') timer.unref();
        } catch (_) {
            finish(onTimeoutValue);
            return;
        }
        try {
            Promise.resolve(promise).then(finish, () => finish(onTimeoutValue));
        } catch (_) {
            finish(onTimeoutValue);
        }
    });
}

/**
 * Turn an arbitrary string into a safe single path segment / identifier:
 * characters outside [A-Za-z0-9._-] become '_', leading dots become '_'
 * (so '.', '..' and hidden names cannot be produced), max 100 characters.
 * An empty result becomes 'default'.
 *
 * @param {*} s
 * @returns {string}
 */
function sanitizeName(s) {
    let str;
    try {
        str = s == null ? '' : String(s);
    } catch (_) {
        str = '';
    }
    const out = str
        .replace(/[^A-Za-z0-9._-]/g, '_')
        .replace(/^\.+/, (dots) => '_'.repeat(dots.length))
        .slice(0, 100);
    return out === '' ? 'default' : out;
}

/**
 * Resolve a directory from configuration. Absolute paths are normalised,
 * relative paths are resolved against `baseDir`, and a leading '~' expands to
 * the user's home directory. Never throws.
 *
 * @param {string} p
 * @param {string} [baseDir] defaults to the current working directory
 * @returns {string}
 */
function resolveDir(p, baseDir) {
    try {
        let dir = p == null ? '' : String(p).trim();
        if (dir === '~' || dir.startsWith('~/') || dir.startsWith('~\\')) {
            dir = path.join(os.homedir(), dir.slice(1));
        }
        if (path.isAbsolute(dir)) return path.normalize(dir);
        const base = baseDir == null || String(baseDir) === '' ? process.cwd() : String(baseDir);
        return path.resolve(base, dir);
    } catch (_) {
        return process.cwd();
    }
}

/**
 * Convert to string and shorten to at most `n` characters, using '…' as the
 * last character when shortened. Never splits a surrogate pair.
 *
 * @param {*} s
 * @param {number} n
 * @returns {string}
 */
function truncate(s, n) {
    let str;
    try {
        str = typeof s === 'string' ? s : String(s);
    } catch (_) {
        str = '';
    }
    const max = toInt(n, 0, { min: 0 });
    if (str.length <= max) return str;
    if (max === 0) return '';
    let cut = max - 1;
    const code = str.charCodeAt(cut - 1);
    if (code >= 0xd800 && code <= 0xdbff) cut -= 1; // do not leave a lone high surrogate
    return str.slice(0, Math.max(0, cut)) + '…';
}

/**
 * A delay for status texts and log lines: '0.1s', '8s', '60s', '5m'.
 * Below 10 s one decimal is shown, so short test/backoff delays stay readable.
 * Never throws.
 *
 * @param {number} ms
 * @returns {string}
 */
function formatDelay(ms) {
    const s = Math.max(0, toNumber(ms, 0)) / 1000;
    if (s < 10) return `${Math.round(s * 10) / 10}s`;
    if (s < 120) return `${Math.round(s)}s`;
    return `${Math.round(s / 60)}m`;
}

/** Whether dir holds a Node-RED user directory (the check Node-RED itself makes). Never throws. */
function hasNodeRedConfig(dir) {
    try {
        return fs.existsSync(path.join(dir, '.config.json')) || fs.existsSync(path.join(dir, '.config.nodes.json'));
    } catch (_) {
        return false;
    }
}

/**
 * The Node-RED user directory. `RED.settings.userDir` is undefined in the test
 * helper and with a custom storage module (only the default local file system
 * storage fills it in), hence the fallbacks. They are the ones Node-RED's
 * storage and context store use: NODE_RED_HOME only when it holds a Node-RED
 * configuration (red.js always sets it to the node-red INSTALL directory, where
 * a queue would be lost on the next upgrade), else ~/.node-red. Never throws.
 *
 * @param {object} RED
 * @returns {string}
 */
function userDir(RED) {
    try {
        const dir = RED && RED.settings && RED.settings.userDir;
        if (typeof dir === 'string' && dir !== '') return dir;
    } catch (_) {
        // fall through to the defaults
    }
    try {
        const env = process.env;
        if (env.NODE_RED_HOME && hasNodeRedConfig(env.NODE_RED_HOME)) return env.NODE_RED_HOME;
        if (env.HOMEPATH && hasNodeRedConfig(path.join(env.HOMEPATH, '.node-red'))) {
            return path.join(env.HOMEPATH, '.node-red');
        }
        return path.join(env.HOME || env.USERPROFILE || env.HOMEPATH || os.homedir(), '.node-red');
    } catch (_) {
        return path.resolve('.node-red');
    }
}

module.exports = {
    toInt,
    toNumber,
    toBool,
    safeOn,
    once,
    withTimeout,
    noop,
    sanitizeName,
    resolveDir,
    truncate,
    formatDelay,
    userDir,
    envReference,
    envProblem
};
