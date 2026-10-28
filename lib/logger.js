'use strict';

/**
 * Bridge kafkajs logging into the Node-RED log.
 *
 * kafkajs calls the log function synchronously from socket handlers and
 * timers, so it must NEVER throw (an exception there crashes Node-RED; the
 * default kafkajs console logger even throws on circular extras). kafkajs is
 * also very chatty during outages (two ERROR lines per failed connection
 * attempt), so identical lines are de-duplicated.
 *
 * The de-duplication state is shared by all log creators of the process
 * (keyed by level + text, and the text contains the node label). The nodes
 * create a new kafkajs client - and therefore a new log creator - for every
 * reconnect attempt, so per-creator state would never suppress anything.
 */

const { truncate, toInt } = require('./util');

const LEVELS = Object.freeze({ NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 });
const MAX_KEYS = 500;
const MAX_TEXT = 2000;

/** level + '\0' + text → { last: ms, suppressed: number } (insertion order = age) */
const recent = new Map();

/**
 * Map a log level name from the broker config to the kafkajs numeric level.
 * 'off'|'nothing' → NOTHING, 'error' → ERROR, 'warn' → WARN, 'info' → INFO,
 * 'debug' → DEBUG; numeric levels pass through; anything else → WARN.
 * kafkajs needs a NUMBER: a string level disables its filtering entirely.
 *
 * @param {*} name
 * @param {{logLevel?: object}} [kafkajs] the kafkajs module (for its constants)
 * @returns {number}
 */
function toKafkaLogLevel(name, kafkajs) {
    let levels = LEVELS;
    try {
        const l = kafkajs && kafkajs.logLevel;
        if (l && typeof l.WARN === 'number') levels = l;
    } catch (_) {
        // use our copy of the constants
    }
    try {
        if (typeof name === 'number' && Object.values(LEVELS).includes(name)) return name;
        const s = String(name == null ? '' : name).trim().toLowerCase();
        switch (s) {
            case 'off':
            case 'nothing':
            case 'none':
            case '0':
                return levels.NOTHING;
            case 'error':
            case '1':
                return levels.ERROR;
            case 'info':
            case '4':
                return levels.INFO;
            case 'debug':
            case '5':
                return levels.DEBUG;
            default:
                return levels.WARN;
        }
    } catch (_) {
        return LEVELS.WARN;
    }
}

/** Render any value as a short single-line string without throwing. */
function render(v) {
    try {
        if (typeof v === 'string') return v;
        if (v instanceof Error || (v && typeof v === 'object' && typeof v.message === 'string')) return v.message;
        if (v && typeof v === 'object') {
            const seen = new WeakSet();
            return JSON.stringify(v, (k, val) => {
                if (val && typeof val === 'object') {
                    if (seen.has(val)) return '[Circular]';
                    seen.add(val);
                }
                return typeof val === 'bigint' ? val.toString() : val;
            });
        }
        return String(v);
    } catch (_) {
        return '[unprintable]';
    }
}

/**
 * Format a kafkajs log entry (exported for tests).
 *
 * @param {{namespace?: string, label?: string, log?: object}} entry
 * @param {string} label our node label, e.g. '[kafkajs-nc-producer:my producer]'
 * @returns {string}
 */
function formatEntry(entry, label) {
    const namespace = entry && entry.namespace ? render(entry.namespace) : '';
    const log = (entry && entry.log) || {};
    const message = render(log.message);
    let text = `[kafkajs${namespace ? ':' + namespace : ''}]${label ? ' ' + label : ''} ${message}`;
    if (log.broker) text += ` (broker ${render(log.broker)})`;
    if (log.error) {
        const error = render(log.error);
        if (error && error !== message) text += `: ${error}`;
    }
    return truncate(text, MAX_TEXT);
}

/**
 * Decide whether a line is printed now. Returns the text to print (with a
 * "(repeated N times)" suffix when lines were suppressed) or null.
 */
function dedupe(level, text, dedupeMs, now) {
    const key = level + '\u0000' + text;
    const prev = recent.get(key);
    if (prev && now - prev.last < dedupeMs) {
        prev.suppressed++;
        return null;
    }
    const suppressed = prev ? prev.suppressed : 0;
    recent.delete(key); // re-insert so Map order stays "oldest first"
    if (recent.size >= MAX_KEYS) recent.delete(recent.keys().next().value);
    recent.set(key, { last: now, suppressed: 0 });
    return suppressed > 0 ? `${text} (repeated ${suppressed} times)` : text;
}

/**
 * Create a kafkajs `logCreator` that writes to RED.log.
 * kafkajs ERROR and WARN → RED.log.warn (kafkajs ERRORs are mostly transient
 * connection noise; our nodes report real failures themselves), INFO →
 * RED.log.info, DEBUG → RED.log.debug. Identical lines within `dedupeMs` are
 * suppressed. Never throws.
 *
 * @param {object} RED
 * @param {string} label prefix identifying the node, e.g. '[kafkajs-nc-producer:my producer]'
 * @param {{dedupeMs?: number}} [options]
 * @returns {(level: number) => (entry: object) => void}
 */
function createLogCreator(RED, label, options) {
    const dedupeMs = toInt(options && options.dedupeMs, 60000, { min: 0 });
    let safeLabel = '';
    try {
        safeLabel = label == null ? '' : String(label);
    } catch (_) {
        safeLabel = '';
    }
    return function logCreator(/* level: kafkajs pre-filters by level itself */) {
        return function logEntry(entry) {
            try {
                const level = entry && entry.level;
                if (typeof level !== 'number' || level <= LEVELS.NOTHING) return;
                const text = dedupe(level, formatEntry(entry, safeLabel), dedupeMs, Date.now());
                if (text === null) return;
                const log = RED && RED.log;
                if (!log) return;
                let fn;
                if (level <= LEVELS.WARN) fn = log.warn;
                else if (level === LEVELS.INFO) fn = log.info;
                else fn = log.debug;
                if (typeof fn === 'function') fn.call(log, text);
            } catch (_) {
                // never throw into kafkajs internals (socket handlers, timers)
            }
        };
    };
}

/** Forget de-duplication state (tests). */
function _resetDedupe() {
    recent.clear();
}

module.exports = {
    LEVELS,
    toKafkaLogLevel,
    createLogCreator,
    formatEntry,
    _resetDedupe
};
