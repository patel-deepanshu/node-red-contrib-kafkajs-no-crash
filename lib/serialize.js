'use strict';

/**
 * Conversions between Node-RED message values and kafkajs messages, plus a
 * JSON-safe encoding of kafkajs messages for the offline queue.
 *
 * kafkajs only accepts string | Buffer | null for keys, values and header
 * values; anything else becomes a TypeError deep inside send() (and a number
 * header value even breaks the whole batch). We convert or reject early with a
 * MessageError so a bad message is reported against the msg that carried it
 * and is never queued.
 */

const { MessageError } = require('./errors');

const MAX_INT32 = 2147483647;
const MAX_INT64 = 9223372036854775807n;

/** Set a property even when the key is '__proto__' (no prototype mutation). */
function setOwn(obj, key, value) {
    if (key === '__proto__') {
        Object.defineProperty(obj, key, { value, enumerable: true, writable: true, configurable: true });
    } else {
        obj[key] = value;
    }
}

/** Buffer view of any ArrayBuffer / typed array / DataView (no copy). */
function viewToBuffer(v) {
    if (v instanceof ArrayBuffer) return Buffer.from(v);
    return Buffer.from(v.buffer, v.byteOffset, v.byteLength);
}

function isBinary(v) {
    return v instanceof ArrayBuffer || ArrayBuffer.isView(v);
}

function dateToIso(d, what) {
    const t = d.getTime();
    if (!Number.isFinite(t)) throw new MessageError(`${what} is an invalid Date`);
    return d.toISOString();
}

function stringify(v, what) {
    let json;
    try {
        json = JSON.stringify(v);
    } catch (err) {
        throw new MessageError(`${what} cannot be converted to JSON: ${err && err.message}`);
    }
    if (typeof json !== 'string') throw new MessageError(`${what} cannot be converted to JSON`);
    return json;
}

/**
 * Convert a scalar-ish value to string | Buffer, or throw MessageError.
 * Shared by keys, values and header values.
 */
function convert(v, what) {
    if (typeof v === 'string') return v;
    if (Buffer.isBuffer(v)) return v;
    if (isBinary(v)) return viewToBuffer(v);
    switch (typeof v) {
        case 'number':
        case 'boolean':
        case 'bigint':
            return String(v);
        case 'object':
            if (v instanceof Date) return dateToIso(v, what);
            return stringify(v, what);
        default:
            throw new MessageError(`${what} has an unsupported type (${typeof v})`);
    }
}

/**
 * msg.payload → kafkajs message value.
 * Buffer/string as is; null → null (tombstone); number/boolean/bigint →
 * String; Date → ISO string; typed arrays/ArrayBuffer → Buffer; objects and
 * arrays → JSON. Throws MessageError for undefined, functions, symbols and
 * values JSON cannot represent (circular objects).
 *
 * @param {*} payload
 * @returns {string|Buffer|null}
 */
function toKafkaValue(payload) {
    if (payload === undefined) throw new MessageError('msg.payload is undefined');
    if (payload === null) return null;
    return convert(payload, 'msg.payload');
}

/**
 * msg.key → kafkajs message key. undefined/null/'' → null (no key); otherwise
 * like {@link toKafkaValue}.
 *
 * @param {*} key
 * @returns {string|Buffer|null}
 */
function toKafkaKey(key) {
    if (key === undefined || key === null || key === '') return null;
    return convert(key, 'msg.key');
}

/**
 * msg.headers → kafkajs headers object. undefined/null → undefined. Header
 * values: null/undefined are dropped, Buffer/string kept, numbers/booleans/
 * bigints → String, Date → ISO string, arrays → arrays of converted elements
 * (null elements dropped; an array left empty drops the header), objects →
 * JSON. Accepts plain objects and Maps.
 *
 * @param {*} h
 * @returns {Object<string, string|Buffer|Array<string|Buffer>>|undefined}
 */
function toKafkaHeaders(h) {
    if (h === undefined || h === null) return undefined;
    if (typeof h !== 'object' || Array.isArray(h) || Buffer.isBuffer(h) || isBinary(h)) {
        throw new MessageError('msg.headers must be an object');
    }
    const out = {};
    try {
        const entries = h instanceof Map ? Array.from(h.entries()) : Object.entries(h);
        for (const [rawKey, value] of entries) {
            const key = String(rawKey);
            if (value === null || value === undefined) continue;
            const what = `msg.headers["${key}"]`;
            if (Array.isArray(value)) {
                const list = [];
                for (const item of value) {
                    if (item === null || item === undefined) continue;
                    list.push(convert(item, what));
                }
                if (list.length > 0) setOwn(out, key, list);
            } else {
                setOwn(out, key, convert(value, what));
            }
        }
    } catch (err) {
        // e.g. a throwing getter or a Symbol key: report it as a message problem
        if (err instanceof MessageError) throw err;
        throw new MessageError(`msg.headers cannot be converted: ${err && err.message}`);
    }
    return out;
}

/**
 * msg.partition → kafkajs partition. undefined/null/'' → undefined (let the
 * partitioner decide); integer 0..2^31-1 (number, bigint or numeric string) →
 * number; anything else throws MessageError.
 *
 * @param {*} p
 * @returns {number|undefined}
 */
function toKafkaPartition(p) {
    if (p === undefined || p === null || p === '') return undefined;
    let n = NaN;
    if (typeof p === 'number') n = p;
    else if (typeof p === 'bigint') n = p <= BigInt(MAX_INT32) && p >= 0n ? Number(p) : NaN;
    else if (typeof p === 'string' && /^\s*\d+\s*$/.test(p)) n = Number(p);
    if (!Number.isInteger(n) || n < 0 || n > MAX_INT32) {
        throw new MessageError(`msg.partition must be an integer >= 0 (got ${describeValue(p)})`);
    }
    return n === 0 ? 0 : n; // normalise -0
}

/**
 * msg.timestamp → kafkajs timestamp (string of epoch milliseconds).
 * undefined/null/'' → undefined (kafkajs uses "now"); Date → its time; finite
 * number → truncated; digit string → as is. Negative or non-numeric values
 * throw MessageError (kafkajs would fail with a BigInt SyntaxError).
 *
 * @param {*} t
 * @returns {string|undefined}
 */
function toKafkaTimestamp(t) {
    if (t === undefined || t === null || t === '') return undefined;
    const bad = () => new MessageError(`msg.timestamp must be a Date or epoch milliseconds (got ${describeValue(t)})`);
    if (t instanceof Date) {
        const ms = t.getTime();
        if (!Number.isFinite(ms) || ms < 0) throw bad();
        return String(ms);
    }
    if (typeof t === 'number') {
        if (!Number.isFinite(t) || t < 0 || t > Number.MAX_SAFE_INTEGER) throw bad();
        return String(Math.trunc(t));
    }
    if (typeof t === 'bigint') {
        if (t < 0n || t > MAX_INT64) throw bad();
        return t.toString();
    }
    if (typeof t === 'string') {
        const s = t.trim();
        if (!/^\d{1,19}$/.test(s) || BigInt(s) > MAX_INT64) throw bad();
        return s;
    }
    throw bad();
}

function describeValue(v) {
    try {
        if (typeof v === 'string') return JSON.stringify(v.length > 40 ? v.slice(0, 40) + '…' : v);
        if (typeof v === 'object' && v !== null) return Array.isArray(v) ? 'array' : 'object';
        return String(v);
    } catch (_) {
        return typeof v;
    }
}

function headerValueOut(v, asBuffer) {
    if (v === null || v === undefined) return null;
    if (Buffer.isBuffer(v)) return asBuffer ? v : v.toString('utf8');
    if (isBinary(v)) {
        const b = viewToBuffer(v);
        return asBuffer ? b : b.toString('utf8');
    }
    if (typeof v === 'string') return asBuffer ? Buffer.from(v, 'utf8') : v;
    return v;
}

/**
 * kafkajs (consumer) headers → msg.headers. Buffer values become UTF-8 strings
 * unless `asBuffer`; null stays null; array values are mapped element-wise.
 * Always returns a new object ({} for undefined or invalid input).
 *
 * @param {*} h
 * @param {{asBuffer?: boolean}} [options]
 * @returns {object}
 */
function fromKafkaHeaders(h, options) {
    const asBuffer = !!(options && options.asBuffer);
    const out = {};
    if (!h || typeof h !== 'object' || Array.isArray(h)) return out;
    for (const key of Object.keys(h)) {
        const v = h[key];
        setOwn(out, key, Array.isArray(v) ? v.map((x) => headerValueOut(x, asBuffer)) : headerValueOut(v, asBuffer));
    }
    return out;
}

/**
 * Decode a consumed key/value. format: 'string' (default) | 'buffer' | 'json'.
 * null/undefined → null. 'json' parses UTF-8 text and THROWS SyntaxError on
 * invalid JSON (the caller reports it against the message).
 *
 * @param {Buffer|string|null|undefined} buf
 * @param {string} [format]
 * @returns {*}
 */
function decodeValue(buf, format) {
    if (buf === null || buf === undefined) return null;
    let b = buf;
    if (!Buffer.isBuffer(b)) {
        if (isBinary(b)) b = viewToBuffer(b);
        else if (typeof b !== 'string') b = String(b);
    }
    if (format === 'buffer') return Buffer.isBuffer(b) ? b : Buffer.from(b, 'utf8');
    const text = Buffer.isBuffer(b) ? b.toString('utf8') : b;
    if (format === 'json') return JSON.parse(text);
    return text;
}

/**
 * string | Buffer | null → JSON-safe form for the queue:
 * null → null, Buffer/typed array → { b: base64 }, string → { s: string }.
 * undefined → undefined (callers omit it).
 *
 * @param {*} v
 * @returns {null|{b: string}|{s: string}|undefined}
 */
function encodeBytes(v) {
    if (v === undefined) return undefined;
    if (v === null) return null;
    if (Buffer.isBuffer(v)) return { b: v.toString('base64') };
    if (isBinary(v)) return { b: viewToBuffer(v).toString('base64') };
    return { s: typeof v === 'string' ? v : String(v) };
}

/**
 * Inverse of {@link encodeBytes}. Also tolerates raw strings and Node's JSON
 * form of a Buffer ({ type: 'Buffer', data: [...] }).
 *
 * @param {*} e
 * @returns {string|Buffer|null|undefined}
 */
function decodeBytes(e) {
    if (e === undefined) return undefined;
    if (e === null) return null;
    if (typeof e === 'string') return e;
    if (typeof e === 'object') {
        if (typeof e.b === 'string') return Buffer.from(e.b, 'base64');
        if (typeof e.s === 'string') return e.s;
        if (e.type === 'Buffer' && Array.isArray(e.data)) return Buffer.from(e.data);
    }
    throw new MessageError('Corrupt queued message: invalid encoded bytes');
}

/**
 * kafkajs message { key, value, headers, partition, timestamp } → compact
 * JSON-safe record { k, v, h, p, ts } (undefined fields omitted).
 *
 * @param {object} m
 * @returns {object}
 */
function encodeMessage(m) {
    if (!m || typeof m !== 'object') throw new MessageError('Invalid Kafka message');
    const e = {};
    if (m.key !== undefined) e.k = encodeBytes(m.key);
    if (m.value !== undefined) e.v = encodeBytes(m.value);
    if (m.headers && typeof m.headers === 'object') {
        const h = {};
        for (const key of Object.keys(m.headers)) {
            const hv = m.headers[key];
            if (hv === undefined) continue;
            setOwn(h, key, Array.isArray(hv) ? hv.map(encodeBytes) : encodeBytes(hv));
        }
        e.h = h;
    }
    if (m.partition !== undefined && m.partition !== null) e.p = m.partition;
    if (m.timestamp !== undefined && m.timestamp !== null) e.ts = String(m.timestamp);
    return e;
}

/**
 * Inverse of {@link encodeMessage}. Throws MessageError for corrupt input.
 *
 * @param {object} e
 * @returns {{key?: *, value?: *, headers?: object, partition?: number, timestamp?: string}}
 */
function decodeMessage(e) {
    if (!e || typeof e !== 'object' || Array.isArray(e)) {
        throw new MessageError('Corrupt queued message: not an object');
    }
    const m = {};
    if (e.k !== undefined) m.key = decodeBytes(e.k);
    if (e.v !== undefined) m.value = decodeBytes(e.v);
    if (e.h && typeof e.h === 'object') {
        const h = {};
        for (const key of Object.keys(e.h)) {
            const hv = e.h[key];
            if (hv === undefined) continue;
            setOwn(h, key, Array.isArray(hv) ? hv.map(decodeBytes) : decodeBytes(hv));
        }
        m.headers = h;
    }
    if (e.p !== undefined && e.p !== null) m.partition = e.p;
    if (e.ts !== undefined && e.ts !== null) m.timestamp = String(e.ts);
    return m;
}

module.exports = {
    toKafkaValue,
    toKafkaKey,
    toKafkaHeaders,
    toKafkaPartition,
    toKafkaTimestamp,
    fromKafkaHeaders,
    decodeValue,
    encodeBytes,
    decodeBytes,
    encodeMessage,
    decodeMessage
};
