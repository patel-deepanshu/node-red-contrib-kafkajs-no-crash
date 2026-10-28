'use strict';

/**
 * kafkajs-nc-producer: sends messages to Kafka and does not lose them while
 * Kafka is unreachable ("store-and-forward").
 *
 * How a message travels (the user-facing description is the help text in
 * kafka-producer.html):
 *
 *   input ─► build the Kafka message(s) synchronously
 *       │      invalid content ⇒ done(err); such a message is never queued
 *       ├─ "live" mode (connected and the offline queue is empty):
 *       │      in-memory pending list, ONE produce request in flight at a time
 *       │      ⇒ delivery report + done(). A connection-level failure moves the
 *       │      failed batch and everything behind it to the offline queue (in
 *       │      order) and reconnects.
 *       └─ "backlog" mode (not connected, or older messages still queued):
 *              append to the offline queue (disk or memory) ⇒ "queued" report +
 *              done(). Once connected, the flusher sends the queue oldest-first
 *              and switches back to live mode when it is empty, so the overall
 *              order of messages is kept.
 *
 * Crash safety (SPEC §0): the input and close handlers are not async, every
 * promise has a rejection handler, kafkajs listeners go through safeOn(),
 * every connect attempt carries a generation number (a connect that completes
 * after a redeploy is disconnected instead of becoming a zombie client), and
 * the close handler always calls done() within 10 s.
 */

const crypto = require('crypto');
const path = require('path');

const { getKafkajs } = require('../lib/kafka-factory');
const { Backoff } = require('../lib/backoff');
const { StatusReporter } = require('../lib/status');
const { DiskQueue } = require('../lib/disk-queue');
const { MemoryQueue } = require('../lib/memory-queue');
const queueRegistry = require('../lib/queue-registry');
const {
    MessageError, ErrorKind, classifyError, describeError, shortMessage, problemText, connectionKind, asConfigError
} = require('../lib/errors');
const serialize = require('../lib/serialize');
const {
    toInt, toNumber, toBool, safeOn, once, withTimeout, noop, sanitizeName, resolveDir, truncate, userDir,
    formatDelay: fmtDelay
} = require('../lib/util');

/**
 * Timeouts in ms. Exported as `module.exports.TIMEOUTS` so that tests can
 * shorten them; production code never changes them.
 */
const TIMEOUTS = {
    /** close: wait this long for an in-flight produce request or flush batch */
    closeSendWait: 5000,
    /** bound for producer.disconnect() (kafkajs can take requestTimeout during a hang) */
    disconnect: 3000,
    /** close: bound for persisting unsent messages and for closing the queue */
    closeQueue: 1500,
    /** close: done() is called after this long whatever happens (Node-RED gives up after 15 s) */
    closeTotal: 10000,
    /** startup: wait for an older instance of this node that is still closing the same queue */
    previousClose: 12000,
    /**
     * live mode: when a produce request has been in flight this long, new
     * messages go to the offline queue (durable) instead of piling up in memory
     */
    liveStall: 3000,
    /** flusher: pause before peeking again when enqueued records are not written yet */
    emptyPoll: 25
};

/**
 * Limits. Exported as `module.exports.LIMITS` so that tests can lower them.
 * maxPending: most messages kept in memory (live pending list, messages
 * waiting for the queue to open); beyond it messages go to the offline queue.
 */
const LIMITS = {
    maxPending: 10000
};
/** A queued batch that keeps failing with an UNKNOWN error is treated as poison after this many attempts. */
const UNKNOWN_LIMIT = 5;
/** Version of the queue record format ({v, t, tf, ts, m}). */
const RECORD_VERSION = 1;

/** acks setting → Kafka acks. Explicit map: never `acks || x` (0 is a valid value, see 3-static-audit D1). */
const ACKS = new Map([['all', -1], ['leader', 1], ['none', 0], ['-1', -1], ['1', 1], ['0', 0]]);
/** compression setting → kafkajs CompressionTypes key (and its numeric fallback). */
const COMPRESSION = new Map([['none', ['None', 0]], ['gzip', ['GZIP', 1]], ['snappy', ['Snappy', 2]], ['lz4', ['LZ4', 3]]]);
const QUEUE_MODES = new Set(['disk', 'memory', 'off']);
const CONTROL_ACTIONS = new Set(['flush', 'reconnect', 'queue-status', 'queue-clear']);
const TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/;
// Node-RED substitutes ${VAR} / $(VAR) only for a whole value and leaves it as is when VAR is unset.
const ENV_REF = /^\s*(?:\$\{([^}]*)\}|\$\(([^)]*)\))\s*$/;

/**
 * Unique ids for records written by switchToBacklog(), so that ackSentHead()
 * can recognise exactly those records (two messages with the same content and
 * timestamp would otherwise look identical).
 */
const FLIGHT_TOKEN = crypto.randomBytes(6).toString('hex');
let flightSeq = 0;

/**
 * Close promises of producer instances that are still shutting down, by queue
 * directory. A new instance (redeploy) waits for the old one before opening
 * the same directory, so two DiskQueue objects never write it at once.
 */
const queueClosers = new Map();

// ---------------------------------------------------------------------------
// configuration

/** A config value as a string ('' for anything that is not a string or a finite number). */
function text(v) {
    if (typeof v === 'string') return v;
    if (typeof v === 'number' && Number.isFinite(v)) return String(v);
    return '';
}

/** Name of an unresolved `${VAR}` reference that makes up the whole value, or null. */
function envRef(s) {
    const m = typeof s === 'string' ? ENV_REF.exec(s) : null;
    if (!m) return null;
    return (m[1] !== undefined ? m[1] : m[2]) || '?';
}

/**
 * Why `topic` cannot be used as a Kafka topic name, or null when it can.
 * Checking here means an invalid topic fails the message immediately instead
 * of being queued and dead-lettered later.
 */
function topicProblem(topic, what) {
    const variable = envRef(topic);
    if (variable !== null) return `${what}: environment variable ${variable} is not set`;
    if (!TOPIC_RE.test(topic) || topic === '.' || topic === '..') {
        return `${what}: invalid Kafka topic name "${truncate(topic, 60)}" ` +
            '(use 1-249 characters: letters, digits, ".", "_" and "-")';
    }
    return null;
}

/**
 * The static headers of the node configuration. The editor saves an object
 * { name: value | [values] }; an array of { key|name, value } pairs and a JSON
 * string are accepted too (hand-written flows).
 *
 * @returns {{headers: (object|undefined), error: (string|null)}}
 */
function parseStaticHeaders(raw) {
    try {
        let h = raw;
        if (h === undefined || h === null || h === '') return { headers: undefined, error: null };
        if (typeof h === 'string') h = JSON.parse(h);
        if (Array.isArray(h)) {
            const obj = {};
            for (const pair of h) {
                if (!pair || typeof pair !== 'object') continue;
                const name = text(pair.key !== undefined ? pair.key : pair.name);
                if (name === '') continue;
                const value = pair.value;
                if (Object.prototype.hasOwnProperty.call(obj, name)) {
                    obj[name] = [].concat(obj[name], value);
                } else {
                    Object.defineProperty(obj, name, { value, enumerable: true, writable: true, configurable: true });
                }
            }
            h = obj;
        }
        const headers = serialize.toKafkaHeaders(h);
        return { headers: headers && Object.keys(headers).length > 0 ? headers : undefined, error: null };
    } catch (err) {
        return { headers: undefined, error: `Invalid static headers setting: ${err && err.message}` };
    }
}

/**
 * Parse the node configuration (the editor saves strings). Never throws;
 * problems that make every message fail are returned as `*Error` texts,
 * harmless ones as `warnings`.
 *
 * @param {object} config node configuration
 * @returns {object}
 */
function parseConfig(config) {
    const c = config && typeof config === 'object' ? config : {};
    const warnings = [];
    const cfg = { warnings };

    cfg.topic = text(c.topic).trim();
    cfg.topicError = cfg.topic ? topicProblem(cfg.topic, 'Topic setting') : null;

    // A key may legitimately contain spaces: only '' means "use msg.key".
    cfg.key = text(c.key);
    const keyVar = envRef(cfg.key);
    cfg.keyError = keyVar !== null ? `Key setting: environment variable ${keyVar} is not set` : null;

    const partition = text(c.partition).trim();
    cfg.partition = undefined;
    cfg.partitionError = null;
    if (partition !== '') {
        const variable = envRef(partition);
        if (variable !== null) {
            cfg.partitionError = `Partition setting: environment variable ${variable} is not set`;
        } else {
            try {
                cfg.partition = serialize.toKafkaPartition(partition);
            } catch (err) {
                cfg.partitionError = `Partition setting: must be an integer >= 0 (got "${truncate(partition, 30)}")`;
            }
        }
    }

    const headers = parseStaticHeaders(c.headers);
    cfg.headers = headers.headers;
    cfg.headersError = headers.error;
    cfg.useMsgHeaders = toBool(c.useMsgHeaders, true);

    let acksName = text(c.acks).trim().toLowerCase();
    if (acksName === '') acksName = 'all';
    if (!ACKS.has(acksName)) {
        warnings.push(`Unknown acks setting "${truncate(text(c.acks), 20)}": using "all"`);
        acksName = 'all';
    }
    cfg.acks = ACKS.get(acksName);
    cfg.ackTimeout = toInt(c.ackTimeout, 30000, { min: 100, max: 600000 });

    let compression = text(c.compression).trim().toLowerCase();
    if (compression === '') compression = 'none';
    if (!COMPRESSION.has(compression)) {
        warnings.push(`Unknown compression "${truncate(text(c.compression), 20)}": using none`);
        compression = 'none';
    }
    cfg.compression = compression;
    cfg.partitioner = text(c.partitioner).trim().toLowerCase() === 'legacy' ? 'legacy' : 'default';
    cfg.idempotent = toBool(c.idempotent, false);
    if (cfg.idempotent && cfg.acks !== -1) {
        warnings.push('An idempotent producer requires acks "all": acks "all" is used');
        cfg.acks = -1;
    }
    cfg.allowAutoTopicCreation = toBool(c.allowAutoTopicCreation, true);
    cfg.batchSize = toInt(c.batchSize, 100, { min: 1, max: 10000 });

    let mode = text(c.queueMode).trim().toLowerCase();
    if (mode === '') mode = 'disk';
    if (!QUEUE_MODES.has(mode)) {
        warnings.push(`Unknown offline queue mode "${truncate(text(c.queueMode), 20)}": using disk`);
        mode = 'disk';
    }
    const maxMB = toNumber(c.queueMaxMB, 100, { min: 0, max: 1024 * 1024 });
    const maxAgeHours = toNumber(c.queueMaxAgeHours, 0, { min: 0, max: 24 * 365 * 100 });
    cfg.queue = {
        mode,
        dir: text(c.queueDir).trim(),
        name: text(c.queueName).trim(),
        maxBytes: Math.round(maxMB * 1024 * 1024),
        maxMessages: toInt(c.queueMaxMessages, 0, { min: 0 }),
        overflow: text(c.queueOverflow).trim() === 'drop-newest' ? 'drop-newest' : 'drop-oldest',
        maxAgeMs: Math.round(maxAgeHours * 3600 * 1000),
        maxAgeHours,
        fsync: toBool(c.queueFsync, true)
    };
    return cfg;
}

/**
 * The offline queue directory: <root>/<queueName>, where root is the node's
 * queueDir, else settings.kafkajsNcProducerQueueDir, else
 * <userDir>/kafkajs-nc-queue (relative paths resolve against userDir) and
 * queueName defaults to the node id.
 *
 * @returns {{dir: (string|null), error: (string|null)}}
 */
function queueDirFor(cfg, RED, nodeId) {
    try {
        let root = cfg.queue.dir;
        let source = 'Queue directory setting';
        if (root === '') {
            let fromSettings = '';
            try {
                const s = RED && RED.settings && RED.settings.kafkajsNcProducerQueueDir;
                fromSettings = typeof s === 'string' ? s.trim() : '';
            } catch (_) {
                fromSettings = '';
            }
            root = fromSettings || 'kafkajs-nc-queue';
            source = 'settings.kafkajsNcProducerQueueDir';
        }
        const rootVar = envRef(root);
        if (rootVar !== null) return { dir: null, error: `${source}: environment variable ${rootVar} is not set` };
        const nameVar = envRef(cfg.queue.name);
        if (nameVar !== null) return { dir: null, error: `Queue name setting: environment variable ${nameVar} is not set` };
        const name = sanitizeName(cfg.queue.name !== '' ? cfg.queue.name : nodeId);
        return { dir: path.join(resolveDir(root, userDir(RED)), name), error: null };
    } catch (err) {
        return { dir: null, error: `Cannot determine the queue directory: ${err && err.message}` };
    }
}

// ---------------------------------------------------------------------------
// messages

/** Rename 'msg.payload' etc. in a serializer error to the msg.messages element it is about. */
function relabel(message, label) {
    return String(message).replace(/^msg\.(payload|key|headers|partition|timestamp)/,
        (all, field) => `${label}.${field === 'payload' ? 'value' : field}`);
}

function present(v) {
    return v !== undefined && v !== null && v !== '';
}

/**
 * Build one Kafka message from `src` (the msg itself, or an element of
 * msg.messages when index >= 0). Node settings win over message values.
 *
 * @returns {{topic: string, tf: number, message: object}} tf = 1 when the topic came from the message
 */
function buildEntry(msg, src, index, cfg, receivedAt) {
    const label = index < 0 ? 'msg' : `msg.messages[${index}]`;
    if (index >= 0 && (src === null || typeof src !== 'object' || Array.isArray(src))) {
        throw new MessageError(`${label} must be an object like { value, key, headers, partition, timestamp, topic }`);
    }
    try {
        let topic;
        let tf;
        if (cfg.topic) {
            topic = cfg.topic;
            tf = 0;
        } else {
            const own = index >= 0 ? src.topic : undefined;
            const raw = present(own) ? own : msg.topic;
            const where = present(own) ? `${label}.topic` : 'msg.topic';
            if (!present(raw)) {
                throw new MessageError(index < 0
                    ? 'No topic: set the node Topic or msg.topic'
                    : `No topic for ${label}: set the node Topic, ${label}.topic or msg.topic`);
            }
            if (typeof raw !== 'string') throw new MessageError(`${where} must be a string`);
            const problem = topicProblem(raw, where);
            if (problem) throw new MessageError(problem);
            topic = raw;
            tf = 1;
        }

        // elements may use `payload` instead of `value` (a common slip)
        const rawValue = index < 0 ? src.payload : (src.value !== undefined ? src.value : src.payload);
        if (index >= 0 && rawValue === undefined) throw new MessageError(`${label}.value is undefined`);
        const value = serialize.toKafkaValue(rawValue);
        const key = cfg.key !== '' ? cfg.key : serialize.toKafkaKey(src.key);
        const partition = cfg.partition !== undefined ? cfg.partition : serialize.toKafkaPartition(src.partition);
        const msgHeaders = cfg.useMsgHeaders ? serialize.toKafkaHeaders(src.headers) : undefined;
        const headers = msgHeaders || cfg.headers ? { ...(msgHeaders || {}), ...(cfg.headers || {}) } : undefined;
        // Without a timestamp kafkajs would use the SEND time; a message delivered
        // from the offline queue hours later must keep the time it was produced.
        let timestamp = serialize.toKafkaTimestamp(src.timestamp);
        if (timestamp === undefined) timestamp = String(receivedAt);

        const message = { value };
        if (key !== null && key !== undefined) message.key = key;
        if (headers && Object.keys(headers).length > 0) message.headers = headers;
        if (partition !== undefined) message.partition = partition;
        message.timestamp = timestamp;
        return { topic, tf, message };
    } catch (err) {
        if (index >= 0 && err instanceof MessageError) throw new MessageError(relabel(err.message, label));
        throw err;
    }
}

/**
 * Build the Kafka messages of an input msg: msg.messages (array) or the single
 * message described by msg.payload/key/headers/partition/timestamp/topic.
 * Throws MessageError for invalid content.
 *
 * @returns {Array<{topic: string, tf: number, message: object}>}
 */
function buildEntries(msg, cfg, receivedAt) {
    if (cfg.topicError) throw new MessageError(cfg.topicError);
    if (cfg.keyError) throw new MessageError(cfg.keyError);
    if (cfg.partitionError) throw new MessageError(cfg.partitionError);
    if (cfg.headersError) throw new MessageError(cfg.headersError);
    if (Array.isArray(msg.messages)) {
        if (msg.messages.length === 0) throw new MessageError('msg.messages is an empty array: nothing to send');
        return msg.messages.map((el, i) => buildEntry(msg, el, i, cfg, receivedAt));
    }
    return [buildEntry(msg, msg, -1, cfg, receivedAt)];
}

/**
 * The JSON-safe queue record of one Kafka message:
 * { v: 1, t: topic, tf: 1 when the topic came from the message, ts: arrival
 * time (ms), m: lib/serialize encodeMessage() of the Kafka message, id?: only
 * on records of an in-flight request queued by switchToBacklog() }.
 */
function toRecord(entry, receivedAt) {
    return { v: RECORD_VERSION, t: entry.topic, tf: entry.tf, ts: receivedAt, m: serialize.encodeMessage(entry.message) };
}

/**
 * One input message on its way to Kafka. Its Kafka messages are split into
 * chunks: consecutive messages for the same topic, at most batchSize each
 * (one produce request only ever carries one topic, which avoids partial
 * multi-topic writes).
 *
 * chunk.state: 'pending' (waiting in memory) → 'inflight' (in a produce
 * request) → 'sent'; or → 'queuing' (being written to the queue) → 'queued'.
 * The item is settled (report + done) once every chunk is sent or queued, or
 * as soon as one of them fails for good.
 */
function makeItem(msg, send, done, entries, receivedAt, batchSize) {
    const item = {
        msg,
        send,
        done: once(done),
        receivedAt,
        count: entries.length,
        chunks: [],
        settled: false,
        results: [],
        location: null
    };
    let chunk = null;
    for (const entry of entries) {
        if (!chunk || chunk.topic !== entry.topic || chunk.entries.length >= batchSize) {
            chunk = { item, topic: entry.topic, tf: entry.tf, entries: [], state: 'pending', isolate: false };
            item.chunks.push(chunk);
        }
        chunk.entries.push(entry);
    }
    return item;
}

/** Queue records of a list of chunks, in order. */
function recordsOf(chunks) {
    const records = [];
    for (const chunk of chunks) {
        for (const entry of chunk.entries) records.push(toRecord(entry, chunk.item.receivedAt));
    }
    return records;
}

/** The distinct items of a list of chunks, in order. */
function itemsOf(chunks) {
    const seen = new Set();
    const items = [];
    for (const chunk of chunks) {
        if (!seen.has(chunk.item)) {
            seen.add(chunk.item);
            items.push(chunk.item);
        }
    }
    return items;
}

function addOffset(base, n) {
    try {
        const b = BigInt(base);
        return b < 0n ? undefined : (b + BigInt(n)).toString();
    } catch (_) {
        return undefined;
    }
}

function sameTopic(r, topic) {
    return r && (r.topicName === undefined || r.topicName === topic);
}

/**
 * Where each message of one produce request ended up, from the kafkajs
 * RecordMetadata[] (one entry per partition written, baseOffset = offset of
 * the first message written to that partition). Only exact answers are
 * given: when the partitioner spread messages over several partitions, the
 * position of an individual message is unknown (null).
 *
 * @param {object[]} messages kafkajs messages in request order
 * @param {object[]} results RecordMetadata[]
 * @param {string} topic
 * @returns {Array<{partition: number, offset: (string|undefined)}|null>}
 */
function locateMessages(messages, results, topic) {
    const out = messages.map(() => null);
    const list = (Array.isArray(results) ? results : []).filter((r) => sameTopic(r, topic));
    if (list.length === 0) return out;
    const explicit = messages.every((m) => m.partition !== undefined);
    if (explicit) {
        const bases = new Map(list.map((r) => [Number(r.partition), r.baseOffset]));
        const counts = new Map();
        messages.forEach((m, j) => {
            if (!bases.has(m.partition)) return;
            const k = counts.get(m.partition) || 0;
            counts.set(m.partition, k + 1);
            out[j] = { partition: m.partition, offset: addOffset(bases.get(m.partition), k) };
        });
        return out;
    }
    if (list.length === 1) {
        const p = Number(list[0].partition);
        if (messages.every((m) => m.partition === undefined || m.partition === p)) {
            messages.forEach((m, j) => {
                out[j] = { partition: p, offset: addOffset(list[0].baseOffset, j) };
            });
        }
    }
    return out;
}

/** Explicit partitions of `messages` that kafkajs did not write (it skips unknown partitions silently). */
function missingPartitions(messages, results, topic) {
    const written = new Set((Array.isArray(results) ? results : [])
        .filter((r) => sameTopic(r, topic)).map((r) => Number(r.partition)));
    const missing = [];
    for (const m of messages) {
        if (m.partition !== undefined && !written.has(m.partition) && !missing.includes(m.partition)) {
            missing.push(m.partition);
        }
    }
    return missing;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// runtime

/**
 * Everything one producer node does at runtime. Kept out of the Node-RED node
 * object so method names can never collide with Node's own (send, close, ...).
 */
class ProducerRuntime {
    /**
     * Never throws: problems are recorded and reported from start().
     *
     * @param {object} RED
     * @param {object} node the Node-RED node
     * @param {object} config node configuration
     */
    constructor(RED, node, config) {
        this.RED = RED;
        this.node = node;
        this.status = new StatusReporter(node, { throttleMs: 1000 });

        this.closing = false;
        this.closePromise = null;
        /** connection generation; bumped on every connect attempt, disconnect and close */
        this.gen = 0;
        /** 'idle' | 'connecting' | 'connected' | 'waiting' | 'stopped' | 'closing' */
        this.state = 'idle';
        /** 'live' | 'backlog' (see the file header) */
        this.mode = 'backlog';
        this.producer = null;
        this.retryTimer = null;
        this.flushRetryTimer = null;

        // live pipeline
        this.pending = [];          // chunks waiting to be sent, in order
        this.inFlight = null;       // { chunks, since, persisted }
        this.working = false;
        this.workerPromise = null;

        // offline queue
        this.queue = null;
        /** 'off' | 'pending' (not open yet) | 'open' | 'failed' */
        this.queueState = 'off';
        this.queueError = null;
        this.queueDir = null;
        this.queueDirError = null;
        this.queueOpening = null;
        this.releaseQueueDir = null;
        this.early = [];            // items that arrived before the queue was open
        this.flushing = false;
        this.flushPromise = null;
        this.unknownHead = null;
        this.unknownCount = 0;

        // failure bookkeeping (log de-duplication, status)
        this.failures = 0;
        this.lastFailKey = null;
        this.lastFailContext = null;
        this.lastError = null;
        this.lastKind = null;
        this.retryInMs = 0;

        this.cfg = parseConfig(config);
        this.compressionType = 0;

        this.broker = null;
        this.brokerError = null;
        let r = {};
        try {
            const id = config && config.broker;
            this.broker = id ? RED.nodes.getNode(id) || null : null;
            if (this.broker && this.broker.configError) this.brokerError = this.broker.configError;
            r = (this.broker && this.broker.reconnect) || {};
        } catch (_) {
            this.broker = null;
        }
        this.reconnect = {
            min: toInt(r.min, 1000, { min: 10, max: 3600000 }),
            max: toInt(r.max, 60000, { min: 10, max: 3600000 }),
            authMin: toInt(r.authMin, 60000, { min: 10, max: 3600000 })
        };
        this.backoff = new Backoff({ min: this.reconnect.min, max: this.reconnect.max });
        this.flushBackoff = new Backoff({ min: this.reconnect.min, max: this.reconnect.max });

        if (this.cfg.queue.mode !== 'off') {
            this.queueState = 'pending';
            if (this.cfg.queue.mode === 'disk') {
                const where = queueDirFor(this.cfg, RED, node.id);
                this.queueDir = where.dir;
                this.queueDirError = where.error;
            }
        }

        // Statuses and errors emitted synchronously in a constructor never reach
        // Status/Catch nodes, so the real work starts after construction.
        setImmediate(() => {
            this.startPromise = this.start().catch((err) => this.bug('start', err));
        });
    }

    // ------------------------------------------------------------ logging

    log(text) {
        try {
            this.node.log(text);
        } catch (_) {
            // logging must never break the producer
        }
    }

    warn(text) {
        try {
            this.node.warn(text);
        } catch (_) {
            // ignore
        }
    }

    error(text) {
        try {
            this.node.error(text);
        } catch (_) {
            // ignore
        }
    }

    debug(text) {
        try {
            this.node.debug(text);
        } catch (_) {
            // ignore
        }
    }

    /** An unexpected internal error: report it, never rethrow. */
    bug(where, err) {
        this.error(`Internal error (${where}): ${describeError(err)}`);
    }

    // ------------------------------------------------------------ status

    /** Number of messages waiting in the offline queue (or for it to open). */
    queuedCount() {
        let n = this.early.length;
        try {
            if (this.queue) n += this.queue.size;
        } catch (_) {
            // a queue getter never throws; defensive only
        }
        return n;
    }

    /**
     * Show the status derived from the current state. `throttled` is used for
     * frequent updates (queue depth), state changes are immediate.
     */
    render(throttled) {
        if (this.closing) return;
        try {
            const n = this.queuedCount();
            const queued = n > 0 ? ` · ${n} queued` : '';
            let fill;
            let shape = 'ring';
            let label;
            if (!this.broker) {
                fill = 'red';
                label = `no broker config${queued}`;
            } else if (this.brokerError) {
                fill = 'red';
                label = `config error${queued}`;
            } else if (this.state === 'connected') {
                fill = 'green';
                if (this.flushing) {
                    label = `flushing · ${n} queued`;
                } else if (n > 0) {
                    shape = 'dot';
                    label = `connected${queued}`;
                } else if (this.queueState === 'failed') {
                    fill = 'yellow';
                    shape = 'dot';
                    label = 'connected · queue unavailable';
                } else {
                    shape = 'dot';
                    label = 'connected';
                }
            } else if (this.state === 'waiting') {
                const retry = ` · retry ${fmtDelay(this.retryInMs)}`;
                if (this.lastKind === ErrorKind.AUTH || this.lastKind === ErrorKind.CONFIG) {
                    // needs a human: 'auth failed', 'config error', 'TLS: self-signed cert', ...
                    fill = 'red';
                    label = problemText(this.lastError, 24) + queued + retry;
                } else {
                    fill = 'yellow';
                    label = (n > 0 ? `offline${queued}` : shortMessage(this.lastError, 28)) + retry;
                }
            } else if (this.state === 'connecting') {
                fill = 'yellow';
                label = `connecting…${queued}`;
            } else {
                fill = 'grey';
                label = n > 0 ? `${n} queued` : 'starting';
            }
            if (throttled) this.status.activity(fill, shape, label);
            else this.status.set(fill, shape, label);
        } catch (_) {
            // status is cosmetic
        }
    }

    // ------------------------------------------------------------ startup

    /** Report configuration problems, open the queue, then start connecting. */
    async start() {
        if (this.closing) return;
        this.reportConfig();
        this.render();
        if (this.queueState === 'pending') {
            this.queueOpening = this.openQueue();
            await this.queueOpening;
        }
        if (this.closing) return;
        this.drainEarly();
        const n = this.queue ? this.queue.size : 0;
        if (n > 0) {
            this.log(`The offline queue holds ${n} message(s) from earlier; they are sent first once Kafka is reachable`);
        }
        if (this.broker && !this.brokerError) {
            this.connect();
        } else {
            this.state = 'stopped';
            this.render();
        }
    }

    reportConfig() {
        for (const w of this.cfg.warnings) this.warn(w);
        for (const e of [this.cfg.topicError, this.cfg.keyError, this.cfg.partitionError, this.cfg.headersError]) {
            if (e) this.error(`${e} — every message will be rejected until this is fixed`);
        }
        if (!this.broker) {
            this.error('No Kafka broker configuration selected');
        } else if (this.brokerError) {
            const details = Array.isArray(this.brokerError.details) && this.brokerError.details.length
                ? this.brokerError.details.join('; ')
                : this.brokerError.message;
            this.error(`Invalid Kafka broker configuration: ${details}`);
        }
    }

    /** Open the offline queue. On failure the node works without one (queue mode effectively 'off'). */
    async openQueue() {
        const q = this.cfg.queue;
        const logger = {
            warn: (m) => this.warn(m),
            error: (m) => this.error(m),
            debug: (m) => this.debug(m)
        };
        try {
            if (q.mode === 'memory') {
                this.queue = new MemoryQueue({
                    maxBytes: q.maxBytes, maxMessages: q.maxMessages, overflow: q.overflow, logger
                });
            } else {
                if (!this.queueDir) throw new Error(this.queueDirError || 'no queue directory');
                const previous = queueClosers.get(this.queueDir);
                if (previous) await withTimeout(previous, TIMEOUTS.previousClose);
                if (this.closing) return;
                this.releaseQueueDir = queueRegistry.acquire(this.queueDir, this.node.id);
                this.queue = new DiskQueue({
                    dir: this.queueDir,
                    maxBytes: q.maxBytes,
                    maxMessages: q.maxMessages,
                    overflow: q.overflow,
                    fsync: q.fsync,
                    logger
                });
            }
            await this.queue.open();
            this.queueState = 'open';
        } catch (err) {
            this.queueState = 'failed';
            this.queueError = err;
            const where = this.queueDir ? ` (${this.queueDir})` : '';
            this.error(`Offline queue unavailable${where}: ${err && err.message} — messages cannot be stored ` +
                'while Kafka is unreachable');
            const queue = this.queue;
            this.queue = null;
            if (queue) queue.close().catch(noop);
            this.releaseDir();
        }
    }

    releaseDir() {
        try {
            if (this.releaseQueueDir) this.releaseQueueDir();
        } catch (_) {
            // release is idempotent and never throws; defensive only
        }
        this.releaseQueueDir = null;
    }

    /** Dispatch the messages that arrived while the queue was opening, in order. */
    drainEarly() {
        const early = this.early;
        this.early = [];
        for (const item of early) this.dispatch(item);
        if (early.length) this.render();
    }

    // ------------------------------------------------------------ input

    /**
     * The input handler (synchronous; failures go to done(err)).
     *
     * @param {object} msg
     * @param {Function} send
     * @param {Function} done
     */
    input(msg, send, done) {
        const finish = once((err) => this.safeDone(done, err));
        try {
            if (typeof msg.action === 'string' && CONTROL_ACTIONS.has(msg.action)) {
                this.control(msg.action, msg, send, finish);
                return;
            }
            const receivedAt = Date.now();
            let entries;
            try {
                entries = buildEntries(msg, this.cfg, receivedAt);
            } catch (err) {
                finish(err);
                return;
            }
            this.dispatch(makeItem(msg, send, finish, entries, receivedAt, this.cfg.batchSize));
        } catch (err) {
            finish(err);
        }
    }

    /** Send an item live, queue it, or reject it (see the file header). */
    dispatch(item) {
        if (this.closing) {
            this.settleFailed(item, new Error('The Kafka producer node is closing — message not sent'));
            return;
        }
        if (this.state === 'connected' && this.mode === 'live') {
            const stalled = this.inFlight !== null && Date.now() - this.inFlight.since > TIMEOUTS.liveStall;
            if (this.pending.length < LIMITS.maxPending && !stalled) {
                this.pushLive(item);
                return;
            }
            if (this.queueState === 'open') {
                // Kafka is slow (at the start of an outage kafkajs retries a
                // request for ~20 s) or too much is waiting in memory: switch to
                // the durable queue, keeping the order (see switchToBacklog).
                this.switchToBacklog();
                this.enqueueItem(item);
                this.render();
                return;
            }
            if (this.pending.length < LIMITS.maxPending) {
                this.pushLive(item);
                return;
            }
            this.settleFailed(item, new Error(`Too many messages (${LIMITS.maxPending}) are waiting for Kafka and the ` +
                'offline queue is off — message not sent'));
            return;
        }
        if (this.queueState === 'pending') {
            if (this.early.length < LIMITS.maxPending) {
                this.early.push(item);
                this.render(true);
                return;
            }
            this.settleFailed(item, new Error('The offline queue is still opening and too many messages are ' +
                'waiting for it — message not sent'));
            return;
        }
        if (this.queueState === 'open') {
            this.enqueueItem(item);
            return;
        }
        this.settleFailed(item, this.notConnectedError());
    }

    notConnectedError() {
        let reason = 'Kafka is not connected';
        if (!this.broker) reason = 'No Kafka broker configuration is selected';
        else if (this.brokerError) reason = 'The Kafka broker configuration is invalid';
        const queue = this.queueState === 'failed'
            ? `the offline queue is unavailable (${this.queueError && this.queueError.message})`
            : 'the offline queue is off';
        return new Error(`${reason} and ${queue} — message not sent`);
    }

    /** Append the pending chunks of an item to the offline queue. */
    enqueueItem(item) {
        return this.enqueueChunks(item.chunks.filter((c) => c.state === 'pending'));
    }

    /**
     * Append chunks to the offline queue, in order. Each item's chunks go in one
     * all-or-nothing append (the queue writes concurrent appends in call order),
     * so a full queue rejects whole messages. Never rejects.
     */
    enqueueChunks(chunks) {
        const groups = new Map();
        for (const chunk of chunks) {
            if (chunk.item.settled) continue;
            if (!groups.has(chunk.item)) groups.set(chunk.item, []);
            groups.get(chunk.item).push(chunk);
        }
        const writes = [];
        for (const [item, list] of groups) writes.push(this.enqueueGroup(item, list));
        this.render(true);
        return Promise.all(writes);
    }

    enqueueGroup(item, list) {
        let p;
        try {
            const records = recordsOf(list);
            for (const chunk of list) chunk.state = 'queuing';
            p = this.queue.enqueueMany(records);
        } catch (err) {
            p = Promise.reject(err);
        }
        return Promise.resolve(p).then(() => {
            for (const chunk of list) chunk.state = 'queued';
            this.checkItem(item);
        }, (err) => this.settleFailed(item, this.queueFailure(err))).catch((err) => this.bug('enqueue', err));
    }

    /**
     * Leave live mode while a request is in flight (Kafka slow or too much
     * waiting in memory). The queue is empty in live mode, so the in-flight
     * batch is appended first, then the pending messages: everything stays in
     * order whatever the in-flight request's outcome. If it succeeds, its
     * records are removed again from the head of the queue (ackSentHead).
     */
    switchToBacklog() {
        this.mode = 'backlog';
        const flight = this.inFlight;
        if (flight && !flight.queued) {
            try {
                const records = recordsOf(flight.chunks).map((r) => ({ ...r, id: `${FLIGHT_TOKEN}.${++flightSeq}` }));
                const json = records.map((r) => JSON.stringify(r));
                let p;
                try {
                    p = this.queue.enqueueMany(records);
                } catch (err) {
                    p = Promise.reject(err);
                }
                const stored = Promise.resolve(p).then(() => true, (err) => {
                    this.debug(`Could not store the in-flight messages in the offline queue: ${err && err.message}`);
                    return false;
                });
                flight.queued = { json, stored };
            } catch (err) {
                this.bug('switch', err);
            }
        }
        const pending = this.pending.splice(0);
        if (pending.length > 0) this.enqueueChunks(pending);
    }

    /**
     * The in-flight batch was also appended to the queue (switchToBacklog) and
     * Kafka has now acknowledged it: remove those records from the head of the
     * queue so they are not sent twice. Only records that are exactly ours are
     * removed (drop-oldest may have dropped them meanwhile). Never rejects; on
     * failure the worst case is a duplicate (at-least-once).
     */
    async ackSentHead(json) {
        try {
            let rest = json;
            while (rest.length > 0) {
                const entries = await this.queue.peek(rest.length);
                let n = 0;
                while (n < entries.length && !entries[n].corrupt && JSON.stringify(entries[n].record) === rest[n]) n++;
                if (n === 0) break;
                await this.queue.ack(entries.slice(0, n));
                rest = rest.slice(n);
                if (n < entries.length) break;
            }
            if (rest.length > 0) {
                this.debug(`${rest.length} sent message(s) were not found at the head of the offline queue ` +
                    '(dropped by the size limit?); any left will be sent again');
            }
        } catch (err) {
            this.debug(`Could not remove sent messages from the offline queue: ${err && err.message} ` +
                '(they will be sent again)');
        }
    }

    /** A QueueFullError explains itself; anything else is a storage problem. */
    queueFailure(err) {
        if (err && err.name === 'KafkaNcQueueFullError') return err;
        const wrapped = new Error(`Could not store the message in the offline queue: ${err && err.message}`);
        wrapped.cause = err;
        if (err && err.code) wrapped.code = err.code;
        return wrapped;
    }

    // ------------------------------------------------------------ settling items

    /** Settle the item once every chunk is sent or queued ("queued" if any part is). */
    checkItem(item) {
        if (item.settled) return;
        let queued = false;
        for (const chunk of item.chunks) {
            if (chunk.state === 'queued') queued = true;
            else if (chunk.state !== 'sent') return;
        }
        if (queued) this.settleQueued(item);
        else this.settleSent(item);
    }

    settleSent(item) {
        if (item.settled) return;
        item.settled = true;
        try {
            const first = item.chunks[0];
            const report = { status: 'sent', topic: first.topic };
            if (item.count === 1) {
                const explicit = first.entries[0].message.partition;
                const partition = item.location ? item.location.partition : explicit;
                if (partition !== undefined) report.partition = partition;
                if (item.location && item.location.offset !== undefined) report.offset = item.location.offset;
            } else {
                report.count = item.count;
            }
            report.results = item.results;
            item.msg.kafka = report;
            if (!this.closing) item.send(item.msg);
        } catch (err) {
            this.bug('report', err);
        }
        this.callDone(item);
    }

    settleQueued(item) {
        if (item.settled) return;
        item.settled = true;
        try {
            const report = { status: 'queued', topic: item.chunks[0].topic, queueSize: this.queuedCount() };
            if (item.count > 1) report.count = item.count;
            item.msg.kafka = report;
            if (!this.closing) item.send(item.msg);
        } catch (err) {
            this.bug('report', err);
        }
        this.callDone(item);
        this.render(true);
    }

    settleFailed(item, err) {
        if (item.settled) return;
        item.settled = true;
        this.callDone(item, err);
    }

    /**
     * Call Node-RED's done() without letting it throw: with a Catch node,
     * done(err) clones the message, which throws for messages that cannot be
     * cloned (e.g. a throwing getter). The problem is then at least logged.
     */
    safeDone(done, err) {
        try {
            if (err) done(err);
            else done();
        } catch (e) {
            this.error(`Could not complete the message: ${(err && err.message) || 'ok'} (${e && e.message})`);
        }
    }

    /** done() must never throw into the send/flush loops. */
    callDone(item, err) {
        try {
            if (err) item.done(err);
            else item.done();
        } catch (e) {
            this.bug('done', e);
        }
    }

    // ------------------------------------------------------------ connection supervisor

    /** True when the work started under generation `gen` must stop. */
    stale(gen) {
        return this.closing || gen !== this.gen;
    }

    /** Start a connect attempt now. */
    connect() {
        if (this.closing) return;
        this.clearRetryTimer();
        const gen = ++this.gen;
        this.state = 'connecting';
        this.render();
        this.connectAttempt(gen).catch((err) => this.bug('connect', err));
    }

    async connectAttempt(gen) {
        let producer = null;
        try {
            const kafka = this.broker.createKafka(this.node);
            const kafkajs = getKafkajs();
            const partitioners = kafkajs.Partitioners || {};
            const types = kafkajs.CompressionTypes || {};
            const [typeName, fallback] = COMPRESSION.get(this.cfg.compression);
            this.compressionType = typeof types[typeName] === 'number' ? types[typeName] : fallback;
            const options = {
                // always explicit, so kafkajs logs no "default partitioner changed" warning
                createPartitioner: this.cfg.partitioner === 'legacy'
                    ? partitioners.LegacyPartitioner
                    : partitioners.DefaultPartitioner,
                allowAutoTopicCreation: this.cfg.allowAutoTopicCreation,
                idempotent: this.cfg.idempotent
            };
            if (this.cfg.idempotent) options.maxInFlightRequests = 1;
            producer = kafka.producer(options);
        } catch (err) {
            // createKafka() throws a ConfigError; kafka.producer() only throws
            // synchronously for invalid options. Either way: a config problem.
            this.connectFailed(gen, asConfigError(err, 'Could not create the Kafka producer'), producer, ErrorKind.CONFIG);
            return;
        }
        if (this.stale(gen)) {
            this.disconnectQuietly(producer);
            return;
        }
        this.producer = producer;
        this.watch(producer, gen);
        try {
            await producer.connect();
        } catch (err) {
            this.connectFailed(gen, err, producer);
            return;
        }
        if (this.stale(gen)) {
            // closed or superseded while connecting: kafkajs cannot cancel a
            // connect, so this client must be shut down now or it lives on
            this.disconnectQuietly(producer);
            return;
        }
        this.state = 'connected';
        if (this.failures > 0 && this.lastFailContext === 'connection') {
            this.log(`Connected to Kafka after ${this.failures + 1} attempts`);
        }
        // older messages kept in memory are sent before the queue
        if (this.pending.length > 0) this.kickWorker();
        else this.maybeStartFlusher();
        // The connection only counts as healthy once a request got through when
        // the last failure was a send (or there is a backlog to send): a broker
        // that accepts connections but rejects every produce request must not
        // make us reconnect at full speed forever.
        if (this.mode === 'live' && this.lastFailContext !== 'send') this.healthy();
        this.render();
    }

    /** A request succeeded: reset the backoff and the failure log state. */
    healthy() {
        if (this.failures === 0 && this.lastError === null) return;
        this.backoff.reset();
        this.failures = 0;
        this.lastFailKey = null;
        this.lastFailContext = null;
        this.lastError = null;
        this.lastKind = null;
    }

    /** kafkajs instrumentation listeners (always through safeOn: a sync throw would crash Node-RED). */
    watch(producer, gen) {
        try {
            const event = producer.events && producer.events.REQUEST_TIMEOUT;
            if (typeof event !== 'string') return;
            safeOn(producer, event, (e) => {
                if (this.stale(gen)) return;
                const p = (e && e.payload) || {};
                this.debug(`Kafka request ${p.apiName || ''} to ${p.broker || 'broker'} timed out`);
            }, (err) => this.debug(`request-timeout listener failed: ${err && err.message}`));
        } catch (_) {
            // instrumentation is optional
        }
    }

    connectFailed(gen, err, producer, forcedKind) {
        if (producer) {
            this.disconnectQuietly(producer);
            if (this.producer === producer) this.producer = null;
        }
        if (this.stale(gen)) return;
        // same mapping as the consumer: TOPIC ⇒ AUTH, MESSAGE ⇒ UNKNOWN
        const kind = forcedKind || connectionKind(err, 'connect');
        this.state = 'waiting';
        this.scheduleReconnect(err, kind, 'connection');
    }

    /**
     * The connection is unusable (a send failed at connection level): drop the
     * producer and reconnect after the backoff.
     */
    markDisconnected(err, kind, context) {
        if (this.closing || this.state !== 'connected') return;
        this.gen++;
        const old = this.producer;
        this.producer = null;
        this.disconnectQuietly(old);
        this.state = 'waiting';
        if (this.queueState === 'open') this.mode = 'backlog';
        this.scheduleReconnect(err, kind, context);
    }

    scheduleReconnect(err, kind, context) {
        this.clearRetryTimer();
        let delay = this.backoff.next();
        // auth/config problems need a human: retry slowly (avoids account lockouts and log floods)
        if (kind === ErrorKind.AUTH || kind === ErrorKind.CONFIG) delay = Math.max(delay, this.reconnect.authMin);
        this.lastError = err;
        this.lastKind = kind;
        this.retryInMs = delay;
        this.noteFailure(err, kind, delay, context);
        this.render();
        this.retryTimer = setTimeout(() => {
            this.retryTimer = null;
            try {
                this.connect();
            } catch (e) {
                this.bug('reconnect', e);
            }
        }, delay);
    }

    clearRetryTimer() {
        if (this.retryTimer) clearTimeout(this.retryTimer);
        this.retryTimer = null;
    }

    /**
     * Log a failure without flooding the log: the first failure of an outage
     * (or a different kind of failure) is an error, repeats are debug, with a
     * warning every 10th attempt.
     */
    noteFailure(err, kind, delay, context) {
        try {
            const c = classifyError(err);
            const key = `${kind}|${c.reason}`;
            this.failures++;
            const what = context === 'send'
                ? 'Kafka send failed'
                : (context === 'flush' ? 'Sending queued messages to Kafka failed' : 'Kafka connection failed');
            const kept = this.queueState === 'open' && context !== 'connection'
                ? ' (the messages are kept in the offline queue)'
                : '';
            const line = `${what}: ${describeError(err)} — retrying in ${fmtDelay(delay)}${kept}`;
            if (this.failures === 1 || key !== this.lastFailKey) {
                this.error(line);
            } else if (this.failures % 10 === 0) {
                this.warn(`Kafka still failing after ${this.failures} attempts: ${describeError(err)} — ` +
                    `retrying in ${fmtDelay(delay)}`);
            } else {
                this.debug(line);
            }
            this.lastFailKey = key;
            this.lastFailContext = context;
        } catch (_) {
            // logging is best effort
        }
    }

    /** producer.disconnect(), bounded and never rejecting. */
    disconnectQuietly(producer) {
        if (!producer) return Promise.resolve();
        let p;
        try {
            p = producer.disconnect();
        } catch (_) {
            return Promise.resolve();
        }
        return withTimeout(p, TIMEOUTS.disconnect);
    }

    /** The send() options shared by the live pipeline and the flusher. */
    sendArgs(topic, messages) {
        return {
            topic,
            messages,
            acks: this.cfg.acks,
            timeout: this.cfg.ackTimeout,
            compression: this.compressionType || 0
        };
    }

    // ------------------------------------------------------------ live pipeline

    pushLive(item) {
        for (const chunk of item.chunks) this.pending.push(chunk);
        this.kickWorker();
    }

    kickWorker() {
        if (this.working || this.closing || this.state !== 'connected' || this.pending.length === 0) return;
        this.working = true;
        this.workerPromise = this.workLoop()
            .catch((err) => this.bug('send', err))
            .then(() => {
                this.working = false;
                this.workerPromise = null;
                this.afterWork();
            })
            .catch((err) => this.bug('send', err));
    }

    afterWork() {
        if (this.closing || this.state !== 'connected') return;
        if (this.pending.length > 0) this.kickWorker();
        else this.maybeStartFlusher();
    }

    /** Next produce request: whole chunks of one topic, up to batchSize messages. */
    takeBatch() {
        const pending = this.pending;
        let i = 0;
        while (i < pending.length && pending[i].item.settled) i++;
        if (i >= pending.length) {
            pending.length = 0;
            return [];
        }
        const first = pending[i];
        let end = i + 1;
        let count = first.entries.length;
        if (!first.isolate) {
            while (end < pending.length) {
                const next = pending[end];
                if (next.item.settled) {
                    end++;
                    continue;
                }
                if (next.isolate || next.topic !== first.topic ||
                    count + next.entries.length > this.cfg.batchSize) break;
                count += next.entries.length;
                end++;
            }
        }
        return pending.splice(0, end).filter((c) => !c.item.settled);
    }

    async workLoop() {
        while (!this.closing && this.state === 'connected' && this.pending.length > 0) {
            const gen = this.gen;
            const producer = this.producer;
            const batch = this.takeBatch();
            if (batch.length === 0) continue;
            for (const chunk of batch) chunk.state = 'inflight';
            const flight = { chunks: batch, since: Date.now(), persisted: false, queued: null };
            this.inFlight = flight;
            const messages = [];
            for (const chunk of batch) for (const e of chunk.entries) messages.push(e.message);
            let results;
            let error = null;
            let forcedKind;
            if (!producer) {
                error = new Error('The Kafka producer is not connected');
                forcedKind = ErrorKind.TRANSIENT;
            } else {
                try {
                    results = await producer.send(this.sendArgs(batch[0].topic, messages));
                } catch (err) {
                    error = err || new Error('Kafka send failed');
                }
            }
            this.inFlight = null;
            if (flight.persisted) return; // close stopped waiting and took care of these
            if (flight.queued && await flight.queued.stored) {
                // We switched to backlog mode meanwhile and this batch is at the
                // head of the queue already.
                if (!error) {
                    await this.ackSentHead(flight.queued.json);
                    this.liveSent(batch, messages, results);
                } else {
                    for (const chunk of batch) {
                        if (chunk.state === 'inflight') chunk.state = 'queued';
                    }
                    for (const item of itemsOf(batch)) this.checkItem(item);
                    // a bad message is dead-lettered by the flusher like any queued one
                    const kind = this.sendKind(error, batch, forcedKind);
                    if (kind !== ErrorKind.MESSAGE && gen === this.gen) this.markDisconnected(error, kind, 'send');
                }
                continue;
            }
            if (error) {
                if (this.closing) {
                    for (const chunk of batch) chunk.state = 'pending';
                    this.pending.unshift(...batch); // close persists them
                    return;
                }
                this.liveFailed(batch, error, forcedKind, gen);
            } else {
                this.liveSent(batch, messages, results);
            }
        }
    }

    /** Classify a failed produce request; TOPIC depends on where the topic came from. */
    sendKind(err, batch, forcedKind) {
        const kind = forcedKind || classifyError(err, { op: 'send' }).kind;
        if (kind === ErrorKind.TOPIC) return batch[0].tf ? ErrorKind.MESSAGE : ErrorKind.AUTH;
        return kind;
    }

    liveSent(batch, messages, results) {
        this.healthy();
        const list = Array.isArray(results) ? results : [];
        const located = locateMessages(messages, list, batch[0].topic);
        const touched = new Set();
        let idx = 0;
        for (const chunk of batch) {
            const item = chunk.item;
            chunk.state = 'sent';
            if (!touched.has(item)) {
                touched.add(item);
                item.results.push(...list);
            }
            if (item.count === 1) item.location = located[idx];
            idx += chunk.entries.length;
            if (this.cfg.acks !== 0 && !item.settled) {
                const missing = missingPartitions(chunk.entries.map((e) => e.message), list, chunk.topic);
                if (missing.length > 0) {
                    this.settleFailed(item, new MessageError(`Partition ${missing.join(', ')} does not exist for ` +
                        `topic ${chunk.topic} (kafkajs wrote nothing for it)`));
                }
            }
        }
        for (const item of touched) this.checkItem(item);
    }

    liveFailed(batch, err, forcedKind, gen) {
        const kind = this.sendKind(err, batch, forcedKind);
        if (kind === ErrorKind.MESSAGE) {
            const items = itemsOf(batch);
            if (items.length > 1) {
                // one of them is bad: send them one at a time to find it
                for (const chunk of batch) {
                    chunk.state = 'pending';
                    chunk.isolate = true;
                }
                this.pending.unshift(...batch);
                return;
            }
            this.settleFailed(items[0], err);
            return;
        }
        // Connection-level problem: everything not sent yet keeps its order.
        for (const chunk of batch) chunk.state = 'pending';
        const chunks = batch.concat(this.pending.splice(0));
        if (this.queueState === 'open' && this.queue.size === 0) {
            this.mode = 'backlog';
            this.enqueueChunks(chunks);
        } else if (this.queueState === 'open') {
            // Newer messages are queued already (rare: only after a failed
            // switchToBacklog): appending these would break the order, so they
            // stay in memory and are sent first after reconnecting.
            this.pending = chunks;
        } else {
            for (const item of itemsOf(chunks)) this.settleFailed(item, err);
        }
        if (gen === this.gen) this.markDisconnected(err, kind, 'send');
    }

    // ------------------------------------------------------------ offline queue flusher

    /** Start sending the offline queue if connected and nothing else is sending. */
    maybeStartFlusher() {
        if (this.closing || this.state !== 'connected' || this.flushing || this.working || this.pending.length) return;
        if (this.queueState !== 'open' || this.queue.size === 0) {
            if (this.mode !== 'live') {
                this.mode = 'live';
                this.render();
            }
            return;
        }
        if (this.flushRetryTimer) {
            clearTimeout(this.flushRetryTimer);
            this.flushRetryTimer = null;
        }
        this.mode = 'backlog';
        this.flushing = true;
        const gen = this.gen;
        this.render();
        this.flushPromise = this.flushLoop(gen)
            .catch((err) => this.flushCrashed(gen, err))
            .then(() => {
                this.flushing = false;
                this.flushPromise = null;
                if (!this.closing && this.state === 'connected' && this.mode === 'live') this.kickWorker();
                this.render();
            })
            .catch((err) => this.bug('flush', err));
    }

    /** The queue itself failed (e.g. a disk error): retry later, keep the messages. */
    flushCrashed(gen, err) {
        if (this.stale(gen) || this.state !== 'connected') return;
        const delay = this.flushBackoff.next();
        this.error(`Offline queue error while sending queued messages: ${describeError(err)} — retrying in ` +
            `${fmtDelay(delay)}`);
        this.flushRetryTimer = setTimeout(() => {
            this.flushRetryTimer = null;
            try {
                this.maybeStartFlusher();
            } catch (e) {
                this.bug('flush', e);
            }
        }, delay);
    }

    flushStale(gen) {
        return this.stale(gen) || this.state !== 'connected' || !this.producer;
    }

    async flushLoop(gen) {
        const queue = this.queue;
        let flushed = 0;
        let lastHead = null;
        for (;;) {
            if (this.flushStale(gen)) return;
            // Checked synchronously after the previous ack: enqueues are counted
            // at call time, so 0 means nothing is waiting and live mode is safe.
            if (queue.size === 0) {
                this.mode = 'live';
                this.flushBackoff.reset();
                if (flushed > 0) this.log(`Sent ${flushed} queued message(s) to Kafka; the offline queue is empty`);
                return;
            }
            const entries = await queue.peek(this.cfg.batchSize);
            if (this.flushStale(gen)) return;
            if (entries.length === 0) {
                await sleep(TIMEOUTS.emptyPoll); // records requested but not written yet
                continue;
            }
            // Every round acks at least its first entry. Should the queue ever
            // fail to move on, back off (flushCrashed) instead of spinning.
            const head = `${entries[0].pos.seg}:${entries[0].pos.end}`;
            if (head === lastHead) throw new Error('the offline queue did not advance after an acknowledgement');
            lastHead = head;
            const sent = await this.flushBatch(gen, queue, entries);
            if (sent < 0) return; // stopped: connection problem
            flushed += sent;
            this.render(true);
        }
    }

    /**
     * Decode a peeked batch. Returns the entries handled in this round, in queue
     * order: the longest prefix whose valid records share the first topic.
     * Corrupt and expired records are marked `dead`.
     */
    planFlush(entries) {
        const items = [];
        let topic = null;
        const now = Date.now();
        const maxAge = this.cfg.queue.maxAgeMs;
        for (const entry of entries) {
            const x = { entry, dead: null, message: null, topic: null, tf: 0 };
            if (entry.corrupt) {
                x.dead = new MessageError('Corrupt queue record (not valid JSON)');
                items.push(x);
                continue;
            }
            const rec = entry.record;
            try {
                if (!rec || typeof rec !== 'object' || rec.v !== RECORD_VERSION || typeof rec.t !== 'string' ||
                    !rec.m || typeof rec.m !== 'object') {
                    throw new MessageError('Invalid queue record (unknown format)');
                }
                const problem = topicProblem(rec.t, 'Queued topic');
                if (problem) throw new MessageError(problem);
                const message = serialize.decodeMessage(rec.m);
                if (message.value === undefined) throw new MessageError('Invalid queue record (no value)');
                x.message = message;
                x.topic = rec.t;
                x.tf = rec.tf === 1 ? 1 : 0;
            } catch (err) {
                x.dead = err instanceof MessageError ? err : new MessageError(`Invalid queue record: ${err && err.message}`);
                items.push(x);
                continue;
            }
            if (maxAge > 0 && Number.isFinite(rec.ts) && now - rec.ts > maxAge) {
                x.dead = `expired: queued longer than ${this.cfg.queue.maxAgeHours} h (queueMaxAgeHours)`;
                items.push(x);
                continue;
            }
            if (topic === null) topic = x.topic;
            else if (x.topic !== topic) break; // next round
            items.push(x);
        }
        return items;
    }

    /**
     * Classify a flush failure. TOPIC depends on where the topic came from. An
     * UNKNOWN error is retried like a connection problem, but when the same
     * head record fails UNKNOWN_LIMIT times in a row it is treated as a bad
     * message (`forced`), so one odd record cannot block the queue forever.
     *
     * @returns {{kind: string, forced: boolean}}
     */
    flushKind(err, first, unknownIsMessage) {
        let kind = classifyError(err, { op: 'send' }).kind;
        if (kind === ErrorKind.TOPIC) kind = first.tf ? ErrorKind.MESSAGE : ErrorKind.AUTH;
        if (kind !== ErrorKind.UNKNOWN) return { kind, forced: false };
        if (unknownIsMessage) return { kind: ErrorKind.MESSAGE, forced: true };
        const head = `${first.entry.pos.seg}:${first.entry.pos.end}`;
        if (this.unknownHead === head) {
            this.unknownCount++;
        } else {
            this.unknownHead = head;
            this.unknownCount = 1;
        }
        if (this.unknownCount >= UNKNOWN_LIMIT) return { kind: ErrorKind.MESSAGE, forced: true };
        return { kind, forced: false };
    }

    /**
     * Send one round of queued records, dead-letter what cannot be delivered,
     * ack what was handled. Returns the number of messages sent, or -1 when
     * flushing must stop (connection problem: the supervisor reconnects).
     */
    async flushBatch(gen, queue, entries) {
        const producer = this.producer;
        const items = this.planFlush(entries);
        const valid = items.filter((x) => !x.dead);
        let sent = 0;
        if (valid.length > 0) {
            const messages = valid.map((x) => x.message);
            try {
                const results = await producer.send(this.sendArgs(valid[0].topic, messages));
                this.unknownHead = null;
                this.unknownCount = 0;
                sent = valid.length - this.markMissingPartitions(valid, messages, results);
            } catch (err) {
                const { kind, forced } = this.flushKind(err, valid[0], false);
                if (kind !== ErrorKind.MESSAGE) {
                    this.markDisconnected(err, kind, 'flush');
                    return -1;
                }
                return this.flushIsolated(gen, queue, producer, items, forced);
            }
        }
        await this.deadLetterAll(queue, items.filter((x) => x.dead));
        await queue.ack(items.map((x) => x.entry));
        if (valid.length > 0) this.healthy(); // a produce request got through
        return sent;
    }

    /** After a successful send: records for partitions that do not exist were not written. */
    markMissingPartitions(valid, messages, results) {
        if (this.cfg.acks === 0) return 0;
        const missing = missingPartitions(messages, results, valid[0].topic);
        if (missing.length === 0) return 0;
        let n = 0;
        for (const x of valid) {
            if (x.message.partition !== undefined && missing.includes(x.message.partition)) {
                x.dead = new MessageError(`Partition ${x.message.partition} does not exist for topic ${x.topic}`);
                n++;
            }
        }
        return n;
    }

    /**
     * A batch was rejected because of (at least) one bad record: send the
     * records one at a time, dead-letter the bad ones, keep going. Stops (and
     * returns -1) on a connection-level failure, after acking what was handled.
     */
    async flushIsolated(gen, queue, producer, items, forced) {
        let handled = 0;
        let sent = 0;
        let stop = null;
        for (const x of items) {
            if (x.dead) {
                await this.deadLetterAll(queue, [x]);
                handled++;
                continue;
            }
            if (this.closing) break;
            try {
                const results = await producer.send(this.sendArgs(x.topic, [x.message]));
                sent += 1 - this.markMissingPartitions([x], [x.message], results);
                if (x.dead) await this.deadLetterAll(queue, [x]);
                handled++;
            } catch (err) {
                const { kind } = this.flushKind(err, x, forced);
                if (kind === ErrorKind.MESSAGE) {
                    x.dead = err;
                    await this.deadLetterAll(queue, [x]);
                    handled++;
                    continue;
                }
                stop = { err, kind };
                break;
            }
        }
        if (handled > 0) await queue.ack(items.slice(0, handled).map((x) => x.entry));
        if (sent > 0) this.healthy();
        if (stop) {
            this.markDisconnected(stop.err, stop.kind, 'flush');
            return -1;
        }
        return this.closing ? -1 : sent;
    }

    /** Append undeliverable records to dead-letter.jsonl. A failing write is logged, never blocks the queue. */
    async deadLetterAll(queue, list) {
        for (const x of list) {
            const reason = x.dead;
            const why = typeof reason === 'string' ? reason : describeError(reason);
            const where = queue.dir ? ` to ${path.join(queue.dir, 'dead-letter.jsonl')}` : ' (memory queue: kept in memory only)';
            try {
                await queue.deadLetter(x.entry, reason);
                this.warn(`Moved a queued message${x.topic ? ` for topic ${x.topic}` : ''} that cannot be delivered ` +
                    `${where}: ${why}`);
            } catch (err) {
                let raw = '';
                try {
                    raw = truncate(JSON.stringify(x.entry.corrupt ? x.entry.raw : x.entry.record), 500);
                } catch (_) {
                    raw = '[unprintable]';
                }
                this.error(`Dropped an undeliverable queued message (${why}); writing the dead-letter file failed: ` +
                    `${err && err.message}. Record: ${raw}`);
            }
        }
    }

    // ------------------------------------------------------------ control messages

    control(action, msg, send, done) {
        if (action === 'flush' || action === 'reconnect') {
            this.backoff.reset();
            if (this.state === 'waiting') this.connect();
            else if (this.state === 'connected') this.maybeStartFlusher();
            done();
            return;
        }
        if (action === 'queue-status') {
            msg.payload = this.stats();
            send(msg);
            done();
            return;
        }
        // queue-clear
        if (this.queueState !== 'open') {
            done(new Error(this.queueState === 'pending'
                ? 'The offline queue is still opening — try again'
                : 'The offline queue is off or unavailable — nothing to clear'));
            return;
        }
        this.queue.clear().then((n) => {
            if (n > 0) this.warn(`Cleared ${n} message(s) from the offline queue (they will not be sent)`);
            try {
                msg.payload = this.stats();
                send(msg);
                done();
            } catch (err) {
                done(err);
            }
            this.maybeStartFlusher();
            this.render();
        }, (err) => done(err)).catch((err) => this.bug('queue-clear', err));
    }

    /** queue-status payload. */
    stats() {
        let base = { size: 0, bytes: 0, segments: 0, droppedTotal: 0, deadLettered: 0 };
        try {
            if (this.queue) base = this.queue.stats();
        } catch (_) {
            // keep the zero stats
        }
        const s = {
            ...base,
            state: this.state,
            mode: this.mode,
            queueMode: this.queueState === 'open' || this.queueState === 'pending' ? this.cfg.queue.mode : 'off',
            queueDir: this.cfg.queue.mode === 'disk' ? this.queueDir : null,
            pending: this.pending.length + this.early.length
        };
        if (this.queueState === 'failed' && this.queueError) s.queueError = this.queueError.message;
        if (this.lastError) s.lastError = shortMessage(this.lastError, 80);
        return s;
    }

    // ------------------------------------------------------------ close

    /**
     * Node-RED close handler body. Calls `done` exactly once, within
     * TIMEOUTS.closeTotal whatever happens.
     */
    close(removed, done) {
        const finish = once(() => {
            clearTimeout(hardStop);
            try {
                done();
            } catch (_) {
                // nothing left to do
            }
        });
        const hardStop = setTimeout(finish, TIMEOUTS.closeTotal);
        if (hardStop && typeof hardStop.unref === 'function') hardStop.unref();
        try {
            this.shutdown(removed).then(finish, (err) => {
                this.bug('close', err);
                finish();
            });
        } catch (err) {
            this.bug('close', err);
            finish();
        }
    }

    shutdown(removed) {
        if (this.closePromise) return this.closePromise;
        this.closing = true;
        this.state = 'closing';
        this.gen++;
        this.clearRetryTimer();
        if (this.flushRetryTimer) clearTimeout(this.flushRetryTimer);
        this.flushRetryTimer = null;
        this.status.dispose();
        const dir = this.queueDir;
        const p = this.shutdownAsync(removed).catch((err) => this.bug('close', err));
        this.closePromise = p;
        if (dir) {
            // a new instance (redeploy) waits for this before opening the same directory
            queueClosers.set(dir, p);
            const forget = () => {
                if (queueClosers.get(dir) === p) queueClosers.delete(dir);
            };
            p.then(forget, forget);
        }
        return p;
    }

    async shutdownAsync(removed) {
        // 1. Let an in-flight produce request / flush batch finish (bounded).
        const busy = [this.workerPromise, this.flushPromise].filter(Boolean);
        if (busy.length > 0) await withTimeout(Promise.all(busy), TIMEOUTS.closeSendWait);
        if (this.queueState === 'pending' && this.queueOpening) {
            await withTimeout(this.queueOpening, TIMEOUTS.closeQueue);
        }

        // 2. Disconnect (bounded) while the unsent messages are persisted.
        const producer = this.producer;
        this.producer = null;
        const disconnecting = this.disconnectQuietly(producer);

        const chunks = [];
        const flight = this.inFlight;
        if (flight && !flight.persisted) {
            // the request did not answer in time: at-least-once, it may still land (duplicate)
            flight.persisted = true;
            const stored = flight.queued ? await withTimeout(flight.queued.stored, TIMEOUTS.closeQueue, false) : false;
            for (const chunk of flight.chunks) {
                if (chunk.state === 'inflight') chunk.state = stored ? 'queued' : 'pending';
            }
            if (stored) {
                for (const item of itemsOf(flight.chunks)) this.checkItem(item);
            } else {
                chunks.push(...flight.chunks);
            }
        }
        chunks.push(...this.pending.splice(0));
        for (const item of this.early.splice(0)) chunks.push(...item.chunks);
        const unsent = chunks.filter((c) => !c.item.settled && c.state === 'pending');
        if (unsent.length > 0) {
            const count = itemsOf(unsent).length;
            if (this.queueState === 'open' && this.cfg.queue.mode === 'disk') {
                await withTimeout(this.enqueueChunks(unsent), TIMEOUTS.closeQueue);
                this.log(`Stored ${count} unsent message(s) in the offline queue while closing`);
            } else {
                this.warn(`${count} message(s) were not sent to Kafka before the node closed` +
                    (this.cfg.queue.mode === 'memory' ? ' (the memory queue does not survive a restart)' : ''));
                const err = new Error('The Kafka producer node closed before the message was sent');
                for (const item of itemsOf(unsent)) this.settleFailed(item, err);
            }
        }
        await disconnecting;

        // 3. Close the queue and release its directory.
        const queue = this.queue;
        if (queue) {
            const left = queue.size;
            if (removed && left > 0 && queue.dir) {
                this.warn(`Node removed with ${left} unsent message(s) left in ${queue.dir}`);
            }
            await withTimeout(queue.close(), TIMEOUTS.closeQueue);
        }
        this.releaseDir();
    }

    /** State snapshot (tests, debugging). */
    snapshot() {
        return {
            state: this.state,
            mode: this.mode,
            queueState: this.queueState,
            pending: this.pending.length,
            early: this.early.length,
            working: this.working,
            flushing: this.flushing,
            gen: this.gen,
            queued: this.queuedCount()
        };
    }
}

module.exports = function (RED) {
    /**
     * Node constructor. Registers the handlers first and never throws (a
     * throwing constructor leaves a node whose close handler never runs).
     *
     * @param {object} config
     */
    function KafkaProducerNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        let runtime = null;
        let startError = null;

        node.on('close', function (removed, done) {
            const finish = once(() => {
                try {
                    done();
                } catch (_) {
                    // ignore
                }
            });
            try {
                if (runtime) runtime.close(removed, finish);
                else finish();
            } catch (_) {
                finish();
            }
        });

        node.on('input', function (msg, send, done) {
            try {
                if (runtime) {
                    runtime.input(msg, send, done);
                } else {
                    done(new Error(`The Kafka producer failed to start: ${startError && startError.message}`));
                }
            } catch (err) {
                try {
                    done(err);
                } catch (_) {
                    // ignore
                }
            }
        });

        try {
            runtime = new ProducerRuntime(RED, node, config);
            node.producerRuntime = runtime;
        } catch (err) {
            startError = err;
            try {
                node.error(`The Kafka producer failed to start: ${describeError(err)}`);
                node.status({ fill: 'red', shape: 'ring', text: 'internal error' });
            } catch (_) {
                // ignore
            }
        }
    }

    RED.nodes.registerType('kafkajs-nc-producer', KafkaProducerNode, {
        settings: {
            kafkajsNcProducerQueueDir: { value: '', exportable: false }
        }
    });
};

/** Test hooks: shorten timeouts / lower limits in tests; production code never changes them. */
module.exports.TIMEOUTS = TIMEOUTS;
module.exports.LIMITS = LIMITS;
/** Pure helpers, exported for unit tests. */
module.exports._internals = {
    parseConfig,
    parseStaticHeaders,
    queueDirFor,
    buildEntries,
    toRecord,
    locateMessages,
    missingPartitions,
    topicProblem,
    fmtDelay,
    ProducerRuntime,
    queueClosers,
    UNKNOWN_LIMIT
};
