'use strict';

/**
 * kafkajs-nc-consumer: subscribes to Kafka topics and sends every record into
 * the flow as a message. The node's input takes control messages (pause,
 * resume, stop, start, restart, seek, commit).
 *
 * The "supervisor" owns the consumer's whole life:
 *
 *   connect() ─► new Kafka + consumer ─► connect ─► subscribe ─► run
 *        ▲                                                         │
 *        │      GROUP_JOIN ⇒ connected, re-apply the wanted pause ◄┘
 *        │      CRASH / any failure ⇒ drop that consumer, then
 *        └──────────── retry after a backoff (slow for auth/config problems)
 *
 * Every connect attempt gets a NEW kafkajs client and a generation number.
 * kafkajs cannot cancel a connect, a join or its own restart, so whatever
 * finishes for an old generation (or after close) is disconnected instead of
 * being used: no zombie group members after a redeploy. We pass
 * `restartOnFailure: async () => false`, so kafkajs never restarts a consumer
 * by itself; the supervisor creates a fresh one instead (which also makes
 * pausing survive restarts, see 5-kafkajs-internals.md §3).
 *
 * Crash safety (SPEC §0): the input and close handlers are not async, every
 * promise has a rejection handler, kafkajs listeners go through safeOn(),
 * eachMessage never throws (a throw there would stop the consumer or make
 * kafkajs re-deliver the same message forever), and close always calls
 * done() within 10 s.
 */

const { Backoff } = require('../lib/backoff');
const { StatusReporter } = require('../lib/status');
const {
    ErrorKind, MessageError, classifyError, describeError, shortMessage, problemText, connectionKind, asConfigError
} = require('../lib/errors');
const { decodeValue, fromKafkaHeaders } = require('../lib/serialize');
const {
    toInt, toNumber, toBool, safeOn, once, withTimeout, noop, sanitizeName, truncate, formatDelay: fmtDelay
} = require('../lib/util');

/**
 * Timeouts in ms. Exported as `module.exports.TIMEOUTS` so that tests can
 * shorten them; production code never changes them.
 */
const TIMEOUTS = {
    /** bound for consumer.disconnect(): it waits for in-flight requests (up to requestTimeout on a hung broker) */
    disconnect: 5000,
    /** close: done() is called after this long whatever happens (Node-RED gives up after 15 s) */
    closeTotal: 10000,
    /**
     * A consumer that stayed in its group this long counts as healthy: its next
     * failure starts a new outage (backoff from the minimum, logged as an
     * error). A consumer that fails sooner after joining keeps backing off, so
     * a join-then-crash loop cannot spin at full speed.
     */
    stable: 30000
};

/** kafkajs must never restart a consumer itself (it MUST be async: a sync one crashes the process). */
const RESTART_ON_FAILURE = async () => false;

/** kafkajs consumer event names (used when a consumer object does not expose `.events`). */
const DEFAULT_EVENTS = Object.freeze({
    CONNECT: 'consumer.connect',
    DISCONNECT: 'consumer.disconnect',
    GROUP_JOIN: 'consumer.group_join',
    CRASH: 'consumer.crash',
    REBALANCING: 'consumer.rebalancing',
    START_BATCH_PROCESS: 'consumer.start_batch_process'
});

const ACTIONS = ['pause', 'resume', 'stop', 'start', 'restart', 'seek', 'commit'];
const ACTION_LIST = ACTIONS.join(', ');

/** Kafka topic names: letters, digits, '.', '_' and '-', at most 249 characters. */
const TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/;
/** Node-RED replaces `${VAR}` / `$(VAR)` only when it is the whole value, and leaves it as-is when VAR is not set. */
const ENV_REF_WHOLE = /^\s*(?:\$\{([^}]*)\}|\$\(([^)]*)\))\s*$/;
const ENV_REF_ANY = /\$\{([^}]*)\}/;

/** Allowed ranges of the numeric settings (the editor saves strings; kafkajs needs numbers). */
const LIMITS = Object.freeze({
    autoCommitInterval: { min: 0, max: 86400000 },
    autoCommitThreshold: { min: 0, max: 1000000000 },
    sessionTimeout: { min: 1000, max: 3600000 },
    heartbeatInterval: { min: 100, max: 3600000 },
    rebalanceTimeout: { min: 1000, max: 3600000 },
    maxBytesPerPartition: { min: 1, max: 2147483647 },
    maxBytes: { min: 1, max: 2147483647 },
    maxWaitTimeInMs: { min: 0, max: 300000 },
    partitionsConsumedConcurrently: { min: 1, max: 100 }
});

const DEFAULTS = Object.freeze({
    autoCommitInterval: 5000,
    autoCommitThreshold: 0,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    maxBytes: 10485760,
    // kafkajs defaults to 5000, but stopping a consumer waits for the fetch that is in
    // flight: with 5000 every redeploy of an idle consumer took ~5 s (measured with a
    // real broker). 1000 keeps deploys fast; idle polling once a second is negligible.
    maxWaitTimeInMs: 1000,
    partitionsConsumedConcurrently: 1
});

/** Extra explanations for errors whose kafkajs text does not say what to do. */
const HINTS = {
    GROUP_COORDINATOR_NOT_FOUND: 'this usually means the user is not authorised for the consumer group',
    GROUP_AUTHORIZATION_FAILED: 'the user is not authorised for the consumer group',
    UNKNOWN_TOPIC_OR_PARTITION: 'check that the topic exists, or allow automatic topic creation',
    TOPIC_AUTHORIZATION_FAILED: 'the user is not authorised to read the topic',
    CODEC_MISSING: 'the topic contains messages in a compression format this node cannot decode (zstd is not supported)'
};

// ---------------------------------------------------------------------------
// configuration

function choice(value, allowed, def) {
    const v = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return allowed.includes(v) ? v : def;
}

/** A leftover `${VAR}` means the environment variable was not set. */
function envProblem(value) {
    const m = ENV_REF_WHOLE.exec(value) || ENV_REF_ANY.exec(value);
    return m ? `environment variable ${(m[1] || m[2] || '').trim()} is not set` : null;
}

/**
 * Topic regex from the configuration: a plain source (`^orders-.*`) or the
 * literal form `/^orders-.*$/i`. The g and y flags are dropped: they make
 * RegExp.test() stateful, and kafkajs tests every topic name with it.
 *
 * @param {string} text
 * @returns {RegExp} (throws SyntaxError for an invalid expression)
 */
function parseTopicRegex(text) {
    const literal = /^\/([\s\S]+)\/([a-z]*)$/.exec(text);
    if (literal) return new RegExp(literal[1], literal[2].replace(/[gy]/g, ''));
    return new RegExp(text);
}

/**
 * Turn the node configuration (strings from the editor) into checked values.
 * Never throws.
 *
 * @param {object} config
 * @param {string} nodeId
 * @returns {{cfg: object, problems: string[], warnings: string[]}}
 */
function parseConfig(config, nodeId) {
    const c = config && typeof config === 'object' ? config : {};
    const problems = [];
    const warnings = [];

    let topicsText = '';
    try {
        topicsText = Array.isArray(c.topics) ? c.topics.join(',') : (c.topics == null ? '' : String(c.topics));
    } catch (_) {
        topicsText = '';
    }
    topicsText = topicsText.trim();
    const topicIsRegex = toBool(c.topicIsRegex, false);
    let topics = [];
    let regex = null;
    if (topicsText === '') {
        problems.push('No topic configured: set the topic(s) to subscribe to');
    } else if (topicIsRegex) {
        const env = ENV_REF_WHOLE.test(topicsText) ? envProblem(topicsText) : null;
        if (env) {
            problems.push(`Topic: ${env}`);
        } else {
            try {
                regex = parseTopicRegex(topicsText);
            } catch (err) {
                problems.push(`Invalid topic regular expression ${JSON.stringify(topicsText)}: ${err && err.message}`);
            }
        }
    } else {
        const seen = new Set();
        for (const token of topicsText.split(/[\s,]+/)) {
            if (token === '' || seen.has(token)) continue;
            seen.add(token);
            const env = envProblem(token);
            if (env) problems.push(`Topic: ${env}`);
            else if (!TOPIC_RE.test(token) || token === '.' || token === '..') {
                problems.push(`Invalid topic name ${JSON.stringify(truncate(token, 60))} ` +
                    '(letters, digits, ".", "_" and "-", at most 249 characters)');
            } else {
                topics.push(token);
            }
        }
        if (topics.length === 0 && problems.length === 0) problems.push('No topic configured: set the topic(s) to subscribe to');
        if (problems.length > 0) topics = [];
    }

    let groupId = c.groupId == null ? '' : String(c.groupId).trim();
    if (groupId !== '' && ENV_REF_WHOLE.test(groupId)) {
        problems.push(`Group ID: ${envProblem(groupId)}`);
        groupId = '';
    }
    // Stable across restarts and redeploys (the node id does not change), so
    // the group's committed offsets are found again. A random id would skip
    // or replay messages and leave orphan groups behind (3-static-audit D6).
    if (groupId === '') groupId = `node-red-${sanitizeName(nodeId)}`;

    const num = (name) => toInt(c[name], DEFAULTS[name], LIMITS[name]);
    const sessionTimeout = num('sessionTimeout');
    let heartbeatInterval = num('heartbeatInterval');
    if (heartbeatInterval >= sessionTimeout) {
        // kafkajs throws for this; the usual recommendation is a third of the session timeout
        const corrected = Math.max(1, Math.floor(sessionTimeout / 3));
        warnings.push(`The heartbeat interval (${heartbeatInterval} ms) must be lower than the session timeout ` +
            `(${sessionTimeout} ms): using ${corrected} ms`);
        heartbeatInterval = corrected;
    }
    const autoCommitInterval = num('autoCommitInterval');
    const autoCommitThreshold = num('autoCommitThreshold');

    const cfg = {
        topics,
        regex,
        topicIsRegex,
        topicsLabel: regex ? String(regex) : topics.join(', '),
        groupId,
        fromBeginning: toBool(c.fromBeginning, false),
        payloadFormat: choice(c.payloadFormat, ['string', 'buffer', 'json'], 'string'),
        keyFormat: choice(c.keyFormat, ['string', 'buffer'], 'string'),
        headersFormat: choice(c.headersFormat, ['string', 'buffer'], 'string'),
        commitMode: choice(c.commitMode, ['auto', 'manual'], 'auto'),
        // 0 means "not set": kafkajs then commits after every batch
        autoCommitInterval: autoCommitInterval > 0 ? autoCommitInterval : undefined,
        autoCommitThreshold: autoCommitThreshold > 0 ? autoCommitThreshold : undefined,
        sessionTimeout,
        heartbeatInterval,
        rebalanceTimeout: num('rebalanceTimeout'),
        maxBytesPerPartition: num('maxBytesPerPartition'),
        maxBytes: num('maxBytes'),
        maxWaitTimeInMs: num('maxWaitTimeInMs'),
        partitionsConsumedConcurrently: num('partitionsConsumedConcurrently'),
        allowAutoTopicCreation: toBool(c.allowAutoTopicCreation, false),
        autoStart: toBool(c.autoStart, true)
    };
    return { cfg, problems, warnings };
}

// ---------------------------------------------------------------------------
// control message parameters

/** A partition number (number or numeric string), else a MessageError. */
function parsePartition(value, where) {
    const ok = (typeof value === 'number' && Number.isInteger(value)) ||
        (typeof value === 'string' && /^\s*\d+\s*$/.test(value));
    const n = ok ? Number(value) : NaN;
    if (!(n >= 0 && n <= 2147483647)) {
        throw new MessageError(`${where}: partition must be a whole number >= 0 (got ${describeValue(value)})`);
    }
    return n;
}

/** Kafka offsets are signed 64-bit integers. */
const MAX_OFFSET = 9223372036854775807n;

/** An offset 0..2^63-1 (number, numeric string or bigint) as a BigInt, else a MessageError. */
function parseOffset(value, where, alternatives = '') {
    let n = null;
    if (typeof value === 'bigint') n = value;
    else if (typeof value === 'number' && Number.isSafeInteger(value)) n = BigInt(value);
    else if (typeof value === 'string' && /^\s*\d+\s*$/.test(value)) n = BigInt(value.trim());
    if (n === null || n < 0n || n > MAX_OFFSET) {
        throw new MessageError(`${where}: offset must be a whole number >= 0${alternatives} (got ${describeValue(value)})`);
    }
    return n;
}

function describeValue(v) {
    try {
        if (typeof v === 'string') return JSON.stringify(truncate(v, 40));
        if (typeof v === 'bigint') return `${v}n`;
        if (v !== null && typeof v === 'object') return Array.isArray(v) ? 'an array' : 'an object';
        return String(v);
    } catch (_) {
        return typeof v;
    }
}

/** The topic of a control message: given, or the node's only (non-regex) topic. */
function controlTopic(value, cfg, where, field) {
    if (typeof value === 'string' && value.trim() !== '') return value.trim();
    if (value !== undefined && value !== null && value !== '') {
        throw new MessageError(`${where}: topic must be a string`);
    }
    if (!cfg.regex && cfg.topics.length === 1) return cfg.topics[0];
    throw new MessageError(`${where}: ${field} is required because the node subscribes to ` +
        (cfg.regex ? 'a topic pattern' : 'several topics'));
}

const SPECIAL_SEEK = new Map([['earliest', '-2'], ['beginning', '-2'], ['latest', '-1'], ['end', '-1'], ['-2', '-2'], ['-1', '-1']]);

/**
 * msg.payload of a 'seek' control message → kafkajs seek() argument.
 * Offsets: a number >= 0, or 'earliest' / 'latest'.
 */
function parseSeek(payload, cfg) {
    const where = 'seek';
    const p = payload !== null && typeof payload === 'object' && !Array.isArray(payload) && !Buffer.isBuffer(payload)
        ? payload
        : null;
    if (!p) throw new MessageError('seek: msg.payload must be an object { topic, partition, offset }');
    const topic = controlTopic(p.topic, cfg, where, 'msg.payload.topic');
    if (p.partition === undefined || p.partition === null || p.partition === '') {
        throw new MessageError('seek: msg.payload.partition is required');
    }
    const partition = parsePartition(p.partition, where);
    const special = SPECIAL_SEEK.get(String(p.offset).trim().toLowerCase());
    const offset = special !== undefined
        ? special
        : parseOffset(p.offset, where, ', "earliest" or "latest"').toString();
    return { topic, partition, offset };
}

/**
 * A 'commit' control message → the kafkajs commitOffsets() entry. Takes
 * topic/partition/offset from msg.kafka (as set on every output message), else
 * from msg. Kafka stores the offset of the NEXT message to read, hence +1.
 */
function parseCommit(msg, cfg) {
    const where = 'commit';
    const k = msg.kafka !== null && typeof msg.kafka === 'object' ? msg.kafka : {};
    const pick = (name) => (k[name] !== undefined && k[name] !== null ? k[name] : msg[name]);
    const topic = controlTopic(pick('topic'), cfg, where, 'msg.kafka.topic (or msg.topic)');
    const rawPartition = pick('partition');
    if (rawPartition === undefined || rawPartition === null || rawPartition === '') {
        throw new MessageError('commit: msg.kafka.partition (or msg.partition) is required');
    }
    const partition = parsePartition(rawPartition, where);
    const rawOffset = pick('offset');
    if (rawOffset === undefined || rawOffset === null || rawOffset === '') {
        throw new MessageError('commit: msg.kafka.offset (or msg.offset) is required');
    }
    const processed = parseOffset(rawOffset, where);
    if (processed >= MAX_OFFSET) throw new MessageError(`commit: offset ${processed} is too large`);
    return { topic, partition, offset: (processed + 1n).toString() };
}

// ---------------------------------------------------------------------------
// output messages

/** A private copy: kafkajs buffers are slices of the whole fetch response, which a stored slice would keep in memory. */
function ownBuffer(b) {
    return Buffer.isBuffer(b) ? Buffer.from(b) : b;
}

function copyHeaderBuffers(headers) {
    for (const name of Object.keys(headers)) {
        const v = headers[name];
        headers[name] = Array.isArray(v) ? v.map(ownBuffer) : ownBuffer(v);
    }
    return headers;
}

function rawText(value) {
    try {
        return decodeValue(value, 'string');
    } catch (_) {
        return null;
    }
}

// ---------------------------------------------------------------------------
// runtime

/**
 * Everything one consumer node does at runtime. Kept out of the Node-RED node
 * object so method names can never collide with Node's own (send, close, ...).
 */
class ConsumerRuntime {
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
        /** bumped on every connect attempt, failure, stop and close: work of an older generation stops */
        this.gen = 0;
        /** 'idle' | 'connecting' | 'connected' | 'waiting' | 'stopped' | 'closing' */
        this.state = 'idle';
        /** stopped by msg.action 'stop' (or autoStart off): no automatic reconnect */
        this.stopped = false;
        this.consumer = null;
        this.retryTimer = null;
        /** disconnects still running (each bounded by TIMEOUTS.disconnect once it has started) */
        this.pendingDisconnects = new Set();
        /** consumer → promise that settles (never rejects) when its run() call has settled */
        this.runPending = new Map();
        this.joining = false;
        this.rebalancing = false;
        this.joinedAt = 0;
        /** wanted pause state: kafkajs forgets pauses when a consumer restarts, so it is re-applied after every join */
        this.pauseWanted = false;
        /** topics to pause/resume: the configured ones plus those seen in assignments (regex subscriptions) */
        this.knownTopics = new Set();
        this.highWatermarks = new Map();

        // failure bookkeeping (log de-duplication, status)
        this.failures = 0;
        this.lastFailKey = null;
        this.lastError = null;
        this.lastKind = null;
        this.retryInMs = 0;
        this.recoveryLogged = false;

        const parsed = parseConfig(config, node.id);
        this.cfg = parsed.cfg;
        this.problems = parsed.problems;
        this.warnings = parsed.warnings;
        for (const topic of this.cfg.topics) this.knownTopics.add(topic);

        this.broker = null;
        try {
            const id = config && config.broker;
            this.broker = id ? RED.nodes.getNode(id) || null : null;
        } catch (_) {
            this.broker = null;
        }
        /** a problem that stops the node from ever connecting in this deployment: { status, texts[] } */
        this.fatal = this.findFatal();

        const r = (this.broker && this.broker.reconnect) || {};
        this.reconnect = {
            min: toInt(r.min, 1000, { min: 10, max: 3600000 }),
            max: toInt(r.max, 60000, { min: 10, max: 3600000 }),
            authMin: toInt(r.authMin, 60000, { min: 10, max: 3600000 })
        };
        this.backoff = new Backoff({ min: this.reconnect.min, max: this.reconnect.max });

        // Statuses and errors emitted synchronously in a constructor never reach
        // Status/Catch nodes, so the real work starts after construction.
        setImmediate(() => {
            try {
                this.start();
            } catch (err) {
                this.bug('start', err);
            }
        });
    }

    findFatal() {
        const texts = [];
        let status = null;
        if (!this.broker) {
            status = 'no broker config';
            texts.push('No Kafka broker configuration selected');
        } else if (this.broker.configError) {
            const e = this.broker.configError;
            const details = Array.isArray(e.details) && e.details.length > 0 ? e.details : [e.message];
            status = 'config error';
            texts.push(`Invalid Kafka broker configuration: ${details.join('; ')}`);
        }
        if (this.problems.length > 0) {
            if (!status) status = this.cfg.topicIsRegex && /regular expression/.test(this.problems.join(' '))
                ? 'invalid topic regex'
                : (/No topic/.test(this.problems.join(' ')) ? 'no topic' : 'config error');
            texts.push(`Invalid Kafka consumer configuration: ${this.problems.join('; ')}`);
        }
        return status ? { status, texts } : null;
    }

    // ------------------------------------------------------------ logging

    log(text) {
        try {
            this.node.log(text);
        } catch (_) {
            // logging must never break the consumer
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
        if (this.closing) return;
        this.error(`Internal error (${where}): ${describeError(err)}`);
    }

    // ------------------------------------------------------------ status

    /** Show the status derived from the current state. */
    render() {
        if (this.closing) return;
        try {
            if (this.fatal) {
                this.status.error(this.fatal.status);
                return;
            }
            const topics = this.cfg.topicsLabel;
            switch (this.state) {
            case 'stopped':
                this.status.idle('stopped');
                break;
            case 'connecting':
                this.status.connecting(this.joining ? 'joining group…' : 'connecting…');
                break;
            case 'connected':
                if (this.rebalancing) this.status.waiting('rebalancing…');
                else if (this.pauseWanted) this.status.set('yellow', 'dot', `paused · ${topics}`);
                else this.status.connected(`connected · ${topics}`);
                break;
            case 'waiting': {
                const retry = ` · retry ${fmtDelay(this.retryInMs)}`;
                if (this.lastKind === ErrorKind.AUTH || this.lastKind === ErrorKind.CONFIG) {
                    // needs a human: 'auth failed', 'config error', 'TLS: self-signed cert', ...
                    this.status.error(problemText(this.lastError, 28) + retry);
                } else {
                    this.status.waiting(shortMessage(this.lastError, 28) + retry);
                }
                break;
            }
            default:
                this.status.idle('starting');
            }
        } catch (_) {
            // status is cosmetic
        }
    }

    // ------------------------------------------------------------ startup

    /** Report configuration problems, then start connecting (unless told not to). */
    start() {
        if (this.closing) return;
        for (const w of this.warnings) this.warn(w);
        if (this.fatal) {
            for (const text of this.fatal.texts) this.error(text);
            this.render();
            return;
        }
        if (this.state !== 'idle') return; // a control message got here first
        if (!this.cfg.autoStart) {
            this.stopped = true;
            this.state = 'stopped';
            this.render();
            return;
        }
        this.connect();
    }

    // ------------------------------------------------------------ supervisor

    /** True when the work started under generation `gen` must stop. */
    stale(gen) {
        return this.closing || gen !== this.gen;
    }

    /** Start a connect attempt now (a new kafkajs client and consumer). */
    connect() {
        if (this.closing || this.stopped || this.fatal) return;
        this.clearRetryTimer();
        const gen = ++this.gen;
        this.state = 'connecting';
        this.joining = false;
        this.rebalancing = false;
        this.joinedAt = 0;
        this.render();
        this.connectAttempt(gen).catch((err) => this.bug('connect', err));
    }

    async connectAttempt(gen) {
        // Let the previous consumer leave the group first (bounded): two members
        // of the same group in one process would only cause an extra rebalance.
        if (this.pendingDisconnects.size > 0) {
            await withTimeout(Promise.all(Array.from(this.pendingDisconnects)), TIMEOUTS.disconnect);
            if (this.stale(gen)) return;
        }
        let consumer = null;
        try {
            const kafka = this.broker.createKafka(this.node);
            consumer = kafka.consumer(this.consumerOptions());
        } catch (err) {
            // createKafka() throws a ConfigError; kafka.consumer() only throws
            // synchronously for invalid options. Either way: a config problem.
            this.fail(gen, asConfigError(err, 'Could not create the Kafka consumer'), consumer, 'connection', ErrorKind.CONFIG);
            return;
        }
        if (this.stale(gen)) {
            this.dropConsumer(consumer);
            return;
        }
        this.consumer = consumer;
        this.watch(consumer, gen);
        if (!(await this.step(gen, consumer, () => consumer.connect()))) return;
        if (!(await this.step(gen, consumer, () => consumer.subscribe(this.subscription())))) return;
        // run() resolves once the group is joined; a failure to join arrives as
        // a CRASH event (handled by the listener) and run() still resolves.
        let running;
        try {
            running = Promise.resolve(consumer.run(this.runOptions()));
        } catch (err) {
            running = Promise.reject(err);
        }
        const settled = running.then(noop, noop);
        this.runPending.set(consumer, settled);
        settled.then(() => this.runPending.delete(consumer));
        await this.step(gen, consumer, () => running);
    }

    /**
     * Await one step of the connect sequence. Returns false when the attempt
     * must end: the step failed (handled here) or the node closed/moved on
     * meanwhile. kafkajs cannot cancel a connect or a join, so a consumer
     * whose step completes for a stale generation is shut down right away,
     * otherwise it would live on as a zombie group member.
     */
    async step(gen, consumer, fn) {
        try {
            await fn();
        } catch (err) {
            this.fail(gen, err, consumer, 'connection');
            return false;
        }
        if (this.stale(gen)) {
            this.dropConsumer(consumer);
            return false;
        }
        return true;
    }

    consumerOptions() {
        const c = this.cfg;
        return {
            groupId: c.groupId,
            sessionTimeout: c.sessionTimeout,
            rebalanceTimeout: c.rebalanceTimeout,
            heartbeatInterval: c.heartbeatInterval,
            maxBytesPerPartition: c.maxBytesPerPartition,
            maxBytes: c.maxBytes,
            maxWaitTimeInMs: c.maxWaitTimeInMs,
            allowAutoTopicCreation: c.allowAutoTopicCreation,
            // merged by kafkajs with the client's retry options (from the broker node)
            retry: { restartOnFailure: RESTART_ON_FAILURE }
        };
    }

    subscription() {
        return {
            topics: this.cfg.regex ? [this.cfg.regex] : this.cfg.topics.slice(),
            fromBeginning: this.cfg.fromBeginning
        };
    }

    runOptions() {
        const c = this.cfg;
        return {
            autoCommit: c.commitMode === 'auto',
            autoCommitInterval: c.autoCommitInterval,
            autoCommitThreshold: c.autoCommitThreshold,
            partitionsConsumedConcurrently: c.partitionsConsumedConcurrently,
            eachMessage: (payload) => this.eachMessage(payload)
        };
    }

    /** kafkajs instrumentation listeners (always through safeOn: a sync throw would crash Node-RED). */
    watch(consumer, gen) {
        const events = Object.assign({}, DEFAULT_EVENTS);
        try {
            const own = consumer.events;
            if (own && typeof own === 'object') {
                for (const key of Object.keys(DEFAULT_EVENTS)) {
                    if (typeof own[key] === 'string') events[key] = own[key];
                }
            }
        } catch (_) {
            // use the default names
        }
        const onListenerError = (err) => this.bug('kafka event', err);
        const on = (name, fn) => safeOn(consumer, events[name], fn, onListenerError);

        on('CONNECT', () => {
            if (this.stale(gen) || this.state !== 'connecting') return;
            this.joining = true; // CONNECT is emitted twice per start; harmless
            this.render();
        });
        on('GROUP_JOIN', (e) => this.onGroupJoin(gen, consumer, e && e.payload));
        on('CRASH', (e) => this.onCrash(gen, consumer, e && e.payload));
        on('REBALANCING', () => {
            if (this.stale(gen) || this.state !== 'connected') return;
            this.rebalancing = true;
            this.render();
        });
        on('DISCONNECT', () => {
            // kafkajs only emits it for disconnect() (ours, or its own before a CRASH)
            if (!this.stale(gen) && !this.stopped) this.debug('Kafka consumer disconnected');
        });
        on('START_BATCH_PROCESS', (e) => {
            const p = e && e.payload;
            if (!p || p.highWatermark === undefined || p.highWatermark === null) return;
            this.highWatermarks.set(`${p.topic}\u0000${p.partition}`, String(p.highWatermark));
        });
    }

    onGroupJoin(gen, consumer, payload) {
        if (this.stale(gen) || consumer !== this.consumer) {
            // A consumer we already gave up on joined anyway (a join that
            // completed after stop/restart/close, or a restart kafkajs did by
            // itself): shut it down as soon as kafkajs has started its runner.
            this.dropConsumer(consumer, true);
            return;
        }
        this.state = 'connected';
        this.joining = false;
        this.rebalancing = false;
        if (!this.joinedAt) this.joinedAt = Date.now();
        try {
            const assignment = payload && payload.memberAssignment;
            if (assignment && typeof assignment === 'object') {
                for (const topic of Object.keys(assignment)) this.knownTopics.add(topic);
            }
        } catch (_) {
            // the assignment only helps pausing regex subscriptions
        }
        if (this.failures > 0 && !this.recoveryLogged) {
            this.log(`Connected to Kafka after ${this.failures + 1} attempts`);
            this.recoveryLogged = true;
        }
        this.debug(`Joined consumer group "${this.cfg.groupId}"`);
        if (this.pauseWanted) {
            const err = this.applyPause(consumer);
            if (err) this.warn(`Could not pause the Kafka consumer after joining the group: ${describeError(err)}`);
        }
        this.render();
    }

    onCrash(gen, consumer, payload) {
        const p = payload || {};
        if (this.stale(gen) || consumer !== this.consumer) {
            // Already dropped (or the node is closing). If kafkajs is about to
            // restart it anyway, the deferred disconnect cancels that restart
            // (see dropConsumer).
            if (p.restart === true) this.dropConsumer(consumer, true);
            return;
        }
        const err = p.error || new Error('The Kafka consumer stopped unexpectedly');
        this.fail(gen, err, consumer, 'crash');
    }

    /**
     * A connect step failed or the running consumer crashed: drop that
     * consumer and retry after the backoff.
     */
    fail(gen, err, consumer, context, forcedKind) {
        // kafkajs sets its own restart timer right after emitting CRASH: a
        // disconnect deferred to the next turn of the event loop cancels it.
        if (consumer) this.dropConsumer(consumer, context === 'crash');
        if (this.stale(gen) || this.stopped) return;
        this.gen++; // events of the failed consumer are ignored from now on

        if (this.joinedAt && Date.now() - this.joinedAt >= TIMEOUTS.stable) this.resetFailures();
        this.joinedAt = 0;
        this.joining = false;
        this.rebalancing = false;

        // TOPIC ⇒ AUTH (a subscribed topic is missing or not authorised: retry
        // slowly), MESSAGE ⇒ UNKNOWN (no single message to blame); same as the producer
        const kind = forcedKind || connectionKind(err, 'consume');
        this.state = 'waiting';
        this.scheduleRetry(err, kind, context);
    }

    scheduleRetry(err, kind, context) {
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

    resetFailures() {
        this.backoff.reset();
        this.failures = 0;
        this.lastFailKey = null;
        this.recoveryLogged = false;
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
            const what = context === 'crash' ? 'Kafka consumer failed' : 'Kafka connection failed';
            const hint = HINTS[c.reason] || HINTS[c.type];
            const text = `${describeError(err)}${hint ? ` (${hint})` : ''}`;
            if (this.failures === 1 || key !== this.lastFailKey) {
                this.error(`${what}: ${text} — retrying in ${fmtDelay(delay)}`);
            } else if (this.failures % 10 === 0) {
                this.warn(`Kafka still failing after ${this.failures} attempts: ${text} — retrying in ${fmtDelay(delay)}`);
            } else {
                this.debug(`${what}: ${text} — retrying in ${fmtDelay(delay)}`);
            }
            this.lastFailKey = key;
        } catch (_) {
            // logging is best effort
        }
    }

    /**
     * Forget a consumer and disconnect it (never rejects; the disconnect itself
     * is bounded by TIMEOUTS.disconnect).
     *
     * When does the disconnect start?
     * - While the consumer's run() is still joining the group: only once run()
     *   has settled. kafkajs' stop() forgets a runner that has not started yet,
     *   and that orphaned runner would then start after the join anyway, out
     *   of reach of any later disconnect(): a zombie group member.
     * - With `defer`: on the next turn of the event loop. Used for CRASH (kafkajs
     *   sets its own restart timer right after emitting CRASH; the disconnect
     *   clears it) and for a stale GROUP_JOIN (kafkajs starts the runner right
     *   after emitting it, so stop() can then stop it properly).
     * - Otherwise: now.
     *
     * @returns {Promise<void>}
     */
    dropConsumer(consumer, defer) {
        if (!consumer) return Promise.resolve();
        if (this.consumer === consumer) this.consumer = null;
        const gate = this.runPending.get(consumer);
        const ready = gate || (defer ? new Promise((resolve) => setImmediate(resolve)) : Promise.resolve());
        const p = ready.then(() => this.disconnectQuietly(consumer));
        this.pendingDisconnects.add(p);
        p.then(() => this.pendingDisconnects.delete(p));
        return p;
    }

    /** consumer.disconnect(), bounded by TIMEOUTS.disconnect and never rejecting. */
    disconnectQuietly(consumer) {
        let p;
        try {
            p = consumer.disconnect();
        } catch (_) {
            return Promise.resolve();
        }
        return withTimeout(p, TIMEOUTS.disconnect);
    }

    // ------------------------------------------------------------ messages

    /**
     * kafkajs eachMessage handler. NEVER rejects: kafkajs would re-deliver the
     * same message forever (plain Error) or stop the consumer (TypeError).
     */
    async eachMessage(payload) {
        let msg = null;
        try {
            if (this.closing) return; // nothing can receive it any more
            const { topic, partition, message } = payload;
            msg = this.buildMessage(topic, partition, message);
            this.noteTopic(topic);
            let value;
            try {
                value = decodeValue(message.value, this.cfg.payloadFormat);
            } catch (err) {
                // Invalid JSON: hand the raw text to Catch nodes and move on (the
                // offset is still committed: re-reading would fail the same way).
                msg.payload = rawText(message.value);
                const e = new SyntaxError(`Invalid JSON in message ${topic}[${partition}] offset ${msg.offset}: ` +
                    `${err && err.message}`);
                this.node.error(e, msg);
                return;
            }
            msg.payload = this.cfg.payloadFormat === 'buffer' ? ownBuffer(value) : value;
            this.node.send(msg);
        } catch (err) {
            try {
                const p = payload || {};
                this.node.error(err, msg || { topic: p.topic, partition: p.partition });
            } catch (_) {
                // nothing left to report to
            }
        }
    }

    /** The output message without its payload (a new object, never the kafkajs record). */
    buildMessage(topic, partition, message) {
        const m = message || {};
        const offset = m.offset === undefined || m.offset === null ? undefined : String(m.offset);
        const timestamp = toNumber(m.timestamp, undefined);
        const key = m.key === undefined || m.key === null
            ? null
            : (this.cfg.keyFormat === 'buffer' ? ownBuffer(decodeValue(m.key, 'buffer')) : decodeValue(m.key, 'string'));
        const asBuffer = this.cfg.headersFormat === 'buffer';
        let headers = fromKafkaHeaders(m.headers, { asBuffer });
        if (asBuffer) headers = copyHeaderBuffers(headers);
        const kafka = { groupId: this.cfg.groupId, topic, partition, offset, timestamp };
        const hw = this.highWatermarks.get(`${topic}\u0000${partition}`);
        if (hw !== undefined) kafka.highWatermark = hw;
        return { payload: null, topic, key, partition, offset, timestamp, headers, kafka };
    }

    /** A topic first seen in a message (regex subscription) is paused too if a pause is wanted. */
    noteTopic(topic) {
        if (typeof topic !== 'string' || this.knownTopics.has(topic)) return;
        this.knownTopics.add(topic);
        if (this.pauseWanted && this.state === 'connected' && this.consumer) this.applyPause(this.consumer);
    }

    /** consumer.pause() for every known topic. Returns the error instead of throwing. */
    applyPause(consumer) {
        return this.applyToTopics(consumer, 'pause');
    }

    applyResume(consumer) {
        return this.applyToTopics(consumer, 'resume');
    }

    applyToTopics(consumer, method) {
        if (!consumer || this.knownTopics.size === 0) return null;
        try {
            consumer[method](Array.from(this.knownTopics, (topic) => ({ topic })));
            return null;
        } catch (err) {
            return err || new Error(`consumer.${method}() failed`);
        }
    }

    // ------------------------------------------------------------ control input

    /**
     * Node-RED input handler body (synchronous). Every outcome ends in
     * exactly one call of `done`.
     */
    input(msg, done) {
        // done(err) clones msg for Catch nodes and throws when it cannot (e.g. a
        // throwing getter): never let that escape, but log it.
        const finish = once((err) => {
            try {
                if (err) done(err);
                else done();
            } catch (e) {
                this.error(`Could not complete the message: ${(err && err.message) || 'ok'} (${e && e.message})`);
            }
        });
        try {
            const raw = msg && msg.action;
            const action = typeof raw === 'string' ? raw.trim().toLowerCase() : '';
            if (raw === undefined || raw === null || (typeof raw === 'string' && action === '')) {
                throw new MessageError('msg.action is required: this node only accepts control messages ' +
                    `(${ACTION_LIST})`);
            }
            if (!ACTIONS.includes(action)) {
                throw new MessageError(`Unknown msg.action ${describeValue(raw)}: use one of ${ACTION_LIST}`);
            }
            if (this.closing) {
                finish();
                return;
            }
            if (this.fatal) throw new Error(this.fatal.texts.join('; '));
            this.control(action, msg, finish);
        } catch (err) {
            finish(err);
        }
    }

    control(action, msg, finish) {
        switch (action) {
        case 'pause': {
            this.pauseWanted = true;
            let err = null;
            if (this.state === 'connected' && this.consumer) err = this.applyPause(this.consumer);
            this.render();
            // when not running yet, the pause is applied after the group is joined
            finish(err);
            break;
        }
        case 'resume': {
            this.pauseWanted = false;
            let err = null;
            if (this.state === 'connected' && this.consumer) err = this.applyResume(this.consumer);
            this.render();
            finish(err);
            break;
        }
        case 'stop': {
            this.stopped = true;
            this.state = 'stopped';
            this.gen++;
            this.clearRetryTimer();
            this.joinedAt = 0;
            const consumer = this.consumer;
            this.render();
            // done() once the consumer has left the group (bounded: it may still be joining)
            withTimeout(this.dropConsumer(consumer), TIMEOUTS.disconnect).then(() => finish());
            break;
        }
        case 'start':
            if (!this.stopped && this.state !== 'idle') {
                finish(); // already running or connecting
                break;
            }
            this.stopped = false;
            this.resetFailures();
            this.connect();
            finish();
            break;
        case 'restart':
            this.stopped = false;
            this.resetFailures();
            this.dropConsumer(this.consumer);
            this.connect(); // waits for the old consumer to leave the group first
            finish();
            break;
        case 'seek': {
            const target = parseSeek(msg.payload, this.cfg);
            this.requireRunning('seek');
            this.consumer.seek(target);
            finish();
            break;
        }
        case 'commit': {
            const entry = parseCommit(msg, this.cfg);
            this.requireRunning('commit');
            // commitOffsets() is async in kafkajs (it rejects when not running) but
            // may also throw synchronously: both end in done(err)
            Promise.resolve(this.consumer.commitOffsets([entry])).then(
                () => finish(),
                (err) => finish(err || new Error('commit failed'))
            );
            break;
        }
        default:
            finish(new MessageError(`Unknown msg.action "${action}": use one of ${ACTION_LIST}`));
        }
    }

    requireRunning(action) {
        if (this.state !== 'connected' || !this.consumer) {
            throw new Error(`Cannot ${action}: the Kafka consumer is not running (${this.state})`);
        }
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
            this.closing = true;
            this.state = 'closing';
            this.gen++;
            this.clearRetryTimer();
            this.status.dispose();
            this.dropConsumer(this.consumer);
            // The group member leaves before a redeployed node joins again
            // (bounded: a hung broker must not block the redeploy).
            withTimeout(Promise.all(Array.from(this.pendingDisconnects)), TIMEOUTS.disconnect).then(finish, finish);
        } catch (_) {
            finish();
        }
    }

    /** State snapshot (tests, debugging). */
    snapshot() {
        return {
            state: this.state,
            stopped: this.stopped,
            gen: this.gen,
            pauseWanted: this.pauseWanted,
            failures: this.failures,
            retryInMs: this.retryInMs,
            groupId: this.cfg.groupId,
            topics: Array.from(this.knownTopics)
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
    function KafkaConsumerNode(config) {
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
                    runtime.input(msg, done);
                } else {
                    done(new Error(`The Kafka consumer failed to start: ${startError && startError.message}`));
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
            runtime = new ConsumerRuntime(RED, node, config);
            node.consumerRuntime = runtime;
        } catch (err) {
            startError = err;
            try {
                node.error(`The Kafka consumer failed to start: ${describeError(err)}`);
                node.status({ fill: 'red', shape: 'ring', text: 'internal error' });
            } catch (_) {
                // ignore
            }
        }
    }

    RED.nodes.registerType('kafkajs-nc-consumer', KafkaConsumerNode);
};

/** Test hooks: shorten timeouts in tests; production code never changes them. */
module.exports.TIMEOUTS = TIMEOUTS;
/** Pure helpers, exported for unit tests. */
module.exports._internals = {
    parseConfig,
    parseTopicRegex,
    parseSeek,
    parseCommit,
    fmtDelay,
    ConsumerRuntime,
    LIMITS,
    DEFAULTS
};
