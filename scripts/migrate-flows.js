#!/usr/bin/env node
'use strict';

/**
 * Convert Node-RED flows that use node-red-contrib-kafkajs (by emrebekar) to
 * node-red-contrib-kafkajs-no-crash.
 *
 *   node scripts/migrate-flows.js <flows.json> [--out <file>] [--credentials <flows_cred.json>] [--in-place]
 *
 * The package installs it as the command `kafkajs-nc-migrate-flows` (package.json
 * "bin"), so it also runs without a checkout:
 *
 *   npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows flows.json
 *
 * - kafkajs-client   → kafkajs-nc-broker   (+ a core tls-config node when it used TLS files)
 * - kafkajs-producer → kafkajs-nc-producer
 * - kafkajs-consumer → kafkajs-nc-consumer
 *
 * Node ids, positions, groups and wires are kept, so links, catch/status
 * scopes and the rest of the flow stay intact. Every property is mapped to
 * the name and value type the new nodes use (see docs/MIGRATION.md for the
 * tables). The result goes to a new file; the input is only replaced with
 * --in-place, after a backup copy (.bak) has been written.
 *
 * Credentials: flows files never contain them. With --credentials pointing at
 * an UNENCRYPTED flows_cred.json, the SASL username/password are renamed for
 * the broker node and the TLS passphrase is moved into the new tls-config
 * node; the result is written next to the output flows file
 * (<name>_cred.json, Node-RED's naming rule). An encrypted file is detected
 * and left alone: the report then asks you to enter the credentials again.
 *
 * Only Node.js core modules are used, so the file also works on its own
 * (copied out of the repository).
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// what the new nodes expect (mirrors the `defaults` of nodes/*.html; a unit
// test checks that both stay in sync)

const TYPE_MAP = Object.freeze({
    'kafkajs-client': 'kafkajs-nc-broker',
    'kafkajs-producer': 'kafkajs-nc-producer',
    'kafkajs-consumer': 'kafkajs-nc-consumer'
});

const BROKER_DEFAULTS = Object.freeze({
    name: '',
    brokers: 'localhost:9092',
    clientId: 'node-red',
    authMechanism: 'none',
    tls: false,
    tlsConfig: '',
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
    requestTimeout: 30000,
    retries: 5,
    initialRetryTime: 300,
    maxRetryTime: 30000,
    reconnectMin: 1000,
    reconnectMax: 60000,
    authRetryMin: 60000,
    logLevel: 'warn',
    safetyNet: true
});

const PRODUCER_DEFAULTS = Object.freeze({
    name: '',
    broker: '',
    topic: '',
    key: '',
    partition: '',
    headers: Object.freeze({}),
    useMsgHeaders: true,
    acks: 'all',
    ackTimeout: 30000,
    compression: 'none',
    partitioner: 'default',
    allowAutoTopicCreation: true,
    batchSize: 100,
    queueMode: 'disk',
    queueDir: '',
    queueName: '',
    queueMaxMB: 100,
    queueMaxMessages: 0,
    queueOverflow: 'drop-oldest',
    queueMaxAgeHours: 0,
    queueFsync: true
});

const CONSUMER_DEFAULTS = Object.freeze({
    name: '',
    broker: '',
    topics: '',
    topicIsRegex: false,
    groupId: '',
    fromBeginning: false,
    autoStart: true,
    payloadFormat: 'string',
    keyFormat: 'string',
    headersFormat: 'string',
    commitMode: 'auto',
    autoCommitInterval: 5000,
    autoCommitThreshold: 0,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    maxBytes: 10485760,
    maxWaitTimeInMs: 1000,
    partitionsConsumedConcurrently: 1,
    allowAutoTopicCreation: false
});

/** Node-RED's core tls-config node as the editor saves it (its constructor needs cert/key/ca strings). */
const TLS_CONFIG_DEFAULTS = Object.freeze({
    name: '',
    cert: '',
    key: '',
    ca: '',
    certname: '',
    keyname: '',
    caname: '',
    servername: '',
    verifyservercert: true,
    alpnprotocol: ''
});

/** Ranges the new nodes accept (values outside are clamped here, with a note). */
const LIMITS = Object.freeze({
    connectionTimeout: { min: 100, max: 300000 },
    requestTimeout: { min: 1000, max: 600000 },
    retries: { min: 0, max: 50 },
    initialRetryTime: { min: 50, max: 60000 },
    maxRetryTime: { min: 100, max: 600000 },
    ackTimeout: { min: 100, max: 600000 },
    autoCommitInterval: { min: 0, max: 86400000 },
    autoCommitThreshold: { min: 0, max: 1000000000 },
    sessionTimeout: { min: 1000, max: 3600000 },
    heartbeatInterval: { min: 100, max: 3600000 },
    rebalanceTimeout: { min: 1000, max: 3600000 },
    maxBytesPerPartition: { min: 1, max: 2147483647 },
    maxBytes: { min: 1, max: 2147483647 },
    maxWaitTimeInMs: { min: 0, max: 300000 }
});

const MECHANISMS = new Set(['plain', 'scram-sha-256', 'scram-sha-512']);
const LOG_LEVELS = Object.freeze({ error: 'error', warn: 'warn', info: 'info', debug: 'debug', nothing: 'off', none: 'off', off: 'off' });
const ACKS = new Set(['all', 'leader', 'none']);
const TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/;
/** Node-RED replaces ${VAR} / $(VAR) only when it is the whole value. */
const ENV_REF = /^\s*(?:\$\{[^}]+\}|\$\([^)]+\))\s*$/;
/** Properties of the old consumer output that moved out of msg.payload. */
const OLD_OUTPUT_RE = /\bpayload\s*(?:\.\s*|\[\s*['"])(value|key|headers|offset|timestamp|attributes|magicByte|isControlRecord|batchContext)\b/g;
/** Node properties that never hold flow logic (skipped when looking for the old consumer output shape). */
const NOT_LOGIC = new Set(['id', 'type', 'z', 'g', 'x', 'y', 'wires', 'name', 'info', 'd', 'l', 'links', 'scope']);

/** Upstream fields that the new nodes do not have, and why that is fine. */
const DROPPED_INFO = {
    metadatamaxage: 'Metadata max age is not configurable (kafkajs default, 5 minutes)',
    minbytes: 'Min bytes is not configurable (kafkajs default, 1 byte)'
};

// ---------------------------------------------------------------------------
// small helpers (never throw on odd input)

const hasOwn = (obj, key) => obj !== null && typeof obj === 'object' && Object.prototype.hasOwnProperty.call(obj, key);

function isBlank(v) {
    return v === undefined || v === null || (typeof v === 'string' && v.trim() === '');
}

function isEnvRef(v) {
    return typeof v === 'string' && ENV_REF.test(v);
}

/** A string property: strings as they are, finite numbers as text, everything else ''. */
function str(v) {
    if (typeof v === 'string') return v;
    if (typeof v === 'number' && Number.isFinite(v)) return String(v);
    return '';
}

/** Checkbox values: booleans, or 'true'/'false' strings from hand-written flows. */
function bool(v, def) {
    if (v === true || v === false) return v;
    if (typeof v === 'string') {
        const s = v.trim().toLowerCase();
        if (s === 'true' || s === 'on' || s === '1') return true;
        if (s === 'false' || s === 'off' || s === '0' || s === '') return false;
    }
    if (v === 1) return true;
    if (v === 0) return false;
    return def;
}

/** A finite number from a number or a numeric string; undefined otherwise. */
function toNumber(v) {
    if (typeof v === 'number') return Number.isFinite(v) ? v : undefined;
    if (typeof v === 'string' && v.trim() !== '') {
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
    }
    return undefined;
}

/** A Node-RED style id (16 hex digits) that is not used yet. */
function defaultIdGenerator(used) {
    for (;;) {
        const id = crypto.randomBytes(8).toString('hex');
        if (!used.has(id)) return id;
    }
}

// ---------------------------------------------------------------------------
// conversion context: notes for the report

class Context {
    constructor(nodes, options) {
        this.notes = [];
        this.byId = new Map();
        for (const n of nodes) if (n && typeof n === 'object' && typeof n.id === 'string') this.byId.set(n.id, n);
        this.usedIds = new Set(this.byId.keys());
        const gen = options && typeof options.idGenerator === 'function' ? options.idGenerator : defaultIdGenerator;
        this.newId = () => {
            let id = gen(this.usedIds);
            while (typeof id !== 'string' || id === '' || this.usedIds.has(id)) id = defaultIdGenerator(this.usedIds);
            this.usedIds.add(id);
            return id;
        };
    }

    /**
     * @param {'action'|'check'|'info'} level action = must be done for the flow to work,
     *   check = verify after deploying, info = a behaviour difference worth knowing
     */
    note(level, type, node, text) {
        this.notes.push({ level, id: node && node.id, type, name: (node && typeof node.name === 'string') ? node.name : '', text });
    }

    /** Number property: env references kept, numbers coerced, clamped to the new range. */
    number(value, def, field, type, node, label) {
        if (isEnvRef(value)) return value.trim();
        const n = toNumber(value);
        if (n === undefined) {
            if (!isBlank(value)) this.note('info', type, node, `${label} "${str(value) || typeof value}" is not a number: using ${def}.`);
            return def;
        }
        let v = Math.trunc(n);
        const lim = LIMITS[field];
        if (lim) {
            const clamped = Math.min(lim.max, Math.max(lim.min, v));
            if (clamped !== v) {
                this.note('info', type, node, `${label} ${v} is outside ${lim.min} – ${lim.max}: using ${clamped}.`);
                v = clamped;
            }
        }
        return v;
    }
}

/** Fields every node keeps as they are (position, group, disabled flag, label visibility, description). */
function base(node, type) {
    const out = { id: node.id, type };
    if (hasOwn(node, 'z')) out.z = node.z;
    if (hasOwn(node, 'g')) out.g = node.g;
    return out;
}

function finish(out, node, wires) {
    for (const key of ['x', 'y']) if (hasOwn(node, key)) out[key] = node[key];
    if (wires !== undefined) out.wires = wires;
    for (const key of ['d', 'l', 'info']) if (hasOwn(node, key)) out[key] = node[key];
    return out;
}

/** Wires for a node with one output: kept when valid, else one empty output. */
function oneOutput(wires) {
    if (Array.isArray(wires) && wires.length > 0 && Array.isArray(wires[0])) {
        return [wires[0].filter((w) => typeof w === 'string')];
    }
    return [[]];
}

// ---------------------------------------------------------------------------
// kafkajs-client → kafkajs-nc-broker (+ tls-config)

function convertClient(node, ctx, creds) {
    const type = TYPE_MAP['kafkajs-client'];
    const out = Object.assign(base(node, type), { ...BROKER_DEFAULTS });
    out.name = str(node.name);

    // brokers: a comma-separated string (the editor default is an empty array)
    let list = [];
    if (Array.isArray(node.brokers)) list = node.brokers.map(str);
    else list = [str(node.brokers)];
    const brokers = list.join(',').split(/[\s,]+/).filter(Boolean);
    if (brokers.length === 1 && isEnvRef(brokers[0])) out.brokers = brokers[0];
    else out.brokers = brokers.join(', ');
    if (out.brokers === '') {
        ctx.note('action', type, node, 'No brokers were configured: enter the broker list (host:port, ...) before deploying.');
    }

    const clientId = str(node.clientid).trim();
    out.clientId = clientId === '' ? BROKER_DEFAULTS.clientId : clientId;

    out.connectionTimeout = ctx.number(node.connectiontimeout, 3000, 'connectionTimeout', type, node, 'Connection timeout');
    out.requestTimeout = ctx.number(node.requesttimeout, 25000, 'requestTimeout', type, node, 'Request timeout');
    if (bool(node.advancedretry, false)) {
        out.retries = ctx.number(node.retries, BROKER_DEFAULTS.retries, 'retries', type, node, 'Retries');
        out.initialRetryTime = ctx.number(node.initialretrytime, BROKER_DEFAULTS.initialRetryTime, 'initialRetryTime',
            type, node, 'Initial retry time');
        out.maxRetryTime = ctx.number(node.maxretrytime, BROKER_DEFAULTS.maxRetryTime, 'maxRetryTime', type, node, 'Max retry time');
        const factor = toNumber(node.factor);
        const multiplier = toNumber(node.multiplier);
        if ((factor !== undefined && factor !== 0.2) || (multiplier !== undefined && multiplier !== 2)) {
            ctx.note('info', type, node, `Retry factor ${str(node.factor)} / multiplier ${str(node.multiplier)} are not ` +
                'configurable any more (fixed 0.2 / 2). The node now reconnects forever by itself after kafkajs gives up.');
        }
    }

    const level = str(node.loglevel).trim().toLowerCase();
    out.logLevel = hasOwn(LOG_LEVELS, level) ? LOG_LEVELS[level] : BROKER_DEFAULTS.logLevel;

    let tlsNode = null;
    let passphrase = '';
    const auth = str(node.auth).trim().toLowerCase();
    if (auth === 'sasl') {
        let mechanism = str(node.saslmechanism).trim().toLowerCase();
        if (mechanism === '') mechanism = 'plain'; // the old runtime's fallback
        if (!MECHANISMS.has(mechanism)) {
            ctx.note('action', type, node, `Unknown SASL mechanism "${str(node.saslmechanism)}": choose PLAIN, ` +
                'SCRAM-SHA-256 or SCRAM-SHA-512 on the Security tab.');
            mechanism = 'plain';
        }
        out.authMechanism = mechanism;
        // the old runtime passed `saslssl` straight to kafkajs: missing means no TLS
        out.tls = bool(node.saslssl, false);
        const c = creds && hasOwn(creds, node.id) ? creds[node.id] : null;
        const haveUser = c && !isBlank(c.saslusername);
        const havePass = c && typeof c.saslpassword === 'string' && c.saslpassword !== '';
        if (!(haveUser && havePass)) {
            ctx.note('action', type, node, 'Enter the SASL username and password again (Security tab), or use ' +
                '${KAFKA_USER} / ${KAFKA_PASSWORD}. Until then the node shows "config error" and does not connect.');
        }
        if (mechanism === 'plain' && !out.tls) {
            ctx.note('info', type, node, 'SASL/PLAIN without TLS sends the password in clear text; the node logs a ' +
                'warning about it. Turn on "Use TLS" if the broker has a SASL_SSL listener.');
        }
    } else if (auth === 'tls') {
        out.tls = true;
        const cert = str(node.tlsclientcert).trim();
        const key = str(node.tlsprivatekey).trim();
        const ca = str(node.tlscacert).trim();
        passphrase = str(node.tlspassphrase);
        if (cert === '' && key === '' && ca === '') {
            ctx.note('action', type, node, 'TLS was selected without certificate files (the old node failed with ENOENT). ' +
                'The broker now uses TLS with the system CA certificates; add a TLS configuration node if you need a ' +
                'private CA or a client certificate.');
        } else {
            tlsNode = Object.assign(base(node, 'tls-config'), { ...TLS_CONFIG_DEFAULTS });
            tlsNode.id = ctx.newId();
            tlsNode.name = `${str(node.name).trim() || 'Kafka'} TLS`;
            tlsNode.cert = cert;
            tlsNode.key = key;
            tlsNode.ca = ca;
            // "Self Sign" meant "allow untrusted certificates" (old README) but the old
            // code did the opposite (rejectUnauthorized = tlsselfsign). Use the meaning.
            const selfSign = bool(node.tlsselfsign, false);
            tlsNode.verifyservercert = !selfSign;
            if (hasOwn(node, 'g')) delete tlsNode.g;
            out.tlsConfig = tlsNode.id;
            if ((cert === '') !== (key === '')) {
                ctx.note('action', 'tls-config', tlsNode, 'Only one of client certificate and private key is set: ' +
                    'set both or neither in the TLS configuration node, otherwise the broker shows "config error".');
            }
            ctx.note('check', type, node, selfSign
                ? 'TLS: "Self Sign" was ticked, so the new TLS configuration does NOT verify the broker certificate ' +
                  '(the old code verified it in this case). Prefer adding the CA certificate and ticking ' +
                  '"Verify server certificate".'
                : 'TLS: the new TLS configuration verifies the broker certificate against the CA file (the old code ' +
                  'skipped verification unless "Self Sign" was ticked). If the status shows "TLS: …", check the CA ' +
                  'file and the host name, or set "Server Name" in the TLS configuration.');
            ctx.note('check', 'tls-config', tlsNode, `Certificate files are read from ${[ca, cert, key].filter(Boolean).join(', ')} ` +
                'when Node-RED starts: they must exist on the machine that runs Node-RED.');
        }
        if (passphrase !== '' && !(tlsNode && creds)) {
            ctx.note('action', tlsNode ? 'tls-config' : type, tlsNode || node, 'Enter the private key passphrase again in ' +
                'the TLS configuration node (it is a credential now and no longer stored in the flow file).');
        }
    } else if (auth !== 'none' && auth !== '') {
        ctx.note('action', type, node, `Unknown authentication "${str(node.auth)}": check the Security tab.`);
    }
    return { broker: finish(out, node, undefined), tlsNode, passphrase };
}

// ---------------------------------------------------------------------------
// kafkajs-producer → kafkajs-nc-producer

function convertProducer(node, ctx) {
    const type = TYPE_MAP['kafkajs-producer'];
    const out = Object.assign(base(node, type), { ...PRODUCER_DEFAULTS, headers: {} });
    out.name = str(node.name);
    out.broker = brokerRef(node, ctx, type);

    out.topic = str(node.topic).trim();
    if (out.topic !== '' && !isEnvRef(out.topic) && (!TOPIC_RE.test(out.topic) || out.topic === '.' || out.topic === '..')) {
        ctx.note('action', type, node, `Topic "${out.topic}" is not a valid Kafka topic name (letters, digits, ".", "_" ` +
            'and "-", at most 249): every message would be rejected until it is fixed.');
    }
    out.key = node.key === null || node.key === undefined ? '' : str(node.key);

    // partition: the old node ignored a numeric 0; "0" from the editor was used
    if (!isBlank(node.partition)) {
        const p = node.partition;
        if (isEnvRef(p)) out.partition = p.trim();
        else if (/^\s*\d+\s*$/.test(str(p)) && Number(p) <= 2147483647) out.partition = String(Number(p));
        else {
            ctx.note('action', type, node, `Partition "${str(p) || typeof p}" is not a whole number >= 0 and was left ` +
                'empty (the partitioner, or msg.partition, decides).');
        }
    }

    // static headers {name: value}; the old node then ignored msg.headers completely
    const headers = {};
    if (node.headeritems && typeof node.headeritems === 'object' && !Array.isArray(node.headeritems)) {
        for (const [name, value] of Object.entries(node.headeritems)) {
            if (name.trim() === '') continue;
            headers[name.trim()] = value === null || value === undefined ? '' : (Array.isArray(value) ? value.map(str) : str(value));
        }
    }
    out.headers = headers;
    if (Object.keys(headers).length > 0) {
        out.useMsgHeaders = false;
        ctx.note('info', type, node, '"Also send msg.headers" is off, as before: the old node ignored msg.headers when ' +
            'static headers were set. Tick it to merge msg.headers with the static headers.');
    }

    const acks = str(node.acknowledge).trim().toLowerCase();
    if (ACKS.has(acks)) {
        out.acks = acks;
        if (acks === 'none') {
            ctx.note('info', type, node, 'Acknowledgement "none" now really means fire-and-forget (acks=0): no ' +
                'partition/offset in the delivery report, and no error if the broker loses the message. The old node ' +
                'waited for answers that never came and sent each message several times.');
        }
    } else {
        out.acks = 'all';
        if (!isBlank(node.acknowledge)) ctx.note('info', type, node, `Unknown acknowledge "${str(node.acknowledge)}": using "all".`);
    }
    out.ackTimeout = ctx.number(node.responsetimeout, PRODUCER_DEFAULTS.ackTimeout, 'ackTimeout', type, node, 'Response timeout');
    // 0.0.7 (what the palette manager installed) ran on kafkajs 1.x: keep its key → partition mapping
    out.partitioner = 'legacy';

    // the old node never passed allowautotopiccreation to kafkajs: topics were always auto-created
    out.allowAutoTopicCreation = true;
    if (bool(node.allowautotopiccreation, false) === false) {
        ctx.note('info', type, node, '"Let the broker create missing topics" is on: the old node ignored this setting ' +
            '(missing topics were always created when the broker allows it). Untick it to make sends to missing ' +
            'topics fail instead.');
    }
    return finish(out, node, [[]]);
}

// ---------------------------------------------------------------------------
// kafkajs-consumer → kafkajs-nc-consumer

function convertConsumer(node, ctx) {
    const type = TYPE_MAP['kafkajs-consumer'];
    const out = Object.assign(base(node, type), { ...CONSUMER_DEFAULTS });
    out.name = str(node.name);
    out.broker = brokerRef(node, ctx, type);

    out.topics = str(node.topic).trim();
    if (out.topics === '') {
        ctx.note('action', type, node, 'No topic was configured: set Topics before deploying.');
    } else if (!isEnvRef(out.topics) && out.topics.split(/[\s,]+/).filter(Boolean)
        .some((t) => !isEnvRef(t) && (!TOPIC_RE.test(t) || t === '.' || t === '..'))) {
        ctx.note('action', type, node, `Topic "${out.topics}" is not a valid Kafka topic name: the node shows ` +
            '"config error" until it is fixed.');
    }

    out.groupId = str(node.groupid).trim();
    if (out.groupId === '') {
        ctx.note('info', type, node, `Group ID was empty, so the old node joined a new random group on every deploy. ` +
            `The new node uses the group "node-red-${node.id}", which stays the same across restarts. Offsets of the ` +
            'old random groups do not carry over: the first start reads only new messages unless "Read from the ' +
            'beginning" is ticked.');
    }

    // the old node only used these settings while "Advanced options" was ticked
    if (bool(node.advancedoptions, false)) {
        out.fromBeginning = bool(node.frombeginning, false);
        out.autoCommitInterval = ctx.number(node.autocommitinterval, 5000, 'autoCommitInterval', type, node, 'Auto commit interval');
        out.autoCommitThreshold = ctx.number(node.autocommitthreshold, 0, 'autoCommitThreshold', type, node, 'Auto commit threshold');
        out.sessionTimeout = ctx.number(node.sessiontimeout, 30000, 'sessionTimeout', type, node, 'Session timeout');
        out.rebalanceTimeout = ctx.number(node.rebalancetimeout, 60000, 'rebalanceTimeout', type, node, 'Rebalance timeout');
        out.heartbeatInterval = ctx.number(node.heartbeatinterval, 3000, 'heartbeatInterval', type, node, 'Heartbeat interval');
        out.maxBytesPerPartition = ctx.number(node.maxbytesperpartition, 1048576, 'maxBytesPerPartition', type, node,
            'Max bytes per partition');
        out.maxBytes = ctx.number(node.maxbytes, 10485760, 'maxBytes', type, node, 'Max bytes');
        out.maxWaitTimeInMs = ctx.number(node.maxwaittimeinms, 5000, 'maxWaitTimeInMs', type, node, 'Max wait time');
        out.allowAutoTopicCreation = bool(node.allowautotopiccreation, false);
        if (typeof out.heartbeatInterval === 'number' && typeof out.sessionTimeout === 'number' &&
            out.heartbeatInterval >= out.sessionTimeout) {
            ctx.note('info', type, node, `Heartbeat interval ${out.heartbeatInterval} ms is not lower than the session ` +
                `timeout ${out.sessionTimeout} ms: the node will use a third of the session timeout (with a warning).`);
        }
        for (const [field, why] of Object.entries(DROPPED_INFO)) {
            if (!isBlank(node[field]) && toNumber(node[field]) !== (field === 'minbytes' ? 1 : 300000)) {
                ctx.note('info', type, node, `${why}: the old value ${str(node[field])} is dropped.`);
            }
        }
        if (bool(node.clearoffsets, false)) {
            ctx.note('action', type, node, '"Clear offsets" (which reset the group to the LATEST offset on every deploy, ' +
                'skipping unread messages) does not exist any more. Use a kafka admin node with action resetOffsets, or ' +
                'msg.action "seek", when you really want to move the group.');
        }
    }
    return finish(out, node, oneOutput(node.wires));
}

/** client → broker reference, with notes when it cannot work. */
function brokerRef(node, ctx, type) {
    const ref = str(node.client).trim();
    if (ref === '') {
        ctx.note('action', type, node, 'No kafkajs-client was selected: choose a broker configuration before deploying.');
        return '';
    }
    const target = ctx.byId.get(ref);
    if (!target) {
        ctx.note('action', type, node, `The selected client ${ref} is not in this file: make sure a kafkajs-nc-broker ` +
            'node with this id exists (convert the file that contains it), or choose another broker.');
    } else if (target.type !== 'kafkajs-client' && target.type !== 'kafkajs-nc-broker') {
        ctx.note('action', type, node, `The selected client ${ref} is a "${target.type}" node, not a kafkajs-client.`);
    }
    return ref;
}

// ---------------------------------------------------------------------------
// nodes after a consumer that still read the old output shape

/** Ids of the nodes reachable from `startIds` through wires, link nodes and subflow inputs. */
function downstream(startIds, byId) {
    const seen = new Set();
    const queue = [];
    const visit = (id) => {
        if (typeof id === 'string' && !seen.has(id) && byId.has(id)) {
            seen.add(id);
            queue.push(id);
        }
    };
    startIds.forEach(visit);
    while (queue.length > 0) {
        const n = byId.get(queue.shift());
        if (!n) continue;
        if (Array.isArray(n.wires)) for (const port of n.wires) if (Array.isArray(port)) port.forEach(visit);
        if (n.type === 'link out' && Array.isArray(n.links)) n.links.forEach(visit);
        if (typeof n.type === 'string' && n.type.startsWith('subflow:')) {
            const def = byId.get(n.type.slice('subflow:'.length));
            if (def && Array.isArray(def.in)) {
                for (const port of def.in) {
                    if (port && Array.isArray(port.wires)) for (const w of port.wires) visit(w && w.id);
                }
            }
        }
    }
    return seen;
}

/** Old-shape property names (value, key, ...) used anywhere in the node's settings. */
function oldShapeUses(node) {
    const found = new Set();
    const scan = (v, depth) => {
        if (depth > 6) return;
        if (typeof v === 'string') {
            for (const m of v.matchAll(OLD_OUTPUT_RE)) found.add(`msg.payload.${m[1]}`);
        } else if (Array.isArray(v)) {
            for (const x of v) scan(x, depth + 1);
        } else if (v !== null && typeof v === 'object') {
            for (const x of Object.values(v)) scan(x, depth + 1);
        }
    };
    for (const [key, value] of Object.entries(node)) if (!NOT_LOGIC.has(key)) scan(value, 0);
    return Array.from(found).sort();
}

function noteConsumerOutput(consumer, ctx) {
    const type = TYPE_MAP['kafkajs-consumer'];
    const start = [];
    for (const port of consumer.wires || []) if (Array.isArray(port)) start.push(...port);
    const uses = [];
    for (const id of downstream(start, ctx.byId)) {
        const n = ctx.byId.get(id);
        if (hasOwn(TYPE_MAP, n.type)) continue; // the Kafka nodes themselves are converted
        const found = oldShapeUses(n);
        if (found.length > 0) uses.push(`${n.type}${n.name ? ` "${n.name}"` : ''} (${n.id}) uses ${found.join(', ')}`);
    }
    const text = 'The output changed: msg.payload is now the record value (the old node put the whole kafkajs record ' +
        'there). Use msg.key, msg.headers, msg.offset, msg.timestamp and msg.kafka instead of msg.payload.key, ' +
        'msg.payload.headers, ... in the nodes after this consumer.';
    if (uses.length > 0) ctx.note('action', type, consumer, `${text} Found: ${uses.join('; ')}.`);
    else ctx.note('check', type, consumer, text);
}

// ---------------------------------------------------------------------------
// credentials

/**
 * Convert the credentials of migrated nodes. `creds` is the parsed
 * flows_cred.json. Encrypted files ({"$": "..."}) cannot be read here.
 *
 * @param {object|null|undefined} creds
 * @param {Array<{brokerId: string, tlsId: (string|null), passphrase: string}>} plan
 * @returns {{status: 'none'|'encrypted'|'invalid'|'converted', credentials: (object|null)}}
 */
function convertCredentials(creds, plan) {
    if (creds === undefined || creds === null) return { status: 'none', credentials: null };
    if (typeof creds !== 'object' || Array.isArray(creds)) return { status: 'invalid', credentials: null };
    if (hasOwn(creds, '$')) return { status: 'encrypted', credentials: null };
    const out = { ...creds };
    for (const { brokerId, tlsId, passphrase } of plan) {
        const old = hasOwn(creds, brokerId) ? creds[brokerId] : null;
        if (old && typeof old === 'object') {
            const next = {};
            if (old.saslusername !== undefined && old.saslusername !== null) next.username = String(old.saslusername);
            if (old.saslpassword !== undefined && old.saslpassword !== null) next.password = String(old.saslpassword);
            if (Object.keys(next).length > 0) out[brokerId] = next;
            else delete out[brokerId];
        }
        if (tlsId && passphrase !== '') out[tlsId] = { passphrase };
    }
    return { status: 'converted', credentials: out };
}

// ---------------------------------------------------------------------------
// the whole file

/**
 * Convert a parsed flows file (an array of nodes, or the admin API's
 * { rev, flows } object). Pure: the input is not modified.
 *
 * @param {Array|object} input
 * @param {object} [options]
 * @param {object|null} [options.credentials] parsed flows_cred.json (unencrypted), if available
 * @param {(used: Set<string>) => string} [options.idGenerator] ids for new tls-config nodes (tests)
 * @returns {{flows: (Array|object), credentials: (object|null), credentialsStatus: string,
 *   report: {migrated: number, counts: object, notes: Array<object>, credentialsStatus: string}}}
 * @throws {TypeError} when the input is not a flows file
 */
function convertFlows(input, options) {
    const o = options && typeof options === 'object' ? options : {};
    let nodes;
    let wrap = (list) => list;
    if (Array.isArray(input)) {
        nodes = input;
    } else if (input && typeof input === 'object' && Array.isArray(input.flows)) {
        nodes = input.flows;
        wrap = (list) => ({ ...input, flows: list });
    } else {
        throw new TypeError('Not a Node-RED flows file: expected an array of nodes (or an object with a "flows" array)');
    }
    const ctx = new Context(nodes, o);
    const credsIn = o.credentials;
    const readable = credsIn && typeof credsIn === 'object' && !Array.isArray(credsIn) && !hasOwn(credsIn, '$') ? credsIn : null;
    const counts = { brokers: 0, producers: 0, consumers: 0, tlsConfigs: 0 };
    const plan = [];
    const consumers = [];
    const out = [];
    for (const node of nodes) {
        const kind = node && typeof node === 'object' ? node.type : undefined;
        if (kind === 'kafkajs-client') {
            const r = convertClient(node, ctx, readable);
            out.push(r.broker);
            counts.brokers++;
            if (r.tlsNode) {
                out.push(r.tlsNode);
                counts.tlsConfigs++;
            }
            plan.push({ brokerId: node.id, tlsId: r.tlsNode ? r.tlsNode.id : null, passphrase: r.passphrase });
        } else if (kind === 'kafkajs-producer') {
            out.push(convertProducer(node, ctx));
            counts.producers++;
        } else if (kind === 'kafkajs-consumer') {
            const c = convertConsumer(node, ctx);
            out.push(c);
            consumers.push(c);
            counts.consumers++;
        } else {
            out.push(node);
        }
    }
    for (const c of consumers) noteConsumerOutput(c, ctx);
    const migrated = counts.brokers + counts.producers + counts.consumers;

    let creds = { status: 'none', credentials: null };
    if (plan.length > 0) creds = convertCredentials(credsIn, plan);
    else if (credsIn !== undefined && credsIn !== null) creds = { status: 'unchanged', credentials: null };
    if (creds.status === 'encrypted' && plan.length > 0) {
        ctx.note('action', TYPE_MAP['kafkajs-client'], null, 'The credentials file is encrypted (credentialSecret), so ' +
            'this script cannot convert it: enter the SASL usernames/passwords and TLS passphrases again in the editor.');
    }
    const report = { migrated, counts, notes: ctx.notes, credentialsStatus: creds.status };
    return {
        flows: wrap(out),
        credentials: creds.credentials,
        credentialsStatus: creds.status,
        report
    };
}

// ---------------------------------------------------------------------------
// report

const SECTIONS = [
    ['action', 'Needs your attention'],
    ['check', 'Check after deploying'],
    ['info', 'Good to know']
];

/**
 * Human-readable report.
 *
 * @param {object} report convertFlows().report
 * @param {{output?: string, credentialsOutput?: string, backup?: string, credentialsBackup?: string}} [files]
 * @returns {string}
 */
function formatReport(report, files) {
    const f = files || {};
    const lines = [];
    const c = report.counts;
    if (report.migrated === 0) {
        return 'Nothing to migrate: the file contains no kafkajs-client, kafkajs-producer or kafkajs-consumer nodes.\n';
    }
    const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
    lines.push(`Converted ${plural(c.brokers, 'kafkajs-client')} → kafkajs-nc-broker, ` +
        `${plural(c.producers, 'kafkajs-producer')} → kafkajs-nc-producer, ` +
        `${plural(c.consumers, 'kafkajs-consumer')} → kafkajs-nc-consumer` +
        (c.tlsConfigs > 0 ? `, and created ${plural(c.tlsConfigs, 'tls-config node')}.` : '.'));
    if (f.backup) lines.push(`Backup of the original flows:  ${f.backup}`);
    if (f.output) lines.push(`Converted flows written to:     ${f.output}`);
    if (f.credentialsBackup) lines.push(`Backup of the credentials:     ${f.credentialsBackup}`);
    if (f.credentialsOutput) lines.push(`Converted credentials written: ${f.credentialsOutput} (plain text: protect it like the original)`);
    if (report.credentialsStatus === 'encrypted') {
        lines.push('Credentials: the file is encrypted and was not changed; enter the credentials again in the editor.');
    } else if (report.credentialsStatus === 'none' && c.brokers > 0) {
        lines.push('Credentials: not supplied (--credentials); enter SASL passwords and TLS passphrases again in the editor.');
    } else if (report.credentialsStatus === 'unchanged') {
        lines.push('Credentials: nothing to convert (no kafkajs-client node in this file); the file was not changed.');
    }
    if (c.producers > 0) {
        lines.push('Producers use Partitioner "Legacy" (the key → partition mapping of node-red-contrib-kafkajs 0.0.7, ' +
            'which ran on kafkajs 1.x) and the offline queue on disk.');
    }
    for (const [level, title] of SECTIONS) {
        const notes = report.notes.filter((n) => n.level === level);
        if (notes.length === 0) continue;
        lines.push('', `${title} (${notes.length}):`);
        for (const n of notes) {
            const who = n.id ? `${n.type}${n.name ? ` "${n.name}"` : ''} (${n.id})` : 'all brokers';
            lines.push(`  - ${who}: ${n.text}`);
        }
    }
    lines.push('', 'Next: install node-red-contrib-kafkajs-no-crash, load the converted flows, fix the items above, deploy.',
        'Details: docs/MIGRATION.md');
    return lines.join('\n') + '\n';
}

// ---------------------------------------------------------------------------
// command line

const USAGE = `Usage: kafkajs-nc-migrate-flows <flows.json> [--out <file>] [--credentials <flows_cred.json>] [--in-place]
   or: node migrate-flows.js <flows.json> [options]   (from a git checkout or a copy of the script)

Without installing anything: npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows <flows.json>

Converts node-red-contrib-kafkajs nodes (kafkajs-client, kafkajs-producer,
kafkajs-consumer) to node-red-contrib-kafkajs-no-crash nodes.

  --out <file>          where to write the converted flows
                        (default: <flows>.migrated.json next to the input)
  --credentials <file>  an UNENCRYPTED flows_cred.json: SASL usernames/passwords and
                        TLS passphrases are converted too and written to
                        <out>_cred.json (Node-RED's naming rule)
  --in-place            replace the input file(s) instead, after writing a .bak copy
                        (stop Node-RED first)
  -h, --help            show this help
`;

class UsageError extends Error {}

/**
 * @param {string[]} argv arguments after the script name
 * @returns {{help: boolean, input?: string, out?: string, credentials?: string, inPlace: boolean}}
 * @throws {UsageError}
 */
function parseArgs(argv) {
    const args = { help: false, inPlace: false };
    const list = Array.isArray(argv) ? argv.slice() : [];
    while (list.length > 0) {
        const a = String(list.shift());
        const value = (name) => {
            const v = list.shift();
            if (v === undefined || String(v).startsWith('--')) throw new UsageError(`${name} needs a file name`);
            return String(v);
        };
        if (a === '-h' || a === '--help') args.help = true;
        else if (a === '--in-place') args.inPlace = true;
        else if (a === '--out' || a === '-o') args.out = value(a);
        else if (a.startsWith('--out=')) args.out = a.slice('--out='.length);
        else if (a === '--credentials' || a === '-c') args.credentials = value(a);
        else if (a.startsWith('--credentials=')) args.credentials = a.slice('--credentials='.length);
        else if (a.startsWith('-')) throw new UsageError(`Unknown option ${a}`);
        else if (args.input === undefined) args.input = a;
        else throw new UsageError(`Unexpected argument ${a} (only one flows file at a time)`);
    }
    if (args.help) return args;
    if (!args.input) throw new UsageError('Missing the flows file');
    if (args.inPlace && args.out !== undefined) throw new UsageError('Use either --out or --in-place, not both');
    if (args.out === '' || args.credentials === '') throw new UsageError('Empty file name');
    return args;
}

/** Node-RED's credentials file name for a flows file: flows.json → flows_cred.json. */
function credentialsFileFor(flowsFile) {
    const ext = path.extname(flowsFile);
    return path.join(path.dirname(flowsFile), `${path.basename(flowsFile, ext)}_cred${ext}`);
}

/** <dir>/<name>.migrated<ext> */
function defaultOutput(input) {
    const ext = path.extname(input) || '.json';
    return path.join(path.dirname(input), `${path.basename(input, path.extname(input))}.migrated${ext}`);
}

/** A free backup name: file.bak, else file.bak.1, file.bak.2, ... */
function backupName(file) {
    if (!fs.existsSync(`${file}.bak`)) return `${file}.bak`;
    for (let i = 1; ; i++) if (!fs.existsSync(`${file}.bak.${i}`)) return `${file}.bak.${i}`;
}

function readJson(file, what) {
    let text;
    try {
        text = fs.readFileSync(file, 'utf8');
    } catch (err) {
        throw new Error(`Cannot read the ${what} ${file}: ${err.message}`);
    }
    if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
    try {
        return { data: JSON.parse(text), pretty: text.trim().includes('\n') };
    } catch (err) {
        throw new Error(`The ${what} ${file} is not valid JSON: ${err.message}`);
    }
}

/**
 * Write via a temporary file + rename, so a crash never leaves half a file
 * behind. `mode`: file permissions (credentials: 0o600; an in-place flows
 * file keeps the permissions of the original).
 */
function writeJson(file, data, pretty, mode) {
    const tmp = `${file}.tmp-${process.pid}`;
    const options = mode === undefined ? {} : { mode };
    try {
        fs.writeFileSync(tmp, JSON.stringify(data, null, pretty ? 4 : undefined), options);
        if (mode !== undefined) fs.chmodSync(tmp, mode);
        fs.renameSync(tmp, file);
    } catch (err) {
        try {
            fs.unlinkSync(tmp);
        } catch (_) {
            // nothing to clean up
        }
        throw err;
    }
}

/** Permission bits of an existing file (undefined when it cannot be read). */
function modeOf(file) {
    try {
        return fs.statSync(file).mode & 0o777;
    } catch (_) {
        return undefined;
    }
}

const samePath = (a, b) => path.resolve(a) === path.resolve(b);

/**
 * Run the command line. Never throws; returns the exit code
 * (0 = done or nothing to do, 1 = a file problem, 2 = wrong usage).
 *
 * @param {string[]} argv
 * @param {{stdout: {write: Function}, stderr: {write: Function}}} [io]
 * @returns {number}
 */
function main(argv, io) {
    const out = (io && io.stdout) || process.stdout;
    const err = (io && io.stderr) || process.stderr;
    let args;
    try {
        args = parseArgs(argv);
    } catch (e) {
        err.write(`${e.message}\n\n${USAGE}`);
        return 2;
    }
    if (args.help) {
        out.write(USAGE);
        return 0;
    }
    try {
        const flows = readJson(args.input, 'flows file');
        const creds = args.credentials ? readJson(args.credentials, 'credentials file') : null;
        const result = convertFlows(flows.data, { credentials: creds ? creds.data : null });
        if (result.report.migrated === 0) {
            out.write(formatReport(result.report));
            return 0;
        }
        if (result.credentialsStatus === 'invalid') {
            throw new Error(`The credentials file ${args.credentials} does not look like a Node-RED flows_cred.json`);
        }
        const files = {};
        let output;
        let credOutput = null;
        if (args.inPlace) {
            output = args.input;
            files.backup = backupName(args.input);
            fs.copyFileSync(args.input, files.backup);
            if (result.credentials) {
                credOutput = args.credentials;
                files.credentialsBackup = backupName(args.credentials);
                fs.copyFileSync(args.credentials, files.credentialsBackup);
            }
        } else {
            output = args.out !== undefined ? args.out : defaultOutput(args.input);
            if (samePath(output, args.input)) {
                throw new Error('The output would overwrite the input file: use --in-place (it keeps a .bak copy) or another --out');
            }
            if (result.credentials) {
                credOutput = credentialsFileFor(output);
                if (samePath(credOutput, args.credentials)) {
                    throw new Error(`The converted credentials would overwrite ${args.credentials}: choose another --out`);
                }
            }
        }
        writeJson(output, result.flows, flows.pretty, args.inPlace ? modeOf(args.input) : undefined);
        files.output = output;
        if (credOutput) {
            writeJson(credOutput, result.credentials, creds.pretty, 0o600);
            files.credentialsOutput = credOutput;
        }
        out.write(formatReport(result.report, files));
        return 0;
    } catch (e) {
        err.write(`migrate-flows: ${e && e.message ? e.message : e}\n`);
        return 1;
    }
}

module.exports = {
    TYPE_MAP,
    BROKER_DEFAULTS,
    PRODUCER_DEFAULTS,
    CONSUMER_DEFAULTS,
    TLS_CONFIG_DEFAULTS,
    LIMITS,
    convertFlows,
    convertCredentials,
    formatReport,
    parseArgs,
    credentialsFileFor,
    defaultOutput,
    main,
    UsageError,
    _internals: { convertClient, convertProducer, convertConsumer, downstream, oldShapeUses, Context }
};

if (require.main === module) {
    process.exitCode = main(process.argv.slice(2));
}
