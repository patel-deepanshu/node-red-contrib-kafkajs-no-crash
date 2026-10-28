'use strict';

/**
 * Error classes, classification and formatting.
 *
 * The classifier decides what the nodes do with a failure (retry fast, retry
 * slowly, reject the message, ...). It is built from the error shapes kafkajs
 * 2.2.4 really produces (5-kafkajs-internals.md §1). Key facts:
 *  - kafkajs wraps errors (KafkaJSNumberOfRetriesExceeded / KafkaJSNonRetriableError),
 *    so `type` (protocol error) and `code` (Node socket/TLS error) usually live on
 *    `.cause`, never on the top-level error. We therefore look at the whole chain.
 *  - TLS certificate failures arrive as retriable KafkaJSConnectionErrors; only the
 *    Node `code` tells them apart, so codes are checked before `retriable`.
 *
 * Every exported function is total: it never throws, whatever it is given.
 */

const { truncate } = require('./util');

/** The broker configuration is invalid (details lists every problem). */
class ConfigError extends Error {
    /**
     * @param {string} message
     * @param {string[]} [details]
     */
    constructor(message, details = []) {
        super(message);
        this.name = 'KafkaNcConfigError';
        this.details = Array.isArray(details) ? details.map(String) : [];
    }
}

/** The message content is invalid; such messages are rejected, never queued. */
class MessageError extends Error {
    /** @param {string} message */
    constructor(message) {
        super(message);
        this.name = 'KafkaNcMessageError';
    }
}

/** The offline queue is full and its overflow policy rejects new messages. */
class QueueFullError extends Error {
    /** @param {string} message */
    constructor(message) {
        super(message);
        this.name = 'KafkaNcQueueFullError';
    }
}

/** What a node should do about an error. */
const ErrorKind = Object.freeze({
    /** network/broker trouble: retry with backoff, queue messages */
    TRANSIENT: 'transient',
    /** authentication/authorisation/TLS trust: retry slowly, needs user action */
    AUTH: 'auth',
    /** invalid configuration or environment: retry slowly, needs user action */
    CONFIG: 'config',
    /** this message can never be delivered: reject it (never retry) */
    MESSAGE: 'message',
    /** topic missing or not authorised: MESSAGE if the topic came from msg, else AUTH */
    TOPIC: 'topic',
    /** not recognised: retry a limited number of times */
    UNKNOWN: 'unknown'
});

// ---------------------------------------------------------------------------
// Classification tables, copied from the research classifier (5-kafkajs-internals.md §1.5)

const NET_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ECONNABORTED', 'ENOTFOUND', 'EAI_AGAIN', 'EAI_FAIL',
    'EAI_NODATA', 'EAI_NONAME', 'ETIMEDOUT', 'ESOCKETTIMEDOUT', 'EHOSTUNREACH', 'EHOSTDOWN', 'ENETUNREACH',
    'ENETDOWN', 'ENETRESET', 'EPIPE', 'EADDRNOTAVAIL', 'ENOBUFS', 'ERR_SOCKET_CLOSED', 'ERR_STREAM_DESTROYED',
    'ERR_STREAM_WRITE_AFTER_END', 'ERR_SOCKET_CONNECTION_TIMEOUT']);

const TLS_CODES = new Set(['DEPTH_ZERO_SELF_SIGNED_CERT', 'SELF_SIGNED_CERT_IN_CHAIN',
    'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'UNABLE_TO_GET_ISSUER_CERT', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
    'UNABLE_TO_DECRYPT_CERT_SIGNATURE', 'UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY', 'CERT_SIGNATURE_FAILURE',
    'CERT_NOT_YET_VALID', 'CERT_HAS_EXPIRED', 'CERT_UNTRUSTED', 'CERT_REJECTED', 'CERT_REVOKED', 'INVALID_CA',
    'PATH_LENGTH_EXCEEDED', 'INVALID_PURPOSE', 'HOSTNAME_MISMATCH', 'ERR_TLS_CERT_ALTNAME_INVALID',
    'ERR_TLS_CERT_ALTNAME_FORMAT', 'EPROTO']);

const isTlsCode = (c) => !!c && (TLS_CODES.has(c) || /^ERR_(SSL|OSSL|TLS)_/.test(c));

// The research table merges "auth" and "config" problems. Both get the same slow
// retry, but the status text differs, so we split them: AUTH_TYPES are about
// credentials/permissions, the remaining AUTH_CONFIG_TYPES are configuration.
const AUTH_TYPES = new Set(['SASL_AUTHENTICATION_FAILED', 'UNSUPPORTED_SASL_MECHANISM', 'ILLEGAL_SASL_STATE',
    'CLUSTER_AUTHORIZATION_FAILED', 'GROUP_AUTHORIZATION_FAILED', 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
    'DELEGATION_TOKEN_AUTH_DISABLED', 'DELEGATION_TOKEN_NOT_FOUND', 'DELEGATION_TOKEN_OWNER_MISMATCH',
    'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED', 'DELEGATION_TOKEN_AUTHORIZATION_FAILED', 'DELEGATION_TOKEN_EXPIRED',
    'SECURITY_DISABLED']);

const AUTH_CONFIG_TYPES = new Set([...AUTH_TYPES, 'UNSUPPORTED_VERSION', 'UNSUPPORTED_COMPRESSION_TYPE',
    'INVALID_GROUP_ID', 'INVALID_SESSION_TIMEOUT', 'INCONSISTENT_GROUP_PROTOCOL', 'GROUP_MAX_SIZE_REACHED',
    'FENCED_INSTANCE_ID', 'INVALID_TRANSACTION_TIMEOUT', 'TRANSACTION_COORDINATOR_FENCED', 'INVALID_PRODUCER_EPOCH',
    'INVALID_PRODUCER_ID_MAPPING', 'INVALID_TXN_STATE', 'UNKNOWN_PRODUCER_ID', 'OUT_OF_ORDER_SEQUENCE_NUMBER']);

const TOPIC_TYPES = new Set(['TOPIC_AUTHORIZATION_FAILED', 'UNKNOWN_TOPIC_OR_PARTITION', 'TOPIC_DELETION_DISABLED']);

const PERMANENT_TYPES = new Set(['MESSAGE_TOO_LARGE', 'RECORD_LIST_TOO_LARGE', 'INVALID_TOPIC_EXCEPTION',
    'CORRUPT_MESSAGE', 'INVALID_RECORD', 'INVALID_TIMESTAMP', 'INVALID_REQUIRED_ACKS', 'UNSUPPORTED_FOR_MESSAGE_FORMAT',
    'OFFSET_METADATA_TOO_LARGE', 'POLICY_VIOLATION', 'INVALID_REQUEST', 'DUPLICATE_SEQUENCE_NUMBER',
    'INVALID_PARTITIONS', 'INVALID_REPLICATION_FACTOR', 'INVALID_REPLICA_ASSIGNMENT', 'INVALID_CONFIG',
    'TOPIC_ALREADY_EXISTS', 'INVALID_COMMIT_OFFSET_SIZE', 'INVALID_FETCH_SIZE', 'NON_EMPTY_GROUP',
    'GROUP_ID_NOT_FOUND', 'GROUP_SUBSCRIBED_TO_TOPIC', 'INVALID_PRINCIPAL_TYPE', 'LOG_DIR_NOT_FOUND',
    'REASSIGNMENT_IN_PROGRESS', 'NO_REASSIGNMENT_IN_PROGRESS', 'OFFSET_OUT_OF_RANGE']);

// Includes types kafkajs marks non-retriable but that are transient in practice
// (BROKER_NOT_AVAILABLE, REBALANCE_IN_PROGRESS, UNKNOWN, ...).
const TRANSIENT_TYPES = new Set(['LEADER_NOT_AVAILABLE', 'NOT_LEADER_FOR_PARTITION', 'REQUEST_TIMED_OUT',
    'BROKER_NOT_AVAILABLE', 'REPLICA_NOT_AVAILABLE', 'NETWORK_EXCEPTION', 'GROUP_LOAD_IN_PROGRESS',
    'GROUP_COORDINATOR_NOT_AVAILABLE', 'NOT_COORDINATOR_FOR_GROUP', 'NOT_ENOUGH_REPLICAS',
    'NOT_ENOUGH_REPLICAS_AFTER_APPEND', 'NOT_CONTROLLER', 'KAFKA_STORAGE_ERROR', 'FETCH_SESSION_ID_NOT_FOUND',
    'INVALID_FETCH_SESSION_EPOCH', 'LISTENER_NOT_FOUND', 'FENCED_LEADER_EPOCH', 'UNKNOWN_LEADER_EPOCH',
    'OFFSET_NOT_AVAILABLE', 'PREFERRED_LEADER_NOT_AVAILABLE', 'ELIGIBLE_LEADERS_NOT_AVAILABLE',
    'ELECTION_NOT_NEEDED', 'CONCURRENT_TRANSACTIONS', 'UNSTABLE_OFFSET_COMMIT', 'REBALANCE_IN_PROGRESS',
    'ILLEGAL_GENERATION', 'UNKNOWN_MEMBER_ID', 'MEMBER_ID_REQUIRED', 'STALE_CONTROLLER_EPOCH', 'STALE_BROKER_EPOCH',
    'OPERATION_NOT_ATTEMPTED', 'UNKNOWN']);

const CONNECTIVITY_NAMES = new Set(['KafkaJSConnectionError', 'KafkaJSConnectionClosedError',
    'KafkaJSRequestTimeoutError', 'KafkaJSNoBrokerAvailableError', 'KafkaJSBrokerNotFound',
    'KafkaJSMetadataNotLoaded', 'KafkaJSTopicMetadataNotLoaded', 'KafkaJSStaleTopicMetadataAssignment',
    'KafkaJSLockTimeout', 'KafkaJSTimeout', 'KafkaJSOffsetOutOfRange']);

// Message patterns of client-side configuration problems. The research list is
// split in two: credentials problems (AUTH) and everything else (CONFIG).
const AUTH_MSG = [/mechanism is not supported by the server/i, /Invalid username or password|authentication failed/i];
const CONFIG_MSG = [/brokers (array is empty|should not be null)|broker at index \d+ is invalid/i,
    /Port should be >= 0 and < 65536/i,
    /PEM routines|no start line|bad decrypt|wrong tag|bad base64|unsupported (certificate|key)|mac verify failure/i,
    /compression not implemented/i, /'timeout' is not a number/i, /reading 'toUpperCase'/i,
    /authenticationProvider is not a function/i, /reading 'compress'/i];
const MESSAGE_MSG = [/^Invalid topic/i, /Invalid message without value/i, /Invalid messages array/i,
    /must be of type string or an instance of Buffer/i, /Cannot convert .* to a BigInt/i];

const JS_ERROR_NAMES = new Set(['TypeError', 'RangeError', 'SyntaxError', 'ReferenceError']);

const MAX_CHAIN = 20;

// ---------------------------------------------------------------------------
// helpers

/** Read a property without ever throwing (getters/proxies can throw). */
function get(obj, key) {
    try {
        return obj == null ? undefined : obj[key];
    } catch (_) {
        return undefined;
    }
}

/** String() that never throws. */
function str(v) {
    try {
        return String(v);
    } catch (_) {
        return '[unprintable]';
    }
}

/** The message of a chain element as a string ('' when missing). */
function msgOf(e) {
    const m = get(e, 'message');
    if (m == null) return '';
    return typeof m === 'string' ? m : str(m);
}

/**
 * Walk an error and everything it wraps: `.cause`, `.originalError`,
 * `.errors[]` (AggregateError, KafkaJSAggregateError) and `.partitions[].error`
 * (KafkaJSDeleteTopicRecordsError). Cycle-safe, at most 20 entries.
 * Non-object values become `{ name: 'NonError', message: String(v) }`.
 *
 * @param {*} err
 * @returns {object[]} the error itself first, then what it wraps
 */
function flatten(err) {
    const out = [];
    const seen = new Set();
    const walk = (e) => {
        if (out.length >= MAX_CHAIN) return;
        if (e === null || (typeof e !== 'object' && typeof e !== 'function')) {
            out.push({ name: 'NonError', message: str(e) });
            return;
        }
        if (typeof e === 'function') {
            out.push({ name: 'NonError', message: '[function]' });
            return;
        }
        if (seen.has(e)) return;
        seen.add(e);
        out.push(e);
        const cause = get(e, 'cause');
        if (cause != null && cause !== e) walk(cause);
        const original = get(e, 'originalError');
        if (original != null) walk(original);
        const errors = get(e, 'errors');
        if (Array.isArray(errors)) {
            for (const inner of errors) {
                if (out.length >= MAX_CHAIN) break;
                if (inner != null) walk(inner);
            }
        }
        const partitions = get(e, 'partitions');
        if (Array.isArray(partitions)) {
            for (const p of partitions) {
                if (out.length >= MAX_CHAIN) break;
                const pe = get(p, 'error');
                if (pe != null) walk(pe);
            }
        }
    };
    try {
        walk(err);
    } catch (_) {
        // walking is best effort; whatever was collected so far is returned
    }
    if (out.length === 0) out.push({ name: 'NonError', message: str(err) });
    return out;
}

/**
 * Classify an error (see ErrorKind). Never throws.
 *
 * @param {*} err
 * @param {{op?: 'connect'|'send'|'subscribe'|'run'|'admin'|string}} [ctx]
 *   `op: 'send'` makes JS TypeErrors/RangeErrors count as MESSAGE (bad message
 *   content) instead of CONFIG.
 * @returns {{kind: string, reason: string, name: string, rootName: string,
 *   type: (string|undefined), code: (string|undefined), message: string,
 *   rootMessage: string, retriable: boolean}}
 *   `retriable` is true when retrying the same operation later may succeed
 *   without user action (kind TRANSIENT or UNKNOWN).
 */
function classifyError(err, ctx) {
    try {
        return classifyUnsafe(err, ctx || {});
    } catch (_) {
        return {
            kind: ErrorKind.UNKNOWN, reason: 'Error', name: 'Error', rootName: 'Error', type: undefined,
            code: undefined, message: str(err), rootMessage: str(err), retriable: true
        };
    }
}

function classifyUnsafe(err, ctx) {
    const chain = flatten(err);
    const root = chain[chain.length - 1];
    const names = chain.map((e) => get(e, 'name')).filter((n) => typeof n === 'string' && n !== '');
    const type = chain.map((e) => get(e, 'type')).find((t) => typeof t === 'string' && t !== '');
    const code = chain.map((e) => get(e, 'code')).find((c) => typeof c === 'string' && c !== '');
    const messages = chain.map(msgOf).join(' | ');
    const base = {
        name: names[0] || 'Error',
        rootName: (typeof get(root, 'name') === 'string' && get(root, 'name')) || names[names.length - 1] || 'Error',
        type,
        code,
        message: msgOf(chain[0]),
        rootMessage: msgOf(root)
    };
    const out = (kind, reason) => ({
        kind,
        reason,
        ...base,
        retriable: kind === ErrorKind.TRANSIENT || kind === ErrorKind.UNKNOWN
    });
    const has = (n) => names.includes(n);

    // 1. our own error classes
    if (has('KafkaNcConfigError')) return out(ErrorKind.CONFIG, 'CONFIG');
    if (has('KafkaNcMessageError')) return out(ErrorKind.MESSAGE, 'INVALID_MESSAGE');
    if (has('KafkaNcQueueFullError')) return out(ErrorKind.MESSAGE, 'QUEUE_FULL');

    // 2. Kafka protocol error type (anywhere in the chain)
    if (type) {
        if (AUTH_TYPES.has(type)) return out(ErrorKind.AUTH, type);
        if (AUTH_CONFIG_TYPES.has(type)) return out(ErrorKind.CONFIG, type);
        if (TOPIC_TYPES.has(type)) return out(ErrorKind.TOPIC, type);
        if (PERMANENT_TYPES.has(type)) return out(ErrorKind.MESSAGE, type);
        if (TRANSIENT_TYPES.has(type)) return out(ErrorKind.TRANSIENT, type);
        if (type === 'KAFKAJS_UNKNOWN_ERROR_CODE') return out(ErrorKind.UNKNOWN, type);
    }

    // 3. TLS trust problems (kafkajs marks them retriable; they are not)
    if (isTlsCode(code)) return out(ErrorKind.AUTH, 'TLS:' + code);

    // 4. SASL, coordinator (usually a hidden GROUP_AUTHORIZATION_FAILED), broker version, codec
    if (has('KafkaJSSASLAuthenticationError')) return out(ErrorKind.AUTH, 'SASL');
    if (has('KafkaJSGroupCoordinatorNotFound')) return out(ErrorKind.AUTH, 'GROUP_COORDINATOR_NOT_FOUND');
    if (has('KafkaJSServerDoesNotSupportApiKey') || has('KafkaJSUnsupportedMagicByteInMessageSet')) {
        return out(ErrorKind.CONFIG, 'BROKER_VERSION');
    }
    if (has('KafkaJSNotImplemented')) return out(ErrorKind.CONFIG, 'CODEC_MISSING');

    // 5. well-known message texts
    if (AUTH_MSG.some((r) => r.test(messages))) return out(ErrorKind.AUTH, 'SASL');
    if (CONFIG_MSG.some((r) => r.test(messages))) return out(ErrorKind.CONFIG, 'CONFIG');
    if (MESSAGE_MSG.some((r) => r.test(messages))) return out(ErrorKind.MESSAGE, 'INVALID_MESSAGE');
    if (/The producer is disconnected/i.test(messages)) return out(ErrorKind.TRANSIENT, 'NOT_CONNECTED');
    if (/The producer is disconnecting/i.test(messages)) return out(ErrorKind.TRANSIENT, 'CLOSING');
    if (/before secure TLS connection was established/i.test(messages)) {
        return out(ErrorKind.TRANSIENT, 'TLS_HANDSHAKE_RESET');
    }

    // 6. network
    if (code && NET_CODES.has(code)) return out(ErrorKind.TRANSIENT, code);
    const connName = names.find((n) => CONNECTIVITY_NAMES.has(n));
    if (connName) return out(ErrorKind.TRANSIENT, connName);

    // 7. programming/argument errors from kafkajs or Node
    const rootName = base.rootName;
    if (JS_ERROR_NAMES.has(rootName)) {
        return ctx.op === 'send'
            ? out(ErrorKind.MESSAGE, 'INVALID_MESSAGE:' + rootName)
            : out(ErrorKind.CONFIG, 'CLIENT_CONFIG:' + rootName);
    }

    // 8. whatever kafkajs says is retriable
    if (get(root, 'retriable') === true || has('KafkaJSNumberOfRetriesExceeded')) {
        return out(ErrorKind.TRANSIENT, 'RETRIABLE');
    }
    return out(ErrorKind.UNKNOWN, names[0] || 'Error');
}

// ---------------------------------------------------------------------------
// formatting

const TLS_SHORT = {
    DEPTH_ZERO_SELF_SIGNED_CERT: 'TLS: self-signed cert',
    SELF_SIGNED_CERT_IN_CHAIN: 'TLS: self-signed cert in chain',
    UNABLE_TO_VERIFY_LEAF_SIGNATURE: 'TLS: untrusted cert (CA?)',
    UNABLE_TO_GET_ISSUER_CERT: 'TLS: untrusted cert (CA?)',
    UNABLE_TO_GET_ISSUER_CERT_LOCALLY: 'TLS: untrusted cert (CA?)',
    CERT_HAS_EXPIRED: 'TLS: cert expired',
    CERT_NOT_YET_VALID: 'TLS: cert not yet valid',
    ERR_TLS_CERT_ALTNAME_INVALID: 'TLS: hostname mismatch',
    HOSTNAME_MISMATCH: 'TLS: hostname mismatch',
    EPROTO: 'TLS: protocol error'
};

const REASON_SHORT = {
    SASL: 'SASL auth failed',
    SASL_AUTHENTICATION_FAILED: 'SASL auth failed',
    UNSUPPORTED_SASL_MECHANISM: 'SASL mechanism unsupported',
    ILLEGAL_SASL_STATE: 'SASL not expected by listener',
    TOPIC_AUTHORIZATION_FAILED: 'not authorized (topic)',
    GROUP_AUTHORIZATION_FAILED: 'not authorized (group)',
    CLUSTER_AUTHORIZATION_FAILED: 'not authorized (cluster)',
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED: 'not authorized (txn id)',
    GROUP_COORDINATOR_NOT_FOUND: 'group coordinator not found',
    UNKNOWN_TOPIC_OR_PARTITION: 'unknown topic/partition',
    MESSAGE_TOO_LARGE: 'message too large',
    RECORD_LIST_TOO_LARGE: 'batch too large',
    INVALID_TOPIC_EXCEPTION: 'invalid topic',
    BROKER_VERSION: 'broker version unsupported',
    CODEC_MISSING: 'compression codec missing',
    NOT_CONNECTED: 'not connected',
    CLOSING: 'disconnecting',
    TLS_HANDSHAKE_RESET: 'TLS handshake failed',
    QUEUE_FULL: 'queue full',
    ECONNRESET: 'connection reset',
    ETIMEDOUT: 'connection timeout',
    ESOCKETTIMEDOUT: 'connection timeout',
    EHOSTUNREACH: 'host unreachable',
    EHOSTDOWN: 'host unreachable',
    ENETUNREACH: 'network unreachable',
    ENETDOWN: 'network unreachable',
    EPIPE: 'connection closed',
    KafkaJSConnectionClosedError: 'connection closed',
    KafkaJSRequestTimeoutError: 'request timeout',
    KafkaJSNoBrokerAvailableError: 'no broker available',
    KafkaJSBrokerNotFound: 'broker not found',
    KafkaJSLockTimeout: 'timeout',
    KafkaJSTimeout: 'timeout'
};

const DNS_CODES = new Set(['ENOTFOUND', 'EAI_AGAIN', 'EAI_FAIL', 'EAI_NODATA', 'EAI_NONAME']);

/** First `broker` ('host:port') found in the chain. */
function brokerOf(chain) {
    for (const e of chain) {
        const b = get(e, 'broker');
        if (typeof b === 'string' && b !== '' && b !== 'undefined:undefined') return b;
    }
    return '';
}

/** Strip noise kafkajs puts in front of socket error messages. */
function cleanMessage(m) {
    return m.replace(/^(Connection error|Failed to connect):\s*/i, '').trim();
}

/**
 * A short, status-friendly description of an error (for node.status texts),
 * e.g. 'ECONNREFUSED 127.0.0.1:9092', 'SASL auth failed',
 * 'broker unreachable (DNS)', 'TLS: self-signed cert'. Never throws.
 *
 * @param {*} err
 * @param {number} [max] maximum length (default 40)
 * @returns {string}
 */
function shortMessage(err, max = 40) {
    try {
        const c = classifyError(err);
        const chain = flatten(err);
        let text;
        if (c.code && TLS_SHORT[c.code]) {
            text = TLS_SHORT[c.code];
        } else if (c.reason.startsWith('TLS:')) {
            text = 'TLS: ' + c.code;
        } else if (c.code && DNS_CODES.has(c.code)) {
            text = 'broker unreachable (DNS)';
        } else if (c.code === 'ECONNREFUSED') {
            const broker = brokerOf(chain);
            text = broker ? `ECONNREFUSED ${broker}` : 'connection refused';
        } else if (REASON_SHORT[c.reason]) {
            text = REASON_SHORT[c.reason];
        } else if (c.code && REASON_SHORT[c.code]) {
            text = REASON_SHORT[c.code];
        } else if (/Connection timeout/i.test(c.rootMessage)) {
            text = 'connection timeout';
        } else if (c.kind === ErrorKind.CONFIG && c.name === 'KafkaNcConfigError') {
            text = 'config error';
        } else if (c.type) {
            text = c.type.toLowerCase().replace(/_/g, ' ');
        } else {
            text = cleanMessage(c.rootMessage || c.message) || c.name;
        }
        return truncate(text, max);
    } catch (_) {
        return truncate('error', max);
    }
}

/**
 * The status text of a failure that needs a human (kind AUTH or CONFIG), used
 * the same way by every client node: 'auth failed' when the credentials were
 * rejected, otherwise shortMessage(), e.g. 'config error' (our ConfigError),
 * 'TLS: self-signed cert', 'not authorized (topic)', 'compression codec missing'.
 * Never throws.
 *
 * @param {*} err
 * @param {number} [max] maximum length (default 40)
 * @returns {string}
 */
function problemText(err, max = 40) {
    try {
        const c = classifyError(err);
        if (c.reason === 'SASL' || c.reason === 'SASL_AUTHENTICATION_FAILED') return truncate('auth failed', max);
        return shortMessage(err, max);
    } catch (_) {
        return truncate('error', max);
    }
}

/**
 * Resolve a TOPIC-kind failure into what the node should do about it:
 *  - the topic came from the message (msg.topic) ⇒ MESSAGE: only that message
 *    is affected, reject or dead-letter it;
 *  - a configured topic does not exist (yet) ⇒ TRANSIENT: it is often created a
 *    moment later (by a producer with auto-creation, or by an admin), so keep
 *    the normal growing backoff instead of waiting a full auth-retry period;
 *  - a configured topic is not authorised ⇒ AUTH: that needs a human, retry slowly.
 * Never throws.
 *
 * @param {*} err
 * @param {boolean} topicFromMsg
 * @returns {string} an ErrorKind
 */
function topicKind(err, topicFromMsg) {
    if (topicFromMsg) return ErrorKind.MESSAGE;
    return classifyError(err).reason === 'UNKNOWN_TOPIC_OR_PARTITION' ? ErrorKind.TRANSIENT : ErrorKind.AUTH;
}

/**
 * The kind of a failure of the connection itself (connect, subscribe, joining
 * the group, a crash of a running consumer), where no single message is to
 * blame. Shared by every client node so they all react the same way:
 *  - TOPIC ⇒ see topicKind() (a topic of the node configuration is missing or
 *    not authorised);
 *  - MESSAGE ⇒ UNKNOWN: there is no message to reject, retry with the normal backoff.
 * Never throws.
 *
 * @param {*} err
 * @param {string} [op] passed to classifyError as ctx.op
 * @returns {string} an ErrorKind
 */
function connectionKind(err, op) {
    const kind = classifyError(err, { op }).kind;
    if (kind === ErrorKind.TOPIC) return topicKind(err, false);
    if (kind === ErrorKind.MESSAGE) return ErrorKind.UNKNOWN;
    return kind;
}

/**
 * Wrap what a kafkajs factory threw synchronously (new Kafka(),
 * kafka.producer(), consumer(), admin(): kafkajs checks some options there) in
 * a ConfigError, so every node reports it the same way: status 'config error'
 * and the slow retry. The original error is kept as `cause`; a ConfigError is
 * returned unchanged. Never throws.
 *
 * @param {*} err
 * @param {string} what e.g. 'Could not create the Kafka producer'
 * @returns {ConfigError}
 */
function asConfigError(err, what) {
    try {
        if (err instanceof ConfigError || get(err, 'name') === 'KafkaNcConfigError') return err;
        let detail = '';
        if (err !== null && (typeof err === 'object' || typeof err === 'function')) detail = msgOf(err);
        else if (err !== undefined && err !== null) detail = str(err); // a thrown string or number
        const text = detail ? `${what}: ${detail}` : what;
        const wrapped = new ConfigError(text, [text]);
        wrapped.cause = err;
        return wrapped;
    } catch (_) {
        return new ConfigError(String(what), [String(what)]);
    }
}

const DESCRIBE_MAX = 2000;

/**
 * A complete one-line description of an error and everything it wraps, e.g.
 * "KafkaJSNumberOfRetriesExceeded: Connection error: connect ECONNREFUSED 127.0.0.1:9092
 *  ← caused by KafkaJSConnectionError [ECONNREFUSED]". Messages that repeat an
 * earlier one (or are part of it) are not printed again. At most 2000
 * characters. Never throws.
 *
 * @param {*} err
 * @returns {string}
 */
function describeError(err) {
    try {
        const chain = flatten(err);
        const seenMessages = new Set();
        const parts = chain.map((e) => {
            const rawName = get(e, 'name');
            const name = typeof rawName === 'string' && rawName !== '' ? rawName : 'Error';
            const code = get(e, 'code');
            const type = get(e, 'type');
            const tag = typeof type === 'string' && type !== ''
                ? ` [${type}]`
                : (typeof code === 'string' && code !== '' ? ` [${code}]` : '');
            let message = msgOf(e);
            const details = get(e, 'details');
            if (name === 'KafkaNcConfigError' && Array.isArray(details) && details.length) {
                const missing = details.map(str).filter((d) => !message.includes(d));
                if (missing.length) message = message ? `${message} (${missing.join('; ')})` : missing.join('; ');
            }
            if (name === 'NonError') return message;
            // A message already printed (or contained in one, e.g. a wrapper that
            // repeats its cause's text) is not printed again: only the name and tag.
            if (message === '' || seenMessages.has(message) ||
                Array.from(seenMessages).some((seen) => seen.includes(message))) return `${name}${tag}`;
            seenMessages.add(message);
            return `${name}${tag}: ${message}`;
        });
        return truncate(parts.join(' ← caused by '), DESCRIBE_MAX);
    } catch (_) {
        return truncate(str(err), DESCRIBE_MAX);
    }
}

/**
 * A JSON-safe summary of an error for msg.error / dead-letter files.
 * Undefined fields are omitted. Never throws.
 *
 * @param {*} err
 * @returns {{name: string, message: string, kind: string, reason: string,
 *   code?: string, type?: string, retriable: boolean}}
 */
function toPlainError(err) {
    const c = classifyError(err);
    const plain = {
        name: c.name,
        message: describeError(err),
        kind: c.kind,
        reason: c.reason,
        retriable: c.retriable
    };
    if (c.code !== undefined) plain.code = c.code;
    if (c.type !== undefined) plain.type = c.type;
    return plain;
}

/**
 * Whether an error comes from kafkajs: some element of its chain has a name
 * starting with 'KafkaJS', or a stack trace that runs through a kafkajs folder.
 * Never throws.
 *
 * @param {*} err
 * @returns {boolean}
 */
function isKafkaJsError(err) {
    try {
        return flatten(err).some((e) => {
            const name = get(e, 'name');
            if (typeof name === 'string' && name.startsWith('KafkaJS')) return true;
            const stack = get(e, 'stack');
            return typeof stack === 'string' && (stack.includes('/kafkajs/') || stack.includes('\\kafkajs\\'));
        });
    } catch (_) {
        return false;
    }
}

module.exports = {
    ConfigError,
    MessageError,
    QueueFullError,
    ErrorKind,
    flatten,
    classifyError,
    shortMessage,
    describeError,
    toPlainError,
    problemText,
    connectionKind,
    topicKind,
    asConfigError,
    isKafkaJsError,
    // exported for tests and for nodes that want to inspect the tables
    _tables: { NET_CODES, TLS_CODES, AUTH_TYPES, AUTH_CONFIG_TYPES, TOPIC_TYPES, PERMANENT_TYPES, TRANSIENT_TYPES,
        CONNECTIVITY_NAMES }
};
