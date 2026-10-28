'use strict';

const assert = require('node:assert/strict');
const K = require('kafkajs/src/errors');
const { createErrorFromCode, errorCodes } = require('kafkajs/src/protocol/error');
const {
    ConfigError, MessageError, QueueFullError, ErrorKind, flatten, classifyError, shortMessage, describeError,
    toPlainError, isKafkaJsError, problemText, connectionKind, topicKind, asConfigError
} = require('../../lib/errors');

// ---------------------------------------------------------------------------
// Factories reproducing the error shapes kafkajs 2.2.4 really produces
// (5-kafkajs-internals.md §1.2 / §1.3), using the REAL kafkajs classes.

const BROKER = '127.0.0.1:9092';

/** KafkaJSProtocolError for a protocol error type (as kafkajs creates it from a response). */
function proto(type) {
    const entry = errorCodes.find((e) => e.type === type);
    if (!entry) throw new Error(`unknown protocol type ${type}`);
    return createErrorFromCode(entry.code);
}

/** Socket error as converted at network/connection.js:207-219. */
function conn(code, message, broker = BROKER) {
    return new K.KafkaJSConnectionError(`Connection error: ${message}`, { broker, code });
}

/** Retries exhausted (retry/index.js:51). */
function retriesExceeded(cause) {
    return new K.KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 4800 });
}

/** Non-retriable error thrown inside a retried function (retry/index.js:55). */
function nonRetriableWrap(e) {
    return new K.KafkaJSNonRetriableError(e, { cause: e.cause || e });
}

/** Node.js TypeError with code, as Buffer.from() throws it for objects. */
function invalidArgType() {
    const e = new TypeError('The first argument must be of type string or an instance of Buffer, ArrayBuffer, '
        + 'or Array or an Array-like Object. Received an instance of Object');
    e.code = 'ERR_INVALID_ARG_TYPE';
    return e;
}

const { TRANSIENT, AUTH, CONFIG, MESSAGE, TOPIC, UNKNOWN } = ErrorKind;

// [description, error factory, ctx, expected kind, expected reason (optional)]
const CASES = [
    // --- connectivity (§1.2 connect / send rows, §1.3) ---
    ['connect: broker unreachable (ECONNREFUSED)',
        () => retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092')), { op: 'connect' },
        TRANSIENT, 'ECONNREFUSED'],
    ['send during outage after a successful connect (NonRetriable wrapping)',
        () => nonRetriableWrap(retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092'))),
        { op: 'send' }, TRANSIENT, 'ECONNREFUSED'],
    ['DNS name does not exist (ENOTFOUND)',
        () => retriesExceeded(conn('ENOTFOUND', 'getaddrinfo ENOTFOUND nonexistent.invalid', 'nonexistent.invalid:9092')),
        {}, TRANSIENT, 'ENOTFOUND'],
    ['DNS temporarily failing (EAI_AGAIN)', () => retriesExceeded(conn('EAI_AGAIN', 'getaddrinfo EAI_AGAIN kafka')), {},
        TRANSIENT, 'EAI_AGAIN'],
    ['no route (ENETUNREACH)',
        () => retriesExceeded(conn('ENETUNREACH', 'connect ENETUNREACH 255.255.255.255:9092 - Local (0.0.0.0:0)')), {},
        TRANSIENT, 'ENETUNREACH'],
    ['host unreachable (EHOSTUNREACH)', () => conn('EHOSTUNREACH', 'connect EHOSTUNREACH 10.0.0.1:9092'), {},
        TRANSIENT, 'EHOSTUNREACH'],
    ['socket timeout (ETIMEDOUT)', () => conn('ETIMEDOUT', 'connect ETIMEDOUT 10.0.0.1:9092'), {}, TRANSIENT, 'ETIMEDOUT'],
    ['broken pipe (EPIPE)', () => nonRetriableWrap(conn('EPIPE', 'write EPIPE')), { op: 'send' }, TRANSIENT, 'EPIPE'],
    ['kafkajs connection timeout (no code)', () => retriesExceeded(new K.KafkaJSConnectionError('Connection timeout',
        { broker: '240.0.0.1:9092' })), {}, TRANSIENT, 'KafkaJSConnectionError'],
    ['TLS client to a PLAINTEXT listener (ECONNRESET during handshake)',
        () => retriesExceeded(conn('ECONNRESET',
            'Client network socket disconnected before secure TLS connection was established')), {},
        TRANSIENT, 'TLS_HANDSHAKE_RESET'],
    ['no SASL against a SASL listener: closed connection on send',
        () => retriesExceeded(new K.KafkaJSConnectionClosedError('Closed connection', { host: '127.0.0.1', port: 9094 })),
        { op: 'send' }, TRANSIENT, 'KafkaJSConnectionClosedError'],
    ['request timeout', () => new K.KafkaJSRequestTimeoutError('Request Produce(key: 0, version: 7) timed out',
        { broker: BROKER, correlationId: 3 }), {}, TRANSIENT, 'KafkaJSRequestTimeoutError'],
    ['no broker available', () => new K.KafkaJSNoBrokerAvailableError(), {}, TRANSIENT, 'KafkaJSNoBrokerAvailableError'],
    ['lock timeout', () => new K.KafkaJSLockTimeout('Timeout while acquiring lock (1 waiting locks)'), {}, TRANSIENT,
        'KafkaJSLockTimeout'],
    ['topic metadata not loaded', () => new K.KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded',
        { topic: 't' }), {}, TRANSIENT, 'KafkaJSTopicMetadataNotLoaded'],
    ['producer used before connect / after failed connect', () => new K.KafkaJSError('The producer is disconnected'),
        { op: 'send' }, TRANSIENT, 'NOT_CONNECTED'],
    ['producer disconnecting',
        () => new K.KafkaJSNonRetriableError("The producer is disconnecting; therefore, it can't safely accept messages anymore"),
        { op: 'send' }, TRANSIENT, 'CLOSING'],
    ['LEADER_NOT_AVAILABLE after retries', () => retriesExceeded(proto('LEADER_NOT_AVAILABLE')), {}, TRANSIENT,
        'LEADER_NOT_AVAILABLE'],
    ['NOT_LEADER_FOR_PARTITION', () => retriesExceeded(proto('NOT_LEADER_FOR_PARTITION')), {}, TRANSIENT,
        'NOT_LEADER_FOR_PARTITION'],
    ['BROKER_NOT_AVAILABLE (kafkajs: non-retriable, transient in practice)', () => proto('BROKER_NOT_AVAILABLE'), {},
        TRANSIENT, 'BROKER_NOT_AVAILABLE'],
    ['REBALANCE_IN_PROGRESS', () => nonRetriableWrap(proto('REBALANCE_IN_PROGRESS')), { op: 'run' }, TRANSIENT,
        'REBALANCE_IN_PROGRESS'],
    ['ILLEGAL_GENERATION', () => proto('ILLEGAL_GENERATION'), {}, TRANSIENT, 'ILLEGAL_GENERATION'],
    ['NOT_ENOUGH_REPLICAS', () => retriesExceeded(proto('NOT_ENOUGH_REPLICAS')), { op: 'send' }, TRANSIENT,
        'NOT_ENOUGH_REPLICAS'],
    ['Node AggregateError of connection errors (autoSelectFamily)', () => {
        const a = new Error('connect ECONNREFUSED ::1:9092');
        a.code = 'ECONNREFUSED';
        const b = new Error('connect ECONNREFUSED 127.0.0.1:9092');
        b.code = 'ECONNREFUSED';
        return new AggregateError([a, b], '');
    }, {}, TRANSIENT, 'ECONNREFUSED'],
    ['generic retriable KafkaJSError', () => new K.KafkaJSError('something transient'), {}, TRANSIENT, 'RETRIABLE'],
    ['retries exceeded around a plain Error', () => retriesExceeded(new Error('weird')), {}, TRANSIENT, 'RETRIABLE'],

    // --- TLS trust (§1.3) ---
    ['self-signed certificate',
        () => retriesExceeded(conn('DEPTH_ZERO_SELF_SIGNED_CERT', 'self-signed certificate')), {}, AUTH,
        'TLS:DEPTH_ZERO_SELF_SIGNED_CERT'],
    ['hostname not in certificate',
        () => retriesExceeded(conn('ERR_TLS_CERT_ALTNAME_INVALID',
            "Hostname/IP does not match certificate's altnames: Host: kafka. is not cert's CN: other")), {},
        AUTH, 'TLS:ERR_TLS_CERT_ALTNAME_INVALID'],
    ['certificate expired', () => conn('CERT_HAS_EXPIRED', 'certificate has expired'), {}, AUTH, 'TLS:CERT_HAS_EXPIRED'],
    ['untrusted CA', () => conn('UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'unable to verify the first certificate'), {}, AUTH,
        'TLS:UNABLE_TO_VERIFY_LEAF_SIGNATURE'],
    ['OpenSSL error code (ERR_SSL_*)', () => conn('ERR_SSL_WRONG_VERSION_NUMBER', 'wrong version number'), {}, AUTH,
        'TLS:ERR_SSL_WRONG_VERSION_NUMBER'],

    // --- authentication / authorisation ---
    ['SASL wrong password / missing username (raw, bail path)',
        () => new K.KafkaJSSASLAuthenticationError('SASL PLAIN authentication failed: Invalid username or password'),
        { op: 'connect' }, AUTH, 'SASL'],
    ['SASL SCRAM failure', () => new K.KafkaJSSASLAuthenticationError(
        'SASL SCRAM SHA256 authentication failed: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256'),
    {}, AUTH, 'SASL'],
    ['unsupported SASL mechanism (raw protocol error)', () => proto('UNSUPPORTED_SASL_MECHANISM'), {}, AUTH,
        'UNSUPPORTED_SASL_MECHANISM'],
    ['SASL against a PLAINTEXT listener', () => proto('ILLEGAL_SASL_STATE'), {}, AUTH, 'ILLEGAL_SASL_STATE'],
    ['group ACL missing', () => proto('GROUP_AUTHORIZATION_FAILED'), { op: 'run' }, AUTH, 'GROUP_AUTHORIZATION_FAILED'],
    ['cluster ACL missing', () => nonRetriableWrap(proto('CLUSTER_AUTHORIZATION_FAILED')), { op: 'admin' }, AUTH,
        'CLUSTER_AUTHORIZATION_FAILED'],
    ['group coordinator not found (hidden GROUP_AUTHORIZATION_FAILED)',
        () => new K.KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator'), { op: 'run' }, AUTH,
        'GROUP_COORDINATOR_NOT_FOUND'],

    // --- topic ---
    ['TOPIC_AUTHORIZATION_FAILED on send (wrapped)', () => nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED')),
        { op: 'send' }, TOPIC, 'TOPIC_AUTHORIZATION_FAILED'],
    ['TOPIC_AUTHORIZATION_FAILED on subscribe (raw)', () => proto('TOPIC_AUTHORIZATION_FAILED'), { op: 'subscribe' },
        TOPIC, 'TOPIC_AUTHORIZATION_FAILED'],
    ['UNKNOWN_TOPIC_OR_PARTITION on send without auto-create (retries exceeded)',
        () => retriesExceeded(proto('UNKNOWN_TOPIC_OR_PARTITION')), { op: 'send' }, TOPIC, 'UNKNOWN_TOPIC_OR_PARTITION'],
    ['UNKNOWN_TOPIC_OR_PARTITION on subscribe (raw)', () => proto('UNKNOWN_TOPIC_OR_PARTITION'), { op: 'subscribe' },
        TOPIC, 'UNKNOWN_TOPIC_OR_PARTITION'],

    // --- message (permanent) ---
    ['MESSAGE_TOO_LARGE (raw)', () => proto('MESSAGE_TOO_LARGE'), { op: 'send' }, MESSAGE, 'MESSAGE_TOO_LARGE'],
    ['INVALID_REQUIRED_ACKS (acks 2)', () => proto('INVALID_REQUIRED_ACKS'), { op: 'send' }, MESSAGE,
        'INVALID_REQUIRED_ACKS'],
    ['INVALID_TOPIC_EXCEPTION on send (wrapped)', () => nonRetriableWrap(proto('INVALID_TOPIC_EXCEPTION')),
        { op: 'send' }, MESSAGE, 'INVALID_TOPIC_EXCEPTION'],
    ['CORRUPT_MESSAGE', () => proto('CORRUPT_MESSAGE'), { op: 'send' }, MESSAGE, 'CORRUPT_MESSAGE'],
    ['offset out of range', () => new K.KafkaJSOffsetOutOfRange(
        errorCodes.find((e) => e.type === 'OFFSET_OUT_OF_RANGE'), { topic: 't', partition: 0 }), {}, MESSAGE,
    'OFFSET_OUT_OF_RANGE'],
    ['value undefined', () => new K.KafkaJSNonRetriableError('Invalid message without value for topic "t": {"key":"k"}'),
        { op: 'send' }, MESSAGE, 'INVALID_MESSAGE'],
    ['value is an object (Buffer.from TypeError)', invalidArgType, { op: 'send' }, MESSAGE, 'INVALID_MESSAGE'],
    ['value is an object, classified outside send', invalidArgType, {}, MESSAGE, 'INVALID_MESSAGE'],
    ["timestamp 'abc'", () => new SyntaxError('Cannot convert abc to a BigInt'), { op: 'send' }, MESSAGE,
        'INVALID_MESSAGE'],
    ["topic ''", () => new K.KafkaJSNonRetriableError('Invalid topic'), { op: 'send' }, MESSAGE, 'INVALID_MESSAGE'],
    ['messages: null', () => new K.KafkaJSNonRetriableError('Invalid messages array [null] for topic "t"'),
        { op: 'send' }, MESSAGE, 'INVALID_MESSAGE'],
    ['createTopics: topic already exists (KafkaJSAggregateError)', () => new K.KafkaJSAggregateError(
        'Topic creation errors', [new K.KafkaJSCreateTopicError(errorCodes.find((e) => e.type === 'TOPIC_ALREADY_EXISTS'),
            't')]), { op: 'admin' }, MESSAGE, 'TOPIC_ALREADY_EXISTS'],
    ['deleteTopicRecords partition error', () => new K.KafkaJSDeleteTopicRecordsError({
        partitions: [{ partition: 0, offset: '5', error: proto('OFFSET_OUT_OF_RANGE') }]
    }), { op: 'admin' }, MESSAGE, 'OFFSET_OUT_OF_RANGE'],
    ['generic TypeError on send', () => new TypeError("Cannot read properties of undefined (reading 'length')"),
        { op: 'send' }, MESSAGE, 'INVALID_MESSAGE:TypeError'],
    ['RangeError on send', () => new RangeError('Invalid array length'), { op: 'send' }, MESSAGE,
        'INVALID_MESSAGE:RangeError'],

    // --- configuration ---
    ['generic TypeError on connect', () => new TypeError("Cannot read properties of undefined (reading 'length')"),
        { op: 'connect' }, CONFIG, 'CLIENT_CONFIG:TypeError'],
    ['invalid port from the socket factory',
        () => retriesExceeded(new K.KafkaJSConnectionError(
            'Failed to connect: Port should be >= 0 and < 65536. Received type number (NaN).', { broker: 'localhost:undefined' })),
        { op: 'connect' }, CONFIG, 'CONFIG'],
    ['garbage TLS key/cert (PEM)', () => retriesExceeded(new K.KafkaJSConnectionError(
        'Failed to connect: error:0480006C:PEM routines::no start line', { broker: BROKER })), {}, CONFIG, 'CONFIG'],
    ['empty brokers array', () => new K.KafkaJSNonRetriableError('Failed to connect: brokers array is empty'), {},
        CONFIG, 'CONFIG'],
    ['invalid broker entry', () => new K.KafkaJSNonRetriableError('Failed to connect: broker at index 0 is invalid "number"'),
        {}, CONFIG, 'CONFIG'],
    ['sasl: {} (raw TypeError)', () => new TypeError("Cannot read properties of undefined (reading 'toUpperCase')"),
        { op: 'connect' }, CONFIG, 'CONFIG'],
    ['requestTimeout as string (Lock TypeError)', () => {
        try {
            const Lock = require('kafkajs/src/utils/lock');
            return new Lock({ timeout: '30000' });
        } catch (e) {
            return e;
        }
    }, {}, CONFIG, 'CONFIG'],
    ['unknown compression id', () => new TypeError("Cannot read properties of null (reading 'compress')"),
        { op: 'send' }, CONFIG, 'CONFIG'],
    ['missing codec (Snappy) on a consumer',
        () => new K.KafkaJSNotImplemented('Snappy compression not implemented'), { op: 'run' }, CONFIG, 'CODEC_MISSING'],
    ['broker too old', () => new K.KafkaJSServerDoesNotSupportApiKey(
        'The Kafka server does not support the requested API version', { apiKey: 0, apiName: 'Produce' }), {}, CONFIG,
    'BROKER_VERSION'],
    ['INVALID_SESSION_TIMEOUT', () => proto('INVALID_SESSION_TIMEOUT'), { op: 'run' }, CONFIG, 'INVALID_SESSION_TIMEOUT'],
    ['our ConfigError', () => new ConfigError('SASL password is required', ['SASL password is required']), {}, CONFIG,
        'CONFIG'],

    // --- ours ---
    ['our MessageError', () => new MessageError('msg.payload is undefined'), {}, MESSAGE, 'INVALID_MESSAGE'],
    ['our QueueFullError', () => new QueueFullError('Offline queue is full'), {}, MESSAGE, 'QUEUE_FULL'],

    // --- unknown ---
    ['unknown protocol error code (THROTTLING_QUOTA_EXCEEDED in 2.2.4)', () => createErrorFromCode(89), {}, UNKNOWN,
        'KAFKAJS_UNKNOWN_ERROR_CODE'],
    ['non-retriable KafkaJS error with an unrecognised message', () => new K.KafkaJSNonRetriableError('weird failure'),
        {}, UNKNOWN, 'KafkaJSNonRetriableError'],
    ['plain Error', () => new Error('boom'), {}, UNKNOWN, 'Error'],
    ['admin timeout error from our own code', () => new Error('Kafka admin listTopics timed out after 30000ms'),
        { op: 'admin' }, UNKNOWN, 'Error'],
    ['thrown string', () => 'just a string', {}, UNKNOWN, 'NonError'],
    ['undefined', () => undefined, {}, UNKNOWN, 'NonError'],
    ['null', () => null, {}, UNKNOWN, 'NonError']
];

describe('lib/errors', function () {
    describe('classifyError: observed kafkajs error shapes', function () {
        it(`has at least 40 cases (${CASES.length})`, function () {
            assert.ok(CASES.length >= 40);
        });

        for (const [title, make, ctx, kind, reason] of CASES) {
            it(`${title} → ${kind}${reason ? ' / ' + reason : ''}`, function () {
                const err = make();
                const c = classifyError(err, ctx);
                assert.equal(c.kind, kind, `kind for ${describeError(err)}`);
                if (reason) assert.equal(c.reason, reason);
                assert.equal(typeof c.message, 'string');
                assert.equal(typeof c.rootMessage, 'string');
                assert.equal(typeof c.name, 'string');
                assert.equal(typeof c.rootName, 'string');
                assert.equal(c.retriable, kind === TRANSIENT || kind === UNKNOWN);
            });
        }
    });

    describe('classifyError: details', function () {
        it('reports the chain fields of a wrapped connection error', function () {
            const err = nonRetriableWrap(retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092')));
            const c = classifyError(err, { op: 'send' });
            assert.equal(c.name, 'KafkaJSNonRetriableError');
            assert.equal(c.rootName, 'KafkaJSConnectionError');
            assert.equal(c.code, 'ECONNREFUSED');
            assert.equal(c.type, undefined);
            assert.equal(c.message, 'Connection error: connect ECONNREFUSED 127.0.0.1:9092');
        });

        it('reports the protocol type and ignores numeric protocol codes as `code`', function () {
            const c = classifyError(retriesExceeded(proto('UNKNOWN_TOPIC_OR_PARTITION')));
            assert.equal(c.type, 'UNKNOWN_TOPIC_OR_PARTITION');
            assert.equal(c.code, undefined);
        });

        it('checks the TLS code before kafkajs\' retriable=true', function () {
            const e = conn('SELF_SIGNED_CERT_IN_CHAIN', 'self-signed certificate in certificate chain');
            assert.equal(e.retriable, true);
            assert.equal(classifyError(e).kind, AUTH);
        });

        it('never throws on hostile input', function () {
            const hostile = new Proxy({}, { get() { throw new Error('trap'); } });
            const cyclic = new Error('a');
            cyclic.cause = new Error('b');
            cyclic.cause.cause = cyclic;
            const getterBomb = Object.defineProperty(new Error('x'), 'cause', { get() { throw new Error('getter'); } });
            for (const v of [hostile, cyclic, getterBomb, Symbol('s'), () => 1, 42, {}, [], { name: 5, message: {} }]) {
                const c = classifyError(v);
                assert.ok(Object.values(ErrorKind).includes(c.kind));
            }
        });

        it('stops walking after 20 chain elements', function () {
            let e = new Error('deepest');
            for (let i = 0; i < 100; i++) {
                const outer = new Error(`level ${i}`);
                outer.cause = e;
                e = outer;
            }
            assert.equal(flatten(e).length, 20);
            assert.equal(classifyError(e).kind, UNKNOWN);
        });
    });

    describe('flatten', function () {
        it('walks cause, originalError, errors[] and partitions[].error', function () {
            const a = new Error('a');
            const b = new Error('b');
            const c = new Error('c');
            const d = new Error('d');
            const top = new Error('top');
            top.cause = a;
            a.originalError = b;
            b.errors = [c, null];
            c.partitions = [{ error: d }, null, {}];
            assert.deepEqual(flatten(top).map((e) => e.message), ['top', 'a', 'b', 'c', 'd']);
        });

        it('is cycle safe', function () {
            const a = new Error('a');
            const b = new Error('b');
            a.cause = b;
            b.cause = a;
            b.errors = [a, b];
            assert.deepEqual(flatten(a).map((e) => e.message), ['a', 'b']);
        });

        it('wraps non-objects', function () {
            assert.deepEqual(flatten('s'), [{ name: 'NonError', message: 's' }]);
            assert.deepEqual(flatten(undefined), [{ name: 'NonError', message: 'undefined' }]);
            assert.deepEqual(flatten(null), [{ name: 'NonError', message: 'null' }]);
            const e = new Error('x');
            e.cause = 'string cause';
            assert.deepEqual(flatten(e)[1], { name: 'NonError', message: 'string cause' });
        });
    });

    describe('shortMessage', function () {
        const cases = [
            [retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092')), 'ECONNREFUSED 127.0.0.1:9092'],
            [nonRetriableWrap(retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 10.1.2.3:9092', '10.1.2.3:9092'))),
                'ECONNREFUSED 10.1.2.3:9092'],
            [new K.KafkaJSSASLAuthenticationError('SASL PLAIN authentication failed: Invalid username or password'),
                'SASL auth failed'],
            [retriesExceeded(conn('ENOTFOUND', 'getaddrinfo ENOTFOUND nope.invalid')), 'broker unreachable (DNS)'],
            [retriesExceeded(conn('DEPTH_ZERO_SELF_SIGNED_CERT', 'self-signed certificate')), 'TLS: self-signed cert'],
            [conn('ERR_TLS_CERT_ALTNAME_INVALID', 'Hostname/IP does not match'), 'TLS: hostname mismatch'],
            [conn('ERR_SSL_SOMETHING', 'x'), 'TLS: ERR_SSL_SOMETHING'],
            [retriesExceeded(new K.KafkaJSConnectionError('Connection timeout', { broker: BROKER })), 'connection timeout'],
            [conn('ECONNRESET', 'Client network socket disconnected before secure TLS connection was established'),
                'TLS handshake failed'],
            [new K.KafkaJSError('The producer is disconnected'), 'not connected'],
            [nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED')), 'not authorized (topic)'],
            [retriesExceeded(proto('UNKNOWN_TOPIC_OR_PARTITION')), 'unknown topic/partition'],
            [proto('MESSAGE_TOO_LARGE'), 'message too large'],
            [proto('NOT_ENOUGH_REPLICAS'), 'not enough replicas'],
            [new K.KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator'), 'group coordinator not found'],
            [new ConfigError('SASL password is required', ['SASL password is required']), 'config error'],
            [new QueueFullError('full'), 'queue full'],
            [retriesExceeded(new K.KafkaJSConnectionClosedError('Closed connection', { host: 'h', port: 1 })),
                'connection closed'],
            [new Error('something odd happened'), 'something odd happened'],
            ['plain string', 'plain string'],
            [undefined, 'undefined']
        ];
        for (const [err, expected] of cases) {
            it(`${describeError(err).slice(0, 60)} → '${expected}'`, function () {
                assert.equal(shortMessage(err), expected);
            });
        }

        it('respects the maximum length', function () {
            const s = shortMessage(new Error('x'.repeat(200)), 20);
            assert.equal(s.length, 20);
            assert.ok(s.endsWith('…'));
            assert.ok(shortMessage(new Error('y'.repeat(200))).length <= 40);
        });

        it('never throws', function () {
            assert.equal(typeof shortMessage(new Proxy({}, { get() { throw new Error('trap'); } })), 'string');
        });
    });

    describe('describeError', function () {
        it('describes the chain and drops repeated messages', function () {
            const err = retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092'));
            assert.equal(describeError(err),
                'KafkaJSNumberOfRetriesExceeded: Connection error: connect ECONNREFUSED 127.0.0.1:9092'
                + ' ← caused by KafkaJSConnectionError [ECONNREFUSED]');
        });

        it('shows protocol types', function () {
            assert.equal(describeError(nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED'))),
                'KafkaJSNonRetriableError: Not authorized to access topics: [Topic authorization failed]'
                + ' ← caused by KafkaJSProtocolError [TOPIC_AUTHORIZATION_FAILED]');
        });

        it('shows distinct messages of wrapped errors', function () {
            const inner = new Error('inner problem');
            const outer = new Error('outer problem', { cause: inner });
            assert.equal(describeError(outer), 'Error: outer problem ← caused by Error: inner problem');
        });

        it('adds ConfigError details missing from the message', function () {
            const e = new ConfigError('Invalid broker configuration', ['No brokers configured', 'SASL password is required']);
            assert.equal(describeError(e),
                'KafkaNcConfigError: Invalid broker configuration (No brokers configured; SASL password is required)');
            const f = new ConfigError('SASL password is required', ['SASL password is required']);
            assert.equal(describeError(f), 'KafkaNcConfigError: SASL password is required');
        });

        it('does not repeat a cause message that a wrapper already contains', function () {
            const cause = new TypeError("'timeout' is not a number, received 'string'");
            const wrapped = new Error(`Could not create the Kafka producer: ${cause.message}`, { cause });
            assert.equal(describeError(wrapped),
                "Error: Could not create the Kafka producer: 'timeout' is not a number, received 'string'"
                + ' ← caused by TypeError');
        });

        it('handles non-errors and caps the length at 2000', function () {
            assert.equal(describeError('boom'), 'boom');
            assert.equal(describeError(undefined), 'undefined');
            assert.equal(describeError(new Error('z'.repeat(5000))).length, 2000);
            assert.equal(typeof describeError(new Proxy({}, { get() { throw new Error('trap'); } })), 'string');
        });
    });

    describe('problemText (status text of AUTH/CONFIG failures, shared by every node)', function () {
        it("is 'auth failed' for rejected credentials", function () {
            assert.equal(problemText(new K.KafkaJSSASLAuthenticationError('SASL PLAIN authentication failed: Invalid username or password')),
                'auth failed');
            assert.equal(problemText(nonRetriableWrap(proto('SASL_AUTHENTICATION_FAILED'))), 'auth failed');
        });

        it('is the short message for every other problem', function () {
            assert.equal(problemText(new ConfigError('SASL password is required', ['SASL password is required'])), 'config error');
            assert.equal(problemText(retriesExceeded(conn('DEPTH_ZERO_SELF_SIGNED_CERT', 'self-signed certificate'))),
                'TLS: self-signed cert');
            assert.equal(problemText(nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED'))), 'not authorized (topic)');
            assert.equal(problemText(proto('UNSUPPORTED_SASL_MECHANISM')), 'SASL mechanism unsupported');
            assert.equal(problemText(new K.KafkaJSNotImplemented('ZSTD compression not implemented')),
                'compression codec missing');
        });

        it('honours max and never throws', function () {
            assert.equal(problemText(new ConfigError('x'), 6), 'confi…');
            assert.equal(typeof problemText(new Proxy({}, { get() { throw new Error('trap'); } })), 'string');
            assert.equal(typeof problemText(undefined), 'string');
        });
    });

    describe('connectionKind (failures of the connection itself)', function () {
        it('maps a missing topic to TRANSIENT, other TOPIC errors to AUTH and MESSAGE to UNKNOWN, keeps the rest', function () {
            // a configured topic that does not exist yet is often created a moment later: normal backoff
            assert.equal(connectionKind(nonRetriableWrap(proto('UNKNOWN_TOPIC_OR_PARTITION')), 'consume'), TRANSIENT);
            assert.equal(connectionKind(nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED')), 'connect'), AUTH);
            assert.equal(connectionKind(proto('CORRUPT_MESSAGE'), 'consume'), UNKNOWN);
            assert.equal(connectionKind(retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092'))), TRANSIENT);
            assert.equal(connectionKind(new K.KafkaJSSASLAuthenticationError('SASL PLAIN authentication failed')), AUTH);
            assert.equal(connectionKind(new ConfigError('bad')), CONFIG);
            // a TypeError outside of send() is a configuration problem
            assert.equal(connectionKind(new TypeError('boom'), 'connect'), CONFIG);
        });

        it('never throws', function () {
            assert.equal(connectionKind(new Proxy({}, { get() { throw new Error('trap'); } })), UNKNOWN);
        });
    });

    describe('topicKind', function () {
        it('a topic from the message only affects that message', function () {
            assert.equal(topicKind(proto('UNKNOWN_TOPIC_OR_PARTITION'), true), MESSAGE);
            assert.equal(topicKind(proto('TOPIC_AUTHORIZATION_FAILED'), true), MESSAGE);
        });

        it('a configured topic: missing ⇒ TRANSIENT, not authorised ⇒ AUTH', function () {
            assert.equal(topicKind(retriesExceeded(proto('UNKNOWN_TOPIC_OR_PARTITION')), false), TRANSIENT);
            assert.equal(topicKind(nonRetriableWrap(proto('TOPIC_AUTHORIZATION_FAILED')), false), AUTH);
        });
    });

    describe('asConfigError (synchronous throws of kafkajs factories)', function () {
        it('wraps the error, keeps it as cause, and classifies as CONFIG', function () {
            const cause = new TypeError("'timeout' is not a number, received 'string'");
            const e = asConfigError(cause, 'Could not create the Kafka producer');
            assert.ok(e instanceof ConfigError);
            assert.equal(e.message, "Could not create the Kafka producer: 'timeout' is not a number, received 'string'");
            assert.deepEqual(e.details, [e.message]);
            assert.equal(e.cause, cause);
            assert.equal(classifyError(e).kind, CONFIG);
            assert.equal(shortMessage(e), 'config error');
            assert.equal(problemText(e), 'config error');
        });

        it('returns a ConfigError unchanged and copes with odd values', function () {
            const c = new ConfigError('TLS configuration node not found');
            assert.equal(asConfigError(c, 'x'), c);
            assert.equal(asConfigError(undefined, 'Could not create').message, 'Could not create');
            assert.equal(asConfigError('text', 'Could not create').message, 'Could not create: text');
            const hostile = new Proxy({}, { get() { throw new Error('trap'); }, getPrototypeOf() { throw new Error('trap'); } });
            assert.equal(asConfigError(hostile, 'Could not create').name, 'KafkaNcConfigError');
        });
    });

    describe('toPlainError', function () {
        it('returns a JSON-safe summary', function () {
            const err = retriesExceeded(conn('ECONNREFUSED', 'connect ECONNREFUSED 127.0.0.1:9092'));
            const p = toPlainError(err);
            assert.deepEqual(p, {
                name: 'KafkaJSNumberOfRetriesExceeded',
                message: describeError(err),
                kind: 'transient',
                reason: 'ECONNREFUSED',
                code: 'ECONNREFUSED',
                retriable: true
            });
            assert.deepEqual(JSON.parse(JSON.stringify(p)), p);
        });

        it('includes the protocol type and omits undefined fields', function () {
            const p = toPlainError(proto('MESSAGE_TOO_LARGE'));
            assert.equal(p.type, 'MESSAGE_TOO_LARGE');
            assert.equal(p.kind, 'message');
            assert.equal(p.retriable, false);
            assert.ok(!('code' in p));
        });

        it('handles circular errors', function () {
            const e = new Error('circ');
            e.self = e;
            e.cause = e;
            assert.doesNotThrow(() => JSON.stringify(toPlainError(e)));
        });
    });

    describe('isKafkaJsError', function () {
        it('recognises kafkajs errors and wrappers around them', function () {
            assert.equal(isKafkaJsError(retriesExceeded(conn('ECONNREFUSED', 'x'))), true);
            assert.equal(isKafkaJsError(new Error('wrapper', { cause: new K.KafkaJSError('inner') })), true);
            const named = new Error('x');
            named.name = 'KafkaJSConnectionError';
            assert.equal(isKafkaJsError(named), true);
        });

        it('recognises errors thrown from kafkajs source files', function () {
            let thrown;
            try {
                const Lock = require('kafkajs/src/utils/lock');
                new Lock({ timeout: 'x' });
            } catch (e) {
                thrown = e;
            }
            assert.equal(thrown.name, 'TypeError');
            assert.equal(isKafkaJsError(thrown), true);
            const fake = new TypeError('x');
            fake.stack = 'TypeError: x\n    at C:\\app\\node_modules\\kafkajs\\src\\index.js:1:1';
            assert.equal(isKafkaJsError(fake), true);
        });

        it('rejects everything else', function () {
            assert.equal(isKafkaJsError(new Error('plain')), false);
            assert.equal(isKafkaJsError(new TypeError('plain')), false);
            assert.equal(isKafkaJsError(new ConfigError('ours')), false);
            assert.equal(isKafkaJsError('KafkaJS string'), false);
            assert.equal(isKafkaJsError(null), false);
            assert.equal(isKafkaJsError(undefined), false);
            assert.equal(isKafkaJsError(new Proxy({}, { get() { throw new Error('trap'); } })), false);
        });
    });

    describe('error classes', function () {
        it('ConfigError', function () {
            const e = new ConfigError('bad', ['a', 'b']);
            assert.ok(e instanceof Error);
            assert.equal(e.name, 'KafkaNcConfigError');
            assert.equal(e.message, 'bad');
            assert.deepEqual(e.details, ['a', 'b']);
            assert.deepEqual(new ConfigError('x').details, []);
            assert.deepEqual(new ConfigError('x', 'not an array').details, []);
        });

        it('MessageError and QueueFullError', function () {
            assert.equal(new MessageError('m').name, 'KafkaNcMessageError');
            assert.equal(new QueueFullError('q').name, 'KafkaNcQueueFullError');
            assert.ok(new MessageError('m') instanceof Error);
        });

        it('ErrorKind is frozen', function () {
            assert.ok(Object.isFrozen(ErrorKind));
            assert.deepEqual(Object.values(ErrorKind).sort(),
                ['auth', 'config', 'message', 'topic', 'transient', 'unknown']);
        });
    });
});
