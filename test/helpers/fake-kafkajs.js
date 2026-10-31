'use strict';

/**
 * A controllable, in-memory fake of the kafkajs 2.2.4 API, for node tests.
 *
 *     const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
 *     const factory = require('../../lib/kafka-factory');
 *     const fake = createFakeKafkajs();
 *     factory.setKafkajs(fake);        // and factory.setKafkajs(null) afterwards
 *     fake.behaviour.connectError = fake.errors.connectionRefused();
 *
 * Fidelity matters more than convenience here: the nodes are only as
 * crash-proof as the fake is realistic. Where kafkajs has a sharp edge, the
 * fake has the same one (all verified against node_modules/kafkajs/src and
 * 5-kafkajs-internals.md), for example:
 * - event names, the {id, type, timestamp, payload} event object and the
 *   listener wrapper come from kafkajs itself: a listener that throws
 *   synchronously makes emit() throw (in kafkajs that crashes the process);
 *   async rejections are caught, logged and recorded in `listenerErrors`;
 * - the factories throw synchronously like kafkajs (string/null
 *   requestTimeout, missing groupId, heartbeatInterval >= sessionTimeout
 *   compared as given, idempotent with retries < 1, bad createPartitioner);
 * - send() before connect() rejects with KafkaJSError('The producer is
 *   disconnected'); argument checks happen in kafkajs' order; message
 *   key/value/header/timestamp types are checked with kafkajs' own record
 *   encoder, so invalid types fail with the real TypeError/SyntaxError;
 *   messages for partitions that do not exist are silently dropped and
 *   acks: 0 resolves [] (both like kafkajs);
 * - disconnect() does not cancel an in-flight connect(): a delayed connect
 *   still completes afterwards and leaves a "zombie" connected client;
 * - pause()/resume()/seek() throw synchronously and commitOffsets() rejects
 *   with 'Consumer group was not initialized, consumer#run must be called
 *   first' when the consumer is not running;
 * - run() resolves after GROUP_JOIN, or after the crash handling when joining
 *   fails; a crash emits STOP, DISCONNECT, CRASH in that order, calls
 *   retry.restartOnFailure like kafkajs (a non-async one breaks the same way)
 *   and restarts the consumer when `restart` is true. Pauses are lost on a
 *   restart. Promises kafkajs drops (restart timer) are dropped here too, so
 *   a crash bug surfaces as an unhandled rejection that setup.js reports.
 *
 * Behaviour knobs (`fake.behaviour`) are read at call time. A knob may be a
 * value or a function `(args, client) => value`; for error knobs a truthy
 * value is the rejection (a knob function may also throw). Knobs:
 *   producerFactoryError, consumerFactoryError, adminFactoryError (thrown by
 *     kafka.producer()/consumer()/admin(); args = options, client = Kafka)
 *   connectError (producer), consumerConnectError, adminConnectError
 *   connectDelayMs, disconnectDelayMs, disconnectError (every client kind;
 *     use a function and `client.kind` to target one); a delay of Infinity
 *     never settles (hung broker)
 *   sendError, sendDelayMs, sendResult (args = the send()/sendBatch() argument;
 *     sendResult replaces the resolved RecordMetadata[]; messages are still
 *     recorded in fake.sent)
 *   partitionCount: number | { [topic]: number } | (topic) => number
 *     (producer: unknown partitions are dropped; default: every partition
 *     exists. Consumer assignment: partitions 0..n-1, default 1)
 *   clusterTopics: string[] (default ['test-topic']): RegExp subscriptions
 *     and admin listTopics/fetchTopicMetadata defaults
 *   subscribeError, runError, groupJoinError (CRASH after run() instead of
 *     GROUP_JOIN), commitError, restartDelayMs (default e.retryTime ||
 *     retry.initialRetryTime || 300)
 *   subscribeDelayMs (subscribe() takes this long), runDelayMs (joining the
 *     group takes this long: run() is pending and the runner exists, like
 *     kafkajs while it joins; args = the run() config)
 *   adminErrors: { [method]: Error | (args, admin) => Error|null }
 *   adminResults: { [method]: value | (args, admin) => value }
 *   adminDelayMs: number | { [method]: number | (args, admin) => number }
 */

const { EventEmitter } = require('node:events');
const realKafkajs = require('kafkajs');
const K = require('kafkajs/src/errors');
const { createErrorFromCode, errorCodes } = require('kafkajs/src/protocol/error');
const InstrumentationEvent = require('kafkajs/src/instrumentation/event');
const SubscriptionState = require('kafkajs/src/consumer/subscriptionState');
const Long = require('kafkajs/src/utils/long');
const encodeRecord = require('kafkajs/src/protocol/recordBatch/record/v0');
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('kafkajs/src/constants');
const { events: PRODUCER_EVENTS } = require('kafkajs/src/producer/instrumentationEvents');
const { events: CONSUMER_EVENTS } = require('kafkajs/src/consumer/instrumentationEvents');
const { events: ADMIN_EVENTS } = require('kafkajs/src/admin/instrumentationEvents');

const LEVELS = Object.freeze({ ...realKafkajs.logLevel });
const NOT_INITIALIZED = 'Consumer group was not initialized, consumer#run must be called first';
const SPECIAL_OFFSETS = [String(EARLIEST_OFFSET), String(LATEST_OFFSET)];
const DEFAULT_TOPICS = ['test-topic'];
const PARTITIONER_WARNING = 'KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning ' +
    'behavior as in previous versions, create the producer with the option "createPartitioner: ' +
    'Partitioners.LegacyPartitioner". See the migration guide at ' +
    'https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. ' +
    'Silence this warning by setting the environment variable "KAFKAJS_NO_PARTITIONER_WARNING=1"';

// Every error class kafkajs exports from its root module (KafkaJSError, ...).
const ERROR_CLASSES = Object.fromEntries(
    Object.entries(K).filter(([name, value]) => typeof value === 'function' && /^KafkaJS/.test(name))
);

// ---------------------------------------------------------------- utilities

/** Read a knob: a value, or a function called with (args, client). */
function readKnob(fake, name, args, client) {
    const value = fake.behaviour[name];
    return typeof value === 'function' ? value(args, client) : value;
}

/** Throw the error a knob asks for (use inside async functions => rejection). */
function failIfKnob(fake, name, args, client) {
    const err = readKnob(fake, name, args, client);
    if (err) throw err;
}

/** Simulated network delay. Infinity never settles (a hung broker). */
function wait(ms) {
    const n = Number(ms);
    if (ms === undefined || ms === null || !(n > 0)) return Promise.resolve();
    if (n === Infinity) return new Promise(() => {});
    return new Promise((resolve) => setTimeout(resolve, n));
}

function nextImmediate() {
    return new Promise((resolve) => setImmediate(resolve));
}

/** kafkajs utils/lock.js: every Lock requires a numeric timeout. */
function assertLockTimeout(timeout) {
    if (typeof timeout !== 'number') {
        throw new TypeError(`'timeout' is not a number, received '${typeof timeout}'`);
    }
}

/** Cluster constructor (called by kafka.producer()/consumer()/admin()): Lock(requestTimeout). */
function checkClusterOptions({ requestTimeout = 30000 }) {
    assertLockTimeout(requestTimeout);
}

/** Broker constructor (at connect time): Lock(2 * connectionTimeout + authenticationTimeout). */
function checkBrokerOptions({ connectionTimeout = 1000, authenticationTimeout = 10000 }) {
    assertLockTimeout(2 * connectionTimeout + authenticationTimeout);
}

/** kafkajs consumer: walk .cause to the original error. */
function originalCause(error) {
    return error.cause ? originalCause(error.cause) : error;
}

/** Normalise a value the way a consumer receives it from Kafka (Buffer or null). */
function toBytes(value) {
    if (value === null) return null;
    if (Buffer.isBuffer(value)) return value;
    if (typeof value === 'string') return Buffer.from(value);
    if (value instanceof ArrayBuffer) return Buffer.from(value);
    if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    return Buffer.from(JSON.stringify(value));
}

function toHeaderBytes(headers) {
    const out = {};
    for (const [k, v] of Object.entries(headers || {})) {
        if (v === undefined) continue;
        out[k] = Array.isArray(v) ? v.map((x) => (x == null ? null : toBytes(x))) : (v === null ? null : toBytes(v));
    }
    return out;
}

// ---------------------------------------------------------- instrumentation

/**
 * Same wiring as kafkajs' InstrumentationEventEmitter plus the client `on()`
 * wrapper (producer/index.js:82-97, consumer/index.js:309-324).
 */
class Instrumentation {
    constructor(prefix, events, logger) {
        this.prefix = prefix;
        this.events = events;
        this.names = Object.values(events);
        this.keysText = Object.keys(events).map((k) => `${prefix}.events.${k}`).join(', ');
        this.emitter = new EventEmitter();
        this.logger = logger;
        this.listenerErrors = [];
    }

    on(eventName, listener) {
        if (!this.names.includes(eventName)) {
            throw new K.KafkaJSNonRetriableError(`Event name should be one of ${this.keysText}`);
        }
        const wrapped = (event) => {
            // like kafkajs: only ASYNC failures are caught; a sync throw escapes emit()
            Promise.resolve(listener(event)).catch((e) => {
                this.listenerErrors.push(e);
                this.logger.error(`Failed to execute listener: ${e.message}`, { eventName, stack: e.stack });
            });
        };
        this.emitter.addListener(eventName, wrapped);
        return () => this.emitter.removeListener(eventName, wrapped);
    }

    emit(eventName, payload) {
        if (this.emitter.listenerCount(eventName) > 0) {
            this.emitter.emit(eventName, new InstrumentationEvent(eventName, payload));
        }
    }
}

/** Shared by producer, consumer and admin: on(), emit() helper, listenerCount(). */
class FakeClient {
    constructor(fake, kafka, kind, events, namespace, options) {
        this.fake = fake;
        this.kafka = kafka;
        this.kind = kind;
        this.events = events;
        this.options = options;
        this.state = 'disconnected';
        this.connectCount = 0;
        this.disconnectCount = 0;
        this._disposed = false;
        this._logger = kafka._createLogger(namespace);
        this._instr = new Instrumentation(kind, events, this._logger);
        /** errors of async listeners, caught and logged by kafkajs */
        this.listenerErrors = this._instr.listenerErrors;
    }

    /** kafkajs `client.on(eventName, listener)`; returns the remover function. */
    on(eventName, listener) {
        return this._instr.on(eventName, listener);
    }

    /**
     * Test helper: emit an instrumentation event to the registered listeners,
     * synchronously, exactly like kafkajs does. Returns whether anyone listened.
     */
    emit(eventName, payload) {
        if (!this._instr.names.includes(eventName)) {
            throw new Error(`fake ${this.kind}: unknown event "${eventName}"`);
        }
        const listened = this._instr.emitter.listenerCount(eventName) > 0;
        this._instr.emit(eventName, payload);
        return listened;
    }

    /** Test helper: number of listeners registered for `eventName`. */
    listenerCount(eventName) {
        return this._instr.emitter.listenerCount(eventName);
    }

    logger() {
        return this._logger;
    }

    _dispose() {
        this._disposed = true;
    }
}

// ----------------------------------------------------------------- producer

class FakeProducer extends FakeClient {
    constructor(fake, kafka, options, { partitioner, idempotent, retry }) {
        super(fake, kafka, 'producer', PRODUCER_EVENTS, 'Producer', options);
        this.index = fake.producers.length;
        this.retry = retry;
        this.sendCalls = [];
        this.inFlight = 0;
        this.maxInFlight = 0;
        this._partitioner = partitioner;
        this._idempotent = idempotent;
    }

    isIdempotent() {
        return this._idempotent;
    }

    async connect() {
        await wait(readKnob(this.fake, 'connectDelayMs', undefined, this));
        checkBrokerOptions(this.kafka.options);
        failIfKnob(this.fake, 'connectError', undefined, this);
        // Deliberately no "was disconnect() called meanwhile?" check: kafkajs
        // cannot cancel a connect either (zombie client after redeploy).
        this.state = 'connected';
        this.connectCount++;
        this._instr.emit(PRODUCER_EVENTS.CONNECT);
    }

    async disconnect() {
        this.state = 'disconnecting';
        await wait(readKnob(this.fake, 'disconnectDelayMs', undefined, this));
        failIfKnob(this.fake, 'disconnectError', undefined, this);
        this.state = 'disconnected';
        this.disconnectCount++;
        this._instr.emit(PRODUCER_EVENTS.DISCONNECT);
    }

    // parameter destructuring like kafkajs, so send() without argument fails the same way
    async send({ acks, timeout, compression, topic, messages }) {
        return this._produce('send', arguments[0], { acks, timeout, compression, topicMessages: [{ topic, messages }] });
    }

    async sendBatch({ acks, timeout, compression, topicMessages }) {
        return this._produce('sendBatch', arguments[0], { acks, timeout, compression, topicMessages });
    }

    /** kafkajs producer/messageProducer.js sendBatch + sendMessages. */
    async _produce(method, args, { acks = -1, compression, topicMessages = [] }) {
        this.sendCalls.push({ method, args });
        if (topicMessages.some(({ topic }) => !topic)) {
            throw new K.KafkaJSNonRetriableError('Invalid topic');
        }
        if (this._idempotent && acks !== -1) {
            throw new K.KafkaJSNonRetriableError(
                'Not requiring ack for all messages invalidates the idempotent producer\'s EoS guarantees');
        }
        for (const { topic, messages } of topicMessages) {
            if (!messages) {
                throw new K.KafkaJSNonRetriableError(`Invalid messages array [${messages}] for topic "${topic}"`);
            }
            const withoutValue = messages.find((message) => message.value === undefined);
            if (withoutValue) {
                throw new K.KafkaJSNonRetriableError(
                    `Invalid message without value for topic "${topic}": ${JSON.stringify(withoutValue)}`);
            }
        }
        if (this.state === 'disconnecting') {
            throw new K.KafkaJSNonRetriableError(
                'The producer is disconnecting; therefore, it can\'t safely accept messages anymore');
        }
        if (this.state === 'disconnected') {
            throw new K.KafkaJSError('The producer is disconnected');
        }

        const merged = [];
        for (const { topic, messages } of topicMessages) {
            const existing = merged.find((m) => m.topic === topic);
            if (existing) existing.messages = [...existing.messages, ...messages];
            else merged.push({ topic, messages });
        }

        this.inFlight++;
        this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
        try {
            await wait(readKnob(this.fake, 'sendDelayMs', args, this));
            validateEncoding(merged, compression);
            failIfKnob(this.fake, 'sendError', args, this);
            const results = this._write(merged, acks, compression);
            const custom = readKnob(this.fake, 'sendResult', args, this);
            if (custom !== undefined) return custom;
            return acks === 0 ? [] : results;
        } finally {
            this.inFlight--;
        }
    }

    _write(merged, acks, compression) {
        const results = [];
        for (const { topic, messages } of merged) {
            const count = this.fake._partitionCount(topic);
            const partitionMetadata = buildPartitionMetadata(Number.isFinite(count) ? count : 1);
            const byPartition = new Map();
            for (const message of messages) {
                const chosen = message.partition !== null && message.partition !== undefined
                    ? message.partition
                    : this._partitioner({ topic, partitionMetadata, message });
                const partition = parseInt(chosen, 10);
                // cluster/index.js findLeaderForPartitions: partitions without metadata are skipped silently
                if (!(Number.isInteger(partition) && partition >= 0 && partition < count)) continue;
                if (!byPartition.has(partition)) byPartition.set(partition, []);
                byPartition.get(partition).push(message);
            }
            for (const [partition, list] of byPartition) {
                const base = this.fake._appendOffsets(topic, partition, list.length);
                list.forEach((m, i) => {
                    this.fake.sent.push({
                        topic, key: m.key, value: m.value, headers: m.headers, partition,
                        timestamp: m.timestamp, acks, compression, offset: String(base + i)
                    });
                });
                results.push({
                    topicName: topic, partition, errorCode: 0, baseOffset: String(base),
                    logAppendTime: '-1', logStartOffset: '0'
                });
            }
        }
        return results;
    }
}

function buildPartitionMetadata(count) {
    const out = [];
    for (let i = 0; i < count; i++) {
        out.push({ partitionErrorCode: 0, partitionId: i, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] });
    }
    return out;
}

/**
 * The encoding step of a produce request, with kafkajs' own code: invalid
 * key/value/header types throw the real TypeError (ERR_INVALID_ARG_TYPE), an
 * invalid timestamp the real SyntaxError/RangeError, an unknown compression
 * id the real "Cannot read properties of null (reading 'compress')".
 * kafkajs bails with these errors unwrapped (sendMessages.js catch => bail).
 */
function validateEncoding(merged, compression = realKafkajs.CompressionTypes.None) {
    for (const { messages } of merged) {
        for (const message of messages) {
            encodeRecord({ ...message, offsetDelta: 0, timestampDelta: 0 });
        }
        // produce/v3/request.js: only the first and the max (string-sorted) timestamps become Longs
        const timestamps = messages.map((m) => m.timestamp).filter((t) => t != null).sort();
        if (timestamps.length > 0) {
            Long.fromValue(timestamps[0]);
            Long.fromValue(timestamps[timestamps.length - 1]);
        }
    }
    if (compression !== realKafkajs.CompressionTypes.None) {
        // recordBatch/v0: lookupCodec(type) returns null for unknown types. The fake
        // treats GZIP, Snappy, LZ4 and ZSTD as available (codecs are registered in production).
        const known = Object.values(realKafkajs.CompressionTypes).includes(compression);
        const codec = known ? { compress() {} } : null;
        codec.compress();
    }
}

// ----------------------------------------------------------------- consumer

class FakeConsumer extends FakeClient {
    constructor(fake, kafka, options, { groupId, retry }) {
        super(fake, kafka, 'consumer', CONSUMER_EVENTS, 'Consumer', options);
        this.index = fake.consumers.length;
        this.groupId = groupId;
        /** effective retry options ({...client retry, ...consumer retry}), incl. restartOnFailure */
        this.retry = retry;
        /** true from GROUP_JOIN until stop/disconnect/crash */
        this.running = false;
        this.runCount = 0;
        this.joinCount = 0;
        this.subscriptions = [];
        this.runConfig = null;
        this.pauses = [];
        this.resumes = [];
        this.seeks = [];
        this.committed = [];
        this.crashes = [];
        this.delivered = [];
        this.eachMessageErrors = [];
        this._topics = {}; // resolved topic name => { fromBeginning } (kafkajs `topics`)
        this._group = null; // kafkajs `consumerGroup`: set from run()/restart until stop()
        this._generation = 0;
        this._restartTimer = null;
        this._nextOffsets = new Map();
    }

    async connect() {
        await wait(readKnob(this.fake, 'connectDelayMs', undefined, this));
        checkBrokerOptions(this.kafka.options);
        failIfKnob(this.fake, 'consumerConnectError', undefined, this);
        this.state = 'connected';
        this.connectCount++;
        this._instr.emit(CONSUMER_EVENTS.CONNECT);
    }

    async disconnect() {
        try {
            await this.stop();
            await wait(readKnob(this.fake, 'disconnectDelayMs', undefined, this));
            failIfKnob(this.fake, 'disconnectError', undefined, this);
            this.state = 'disconnected';
            this.disconnectCount++;
            this._instr.emit(CONSUMER_EVENTS.DISCONNECT);
        } catch (e) {
            this._logger.error(`Caught error when disconnecting the consumer: ${e.message}`,
                { stack: e.stack, groupId: this.groupId });
            throw e;
        }
    }

    async stop() {
        if (this._group) {
            this._group = null;
            this.running = false;
            this._instr.emit(CONSUMER_EVENTS.STOP);
        }
        clearTimeout(this._restartTimer);
        this._restartTimer = null;
    }

    async subscribe({ topic, topics: subscriptionTopics, fromBeginning = false }) {
        if (this._group) {
            throw new K.KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is running');
        }
        if (!topic && !subscriptionTopics) {
            throw new K.KafkaJSNonRetriableError('Missing required argument "topics"');
        }
        if (subscriptionTopics != null && !Array.isArray(subscriptionTopics)) {
            throw new K.KafkaJSNonRetriableError('Argument "topics" must be an array');
        }
        const subscriptions = subscriptionTopics || [topic];
        for (const subscription of subscriptions) {
            if (typeof subscription !== 'string' && !(subscription instanceof RegExp)) {
                throw new K.KafkaJSNonRetriableError(
                    `Invalid topic ${subscription} (${typeof subscription}), the topic name has to be a String or a RegExp`);
            }
        }
        await wait(readKnob(this.fake, 'subscribeDelayMs', { topic, topics: subscriptionTopics, fromBeginning }, this));
        failIfKnob(this.fake, 'subscribeError', { topic, topics: subscriptionTopics, fromBeginning }, this);
        // a RegExp is resolved once, against the topics that exist now (consumer/index.js:157-178)
        const clusterTopics = this.fake._clusterTopics();
        const resolved = [];
        for (const subscription of subscriptions) {
            if (subscription instanceof RegExp) resolved.push(...clusterTopics.filter((t) => subscription.test(t)));
            else resolved.push(subscription);
        }
        for (const t of resolved) this._topics[t] = { fromBeginning };
        this.subscriptions.push({ topics: subscriptions.slice(), fromBeginning, resolved });
    }

    async run(config = {}) {
        const { eachMessage = null } = config;
        if (this._group) {
            this._logger.warn('consumer#run was called, but the consumer is already running', { groupId: this.groupId });
            return;
        }
        failIfKnob(this.fake, 'runError', config, this);
        this.runConfig = config;
        this._eachMessage = eachMessage;
        this.runCount++;
        await this._start();
    }

    /** kafkajs run() -> start(onCrash) -> runner.start(): connect + join, or crash. */
    async _start() {
        if (this._disposed) return undefined;
        const generationId = ++this._generation;
        const group = {
            generationId,
            memberId: `${this.kafka.options.clientId || 'kafkajs'}-fake-member-${this.index}-${generationId}`,
            subscriptionState: new SubscriptionState()
        };
        this._group = group;
        const started = Date.now();
        await nextImmediate();
        await wait(readKnob(this.fake, 'runDelayMs', this.runConfig, this)); // a slow group join
        if (this._group !== group || this._disposed) return undefined; // stopped while joining
        try {
            this.state = 'connected';
            this._instr.emit(CONSUMER_EVENTS.CONNECT); // consumerGroup.connect(): the 2nd CONNECT
            failIfKnob(this.fake, 'groupJoinError', this.runConfig, this);
            const assignment = Object.keys(this._topics).map((topic) => {
                const count = this.fake._partitionCount(topic);
                const n = Number.isFinite(count) ? count : 1;
                return { topic, partitions: Array.from({ length: n }, (_, i) => i) };
            });
            group.subscriptionState.assign(assignment);
            const memberAssignment = assignment.reduce((acc, { topic, partitions }) => ({ ...acc, [topic]: partitions }), {});
            this.joinCount++;
            this._instr.emit(CONSUMER_EVENTS.GROUP_JOIN, {
                groupId: this.groupId,
                memberId: group.memberId,
                leaderId: group.memberId,
                isLeader: true,
                memberAssignment,
                groupProtocol: 'RoundRobinAssigner',
                duration: Date.now() - started
            });
        } catch (e) {
            return this._onCrash(e);
        }
        if (this._group === group) this.running = true;
        return undefined;
    }

    /**
     * kafkajs consumer onCrash (consumer/index.js:246-302): disconnect (STOP,
     * DISCONNECT), decide `restart` (retriable && restartOnFailure), emit CRASH,
     * schedule the restart. A boolean `forcedRestart` skips the decision.
     */
    async _onCrash(e, forcedRestart) {
        this._logger.error(`Crash: ${e.name}: ${e.message}`, { groupId: this.groupId, retryCount: e.retryCount, stack: e.stack });
        await this.disconnect();
        let restart;
        if (typeof forcedRestart === 'boolean') {
            restart = forcedRestart;
        } else {
            const retry = this.retry;
            const retriable = e.name === 'KafkaJSNumberOfRetriesExceeded' || originalCause(e).retriable === true;
            restart = retriable &&
                (!retry || !retry.restartOnFailure ||
                    (await retry.restartOnFailure(e).catch((error) => {
                        this._logger.error('Caught error when invoking user-provided "restartOnFailure" callback. Defaulting to restarting.',
                            { error: error.message || error, cause: e.message || e, groupId: this.groupId });
                        return true;
                    })));
        }
        const payload = { error: e, groupId: this.groupId, restart };
        this.crashes.push(payload);
        this._instr.emit(CONSUMER_EVENTS.CRASH, payload);
        if (restart && !this._disposed) {
            const knob = readKnob(this.fake, 'restartDelayMs', e, this);
            const retryTime = knob !== undefined ? knob : (e.retryTime || (this.retry && this.retry.initialRetryTime) || 300);
            this._logger.error(`Restarting the consumer in ${retryTime}ms`, { retryCount: e.retryCount, retryTime, groupId: this.groupId });
            // like kafkajs, the promise of the restart is not handled by anyone
            this._restartTimer = setTimeout(() => {
                this._restartTimer = null;
                this._start();
            }, retryTime);
        }
    }

    /**
     * Test helper: make the running consumer crash like kafkajs does at runtime
     * (fetch error, broker outage...). `restart` undefined => decided like
     * kafkajs (retriable error and restartOnFailure), boolean => forced.
     * Returns the crash-handling promise (rejects if a listener or
     * restartOnFailure breaks; unawaited, that is an unhandled rejection, as in kafkajs).
     */
    crash(error, restart) {
        return this._onCrash(error, restart);
    }

    pause(topicPartitions = []) {
        validateTopicPartitions(topicPartitions, 'pause');
        if (!this._group) throw new K.KafkaJSNonRetriableError(NOT_INITIALIZED);
        this._group.subscriptionState.pause(topicPartitions);
        this.pauses.push(topicPartitions);
    }

    resume(topicPartitions = []) {
        validateTopicPartitions(topicPartitions, 'resume');
        if (!this._group) throw new K.KafkaJSNonRetriableError(NOT_INITIALIZED);
        this._group.subscriptionState.resume(topicPartitions);
        this.resumes.push(topicPartitions);
    }

    paused() {
        if (!this._group) return [];
        return this._group.subscriptionState.paused();
    }

    seek({ topic, partition, offset }) {
        if (!topic) throw new K.KafkaJSNonRetriableError(`Invalid topic ${topic}`);
        if (isNaN(partition)) {
            throw new K.KafkaJSNonRetriableError(`Invalid partition, expected a number received ${partition}`);
        }
        let seekOffset;
        try {
            seekOffset = Long.fromValue(offset);
        } catch (_) {
            throw new K.KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`);
        }
        if (seekOffset.lessThan(0) && !SPECIAL_OFFSETS.includes(seekOffset.toString())) {
            throw new K.KafkaJSNonRetriableError('Offset must not be a negative number');
        }
        if (!this._group) throw new K.KafkaJSNonRetriableError(NOT_INITIALIZED);
        this.seeks.push({ topic, partition, offset: seekOffset.toString() });
    }

    async commitOffsets(topicPartitions = []) {
        const entries = topicPartitions.map(({ topic, partition, offset, metadata = null }) => {
            if (!topic) throw new K.KafkaJSNonRetriableError(`Invalid topic ${topic}`);
            if (isNaN(partition)) {
                throw new K.KafkaJSNonRetriableError(`Invalid partition, expected a number received ${partition}`);
            }
            let commitOffset;
            try {
                commitOffset = Long.fromValue(offset);
            } catch (_) {
                throw new K.KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`);
            }
            if (commitOffset.lessThan(0)) throw new K.KafkaJSNonRetriableError('Offset must not be a negative number');
            if (metadata !== null && typeof metadata !== 'string') {
                throw new K.KafkaJSNonRetriableError(`Invalid offset metadata, expected string or null, received ${metadata}`);
            }
            return { topic, partition, offset: commitOffset.toString(), metadata };
        });
        if (!this._group) throw new K.KafkaJSNonRetriableError(NOT_INITIALIZED);
        const group = this._group;
        failIfKnob(this.fake, 'commitError', topicPartitions, this);
        this.committed.push(...entries);
        const byTopic = new Map();
        for (const { topic, partition, offset } of entries) {
            if (!byTopic.has(topic)) byTopic.set(topic, []);
            byTopic.get(topic).push({ partition, offset });
        }
        this._instr.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, {
            groupId: this.groupId,
            memberId: group.memberId,
            groupGenerationId: group.generationId,
            topics: Array.from(byTopic, ([topic, partitions]) => ({ topic, partitions }))
        });
    }

    async describeGroup() {
        return {
            groupId: this.groupId, members: [], protocol: 'RoundRobinAssigner', protocolType: 'consumer',
            state: this.running ? 'Stable' : 'Empty', errorCode: 0
        };
    }

    /**
     * Test helper: hand one message to the running consumer's eachMessage and
     * wait for it to finish. Resolves true, or false when the topic-partition
     * is paused (kafkajs would not fetch it). Rejects when the consumer is not
     * running/joined, the topic is not subscribed, or eachMessage throws (in
     * kafkajs that would crash or endlessly retry the consumer).
     * Message defaults: key null, value empty Buffer, headers {}, offset
     * auto-incremented per partition, timestamp String(Date.now()); strings
     * are converted to Buffers like real fetched records.
     * Like kafkajs' batch processing, the message is wrapped in
     * START_BATCH_PROCESS / END_BATCH_PROCESS events of a one-message batch;
     * `highWatermark` defaults to offset + 1 (the message is the newest one).
     */
    async deliver({ topic, partition = 0, message = {}, highWatermark } = {}) {
        if (!this._group || !this.running) {
            throw new Error(`fake consumer #${this.index}: deliver() needs a running consumer (run() called and GROUP_JOIN emitted)`);
        }
        if (typeof this._eachMessage !== 'function') {
            throw new Error(`fake consumer #${this.index}: run() was not given an eachMessage handler`);
        }
        if (!Object.prototype.hasOwnProperty.call(this._topics, topic)) {
            const subscribed = Object.keys(this._topics).join(', ') || 'none';
            throw new Error(`fake consumer #${this.index}: topic "${topic}" is not subscribed (subscribed: ${subscribed})`);
        }
        if (this._group.subscriptionState.isPaused(topic, partition)) return false;
        const kafkaMessage = this._buildMessage(topic, partition, message);
        const hw = highWatermark !== undefined && highWatermark !== null
            ? String(highWatermark)
            : (BigInt(kafkaMessage.offset) + 1n).toString();
        // consumer/runner.js: payload of START_BATCH_PROCESS / END_BATCH_PROCESS
        const batchInfo = {
            topic,
            partition,
            highWatermark: hw,
            offsetLag: (BigInt(hw) - 1n - BigInt(kafkaMessage.offset)).toString(),
            offsetLagLow: (BigInt(hw) - 1n - BigInt(kafkaMessage.offset)).toString(),
            batchSize: 1,
            firstOffset: kafkaMessage.offset,
            lastOffset: kafkaMessage.offset
        };
        this._instr.emit(CONSUMER_EVENTS.START_BATCH_PROCESS, batchInfo);
        try {
            await this._eachMessage({
                topic,
                partition,
                message: kafkaMessage,
                heartbeat: async () => {},
                pause: () => {
                    this.pause([{ topic, partitions: [partition] }]);
                    return () => this.resume([{ topic, partitions: [partition] }]);
                }
            });
        } catch (e) {
            this.eachMessageErrors.push(e);
            throw e;
        }
        this._instr.emit(CONSUMER_EVENTS.END_BATCH_PROCESS, { ...batchInfo, duration: 0 });
        this.delivered.push({ topic, partition, offset: kafkaMessage.offset });
        return true;
    }

    _buildMessage(topic, partition, m) {
        const key = `${topic}\u0000${partition}`;
        let offset;
        if (m.offset !== undefined && m.offset !== null) {
            offset = String(m.offset);
            this._nextOffsets.set(key, BigInt(offset) + 1n);
        } else {
            const next = this._nextOffsets.get(key) || 0n;
            offset = String(next);
            this._nextOffsets.set(key, next + 1n);
        }
        const timestamp = m.timestamp !== undefined && m.timestamp !== null ? String(m.timestamp) : String(Date.now());
        return {
            magicByte: 2,
            attributes: 0,
            timestamp,
            offset,
            key: m.key === undefined ? null : toBytes(m.key),
            value: m.value === undefined ? Buffer.alloc(0) : toBytes(m.value),
            headers: toHeaderBytes(m.headers),
            isControlRecord: false,
            batchContext: {
                firstOffset: offset, firstTimestamp: timestamp, partitionLeaderEpoch: 0, inTransaction: false,
                isControlBatch: false, lastOffsetDelta: 0, producerId: '-1', producerEpoch: 0,
                firstSequence: 0, maxTimestamp: timestamp, timestampType: 0, magicByte: 2
            }
        };
    }

    _dispose() {
        super._dispose();
        clearTimeout(this._restartTimer);
        this._restartTimer = null;
    }
}

/** kafkajs consumer pause()/resume() argument checks. */
function validateTopicPartitions(topicPartitions, verb) {
    for (const topicPartition of topicPartitions) {
        if (!topicPartition || !topicPartition.topic) {
            throw new K.KafkaJSNonRetriableError(`Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`);
        } else if (typeof topicPartition.partitions !== 'undefined' &&
            (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some(isNaN))) {
            throw new K.KafkaJSNonRetriableError(
                `Array of valid partitions required to ${verb} specific partitions instead of ${topicPartition.partitions}`);
        }
    }
}

// -------------------------------------------------------------------- admin

const CONFIG_RESOURCE_TYPES = Object.values(realKafkajs.ConfigResourceTypes);

function nonRetriable(message) {
    return new K.KafkaJSNonRetriableError(message);
}

/** kafkajs admin argument checks (admin/index.js), each throws inside the async method. */
const ADMIN_VALIDATORS = {
    createTopics({ topics }) {
        if (!topics || !Array.isArray(topics)) throw nonRetriable(`Invalid topics array ${topics}`);
        if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
            throw nonRetriable('Invalid topics array, the topic names have to be a valid string');
        }
        if (new Set(topics.map(({ topic }) => topic)).size < topics.length) {
            throw nonRetriable('Invalid topics array, it cannot have multiple entries for the same topic');
        }
        for (const { topic, configEntries } of topics) {
            if (configEntries != null && !Array.isArray(configEntries)) {
                throw nonRetriable(`Invalid configEntries for topic "${topic}", must be an array`);
            }
        }
    },
    deleteTopics({ topics }) {
        if (!topics || !Array.isArray(topics)) throw nonRetriable(`Invalid topics array ${topics}`);
        if (topics.filter((topic) => typeof topic !== 'string').length > 0) {
            throw nonRetriable('Invalid topics array, the names must be a valid string');
        }
    },
    createPartitions({ topicPartitions }) {
        if (!topicPartitions || !Array.isArray(topicPartitions)) {
            throw nonRetriable(`Invalid topic partitions array ${topicPartitions}`);
        }
        if (topicPartitions.length === 0) throw nonRetriable('Empty topic partitions array');
        if (topicPartitions.filter(({ topic }) => typeof topic !== 'string').length > 0) {
            throw nonRetriable('Invalid topic partitions array, the topic names have to be a valid string');
        }
    },
    fetchTopicMetadata({ topics = [] } = {}) {
        if (topics) {
            topics.forEach((topic) => {
                if (!topic || typeof topic !== 'string') throw nonRetriable(`Invalid topic ${topic}`);
            });
        }
    },
    fetchTopicOffsets(topic) {
        if (!topic || typeof topic !== 'string') throw nonRetriable(`Invalid topic ${topic}`);
    },
    fetchTopicOffsetsByTimestamp({ topic }) {
        if (!topic || typeof topic !== 'string') throw nonRetriable(`Invalid topic ${topic}`);
    },
    fetchOffsets({ groupId, topics }) {
        if (!groupId) throw nonRetriable(`Invalid groupId ${groupId}`);
        if (topics && !Array.isArray(topics)) throw nonRetriable('Expected topics array to be set');
    },
    resetOffsets({ groupId, topic }) {
        if (!groupId) throw nonRetriable(`Invalid groupId ${groupId}`);
        if (!topic) throw nonRetriable(`Invalid topic ${topic}`);
    },
    setOffsets({ groupId, topic, partitions }) {
        if (!groupId) throw nonRetriable(`Invalid groupId ${groupId}`);
        if (!topic) throw nonRetriable(`Invalid topic ${topic}`);
        if (!partitions || partitions.length === 0) throw nonRetriable('Invalid partitions');
    },
    describeGroups(groupIds) {
        groupIds.map((g) => g); // kafkajs calls groupIds.map(): a non-array fails with its TypeError
    },
    deleteGroups(groupIds) {
        if (!groupIds || !Array.isArray(groupIds)) throw nonRetriable(`Invalid groupIds array ${groupIds}`);
        const invalidGroupId = groupIds.some((g) => typeof g !== 'string');
        if (invalidGroupId) throw nonRetriable(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`);
    },
    describeConfigs: validateConfigResources,
    alterConfigs: validateConfigResources
};

function validateConfigResources({ resources }) {
    if (!resources || !Array.isArray(resources)) throw nonRetriable(`Invalid resources array ${resources}`);
    if (resources.length === 0) throw nonRetriable('Resources array cannot be empty');
    const invalidType = resources.find((r) => !CONFIG_RESOURCE_TYPES.includes(r.type));
    if (invalidType) throw nonRetriable(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    const invalidName = resources.find((r) => !r.name || typeof r.name !== 'string');
    if (invalidName) throw nonRetriable(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
}

/** Canned results, shaped like kafkajs 2.2.4 admin results. `fake` gives access to clusterTopics. */
const ADMIN_DEFAULT_RESULTS = {
    listTopics: (args, fake) => fake._clusterTopics().slice(),
    createTopics: () => true,
    deleteTopics: () => undefined,
    createPartitions: () => undefined,
    fetchTopicMetadata: ({ topics = [] } = {}, fake) => ({
        topics: (topics && topics.length ? topics : fake._clusterTopics()).map((name) => ({
            name, partitions: buildPartitionMetadata(1)
        }))
    }),
    fetchTopicOffsets: () => [{ partition: 0, offset: '0', high: '0', low: '0' }],
    fetchTopicOffsetsByTimestamp: () => [{ partition: 0, offset: '0' }],
    fetchOffsets: ({ topics } = {}, fake) => (topics && topics.length ? topics : fake._clusterTopics()).map((topic) => ({
        topic, partitions: [{ partition: 0, offset: '-1', metadata: null }]
    })),
    resetOffsets: () => undefined,
    setOffsets: () => undefined,
    listGroups: () => ({ groups: [{ groupId: 'test-group', protocolType: 'consumer' }] }),
    describeGroups: (groupIds) => ({
        groups: groupIds.map((groupId) => ({
            errorCode: 0, groupId, state: 'Stable', protocolType: 'consumer', protocol: 'RoundRobinAssigner', members: []
        }))
    }),
    deleteGroups: (groupIds) => groupIds.map((groupId) => ({ groupId, errorCode: 0 })),
    describeCluster: () => ({ brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }], controller: 1, clusterId: 'fake-cluster-id' }),
    describeConfigs: ({ resources }) => ({
        resources: resources.map((r) => ({
            errorCode: 0, errorMessage: null, resourceType: r.type, resourceName: r.name, configEntries: []
        })),
        throttleTime: 0
    }),
    alterConfigs: ({ resources }) => ({
        resources: resources.map((r) => ({ errorCode: 0, errorMessage: null, resourceType: r.type, resourceName: r.name })),
        throttleTime: 0
    })
};

/** The admin methods the fake implements (all of SPEC §17, plus alterConfigs). */
const ADMIN_METHODS = Object.keys(ADMIN_DEFAULT_RESULTS);

class FakeAdmin extends FakeClient {
    constructor(fake, kafka, options) {
        super(fake, kafka, 'admin', ADMIN_EVENTS, 'Admin', options);
        this.index = fake.admins.length;
        /** every admin method call, in order: { method, args } */
        this.calls = [];
    }

    async connect() {
        await wait(readKnob(this.fake, 'connectDelayMs', undefined, this));
        checkBrokerOptions(this.kafka.options);
        failIfKnob(this.fake, 'adminConnectError', undefined, this);
        this.state = 'connected';
        this.connectCount++;
        this._instr.emit(ADMIN_EVENTS.CONNECT);
    }

    async disconnect() {
        await wait(readKnob(this.fake, 'disconnectDelayMs', undefined, this));
        failIfKnob(this.fake, 'disconnectError', undefined, this);
        this.state = 'disconnected';
        this.disconnectCount++;
        this._instr.emit(ADMIN_EVENTS.DISCONNECT);
    }

    /** Pick a per-method knob value from `{ [method]: value | fn }`. */
    _methodKnob(name, method, args) {
        const table = this.fake.behaviour[name];
        if (table === undefined || table === null) return undefined;
        if (typeof table !== 'object') return table; // e.g. adminDelayMs: 100 for every method
        if (!Object.prototype.hasOwnProperty.call(table, method)) return undefined;
        const value = table[method];
        return typeof value === 'function' ? value(args, this) : value;
    }

    async _call(method, args) {
        this.calls.push({ method, args });
        const validate = ADMIN_VALIDATORS[method];
        if (validate) validate(args);
        await wait(this._methodKnob('adminDelayMs', method, args));
        const err = this._methodKnob('adminErrors', method, args);
        if (err) throw err;
        const results = this.fake.behaviour.adminResults;
        if (results && typeof results === 'object' && Object.prototype.hasOwnProperty.call(results, method)) {
            return this._methodKnob('adminResults', method, args);
        }
        return ADMIN_DEFAULT_RESULTS[method](args, this.fake);
    }

    listTopics() { return this._call('listTopics', undefined); }
    createTopics(args) { return this._call('createTopics', args); }
    deleteTopics(args) { return this._call('deleteTopics', args); }
    createPartitions(args) { return this._call('createPartitions', args); }
    fetchTopicMetadata(args) { return this._call('fetchTopicMetadata', args); }
    fetchTopicOffsets(topic) { return this._call('fetchTopicOffsets', topic); }
    /** args for knobs and `calls` are `{ topic, timestamp }` */
    fetchTopicOffsetsByTimestamp(topic, timestamp) { return this._call('fetchTopicOffsetsByTimestamp', { topic, timestamp }); }
    fetchOffsets(args) { return this._call('fetchOffsets', args); }
    resetOffsets(args) { return this._call('resetOffsets', args); }
    setOffsets(args) { return this._call('setOffsets', args); }
    listGroups() { return this._call('listGroups', undefined); }
    describeGroups(groupIds) { return this._call('describeGroups', groupIds); }
    deleteGroups(groupIds) { return this._call('deleteGroups', groupIds); }
    describeCluster() { return this._call('describeCluster', undefined); }
    describeConfigs(args) { return this._call('describeConfigs', args); }
    alterConfigs(args) { return this._call('alterConfigs', args); }
}

// ------------------------------------------------------------------- errors

function protocolError(typeOrCode) {
    const entry = typeof typeOrCode === 'number'
        ? errorCodes.find((e) => e.code === typeOrCode)
        : errorCodes.find((e) => e.type === typeOrCode);
    if (!entry) throw new Error(`fake-kafkajs: unknown Kafka protocol error ${typeOrCode}`);
    return createErrorFromCode(entry.code);
}

function retriesExceeded(cause, { retryCount = 5, retryTime = 4800 } = {}) {
    return new K.KafkaJSNumberOfRetriesExceeded(cause, { retryCount, retryTime });
}

/** What kafkajs' retrier does with a non-retriable error (retry/index.js:55). */
function nonRetriableWrap(cause) {
    return new K.KafkaJSNonRetriableError(cause, { cause: (cause && cause.cause) || cause });
}

function connectionError({ broker = '127.0.0.1:9092', code = 'ECONNREFUSED', message } = {}) {
    const text = message || `Connection error: connect ${code} ${broker}`;
    return new K.KafkaJSConnectionError(text, { broker, code });
}

/**
 * Factories for the error shapes observed from kafkajs 2.2.4
 * (5-kafkajs-internals.md §1.2-1.3). Every call returns a new instance of
 * the real kafkajs error classes.
 */
function createErrorFactories() {
    return {
        protocol: protocolError,
        retriesExceeded,
        nonRetriableWrap,
        connectionError,
        /** connect() with the broker down: NumberOfRetriesExceeded <- ConnectionError ECONNREFUSED */
        connectionRefused(broker = '127.0.0.1:9092') {
            return retriesExceeded(connectionError({ broker, code: 'ECONNREFUSED' }));
        },
        /** connect() to a host name that does not resolve */
        dnsFailure(host = 'nonexistent.invalid', port = 9092) {
            return retriesExceeded(connectionError({
                broker: `${host}:${port}`, code: 'ENOTFOUND', message: `Connection error: getaddrinfo ENOTFOUND ${host}`
            }));
        },
        /** blackhole address: kafkajs' own connection timer, no code */
        connectionTimeout(broker = '10.255.255.1:9092') {
            return retriesExceeded(new K.KafkaJSConnectionError('Connection timeout', { broker }));
        },
        /** e.g. send() when the listener needs SASL/SSL the client does not use */
        connectionClosed(host = '127.0.0.1', port = 9092) {
            return retriesExceeded(new K.KafkaJSConnectionClosedError('Closed connection', { host, port }));
        },
        /** TLS: self-signed broker certificate (retriable for kafkajs, code on the cause) */
        tlsSelfSigned(broker = '127.0.0.1:9093') {
            return retriesExceeded(connectionError({
                broker, code: 'DEPTH_ZERO_SELF_SIGNED_CERT', message: 'Connection error: self-signed certificate'
            }));
        },
        /** send() during an outage after a successful connect */
        sendDuringOutage(broker = '127.0.0.1:9092') {
            return nonRetriableWrap(retriesExceeded(connectionError({ broker, code: 'ECONNREFUSED' })));
        },
        /** wrong SASL password: raw, non-retriable */
        saslAuth(mechanism = 'PLAIN') {
            return new K.KafkaJSSASLAuthenticationError(
                `SASL ${mechanism} authentication failed: Authentication failed: Invalid username or password`);
        },
        /** SASL configured without username/password */
        saslMissingCredentials() {
            return new K.KafkaJSSASLAuthenticationError('SASL Plain: Invalid username or password');
        },
        producerDisconnected() {
            return new K.KafkaJSError('The producer is disconnected');
        },
        producerDisconnecting() {
            return new K.KafkaJSNonRetriableError(
                'The producer is disconnecting; therefore, it can\'t safely accept messages anymore');
        },
        /** send(): raw ProtocolError MESSAGE_TOO_LARGE (code 10, non-retriable) */
        messageTooLarge() {
            return protocolError('MESSAGE_TOO_LARGE');
        },
        /** send() with allowAutoTopicCreation:false: NumberOfRetriesExceeded <- UNKNOWN_TOPIC_OR_PARTITION */
        unknownTopic() {
            return retriesExceeded(protocolError('UNKNOWN_TOPIC_OR_PARTITION'));
        },
        /** send() without topic ACL: NonRetriable <- TOPIC_AUTHORIZATION_FAILED */
        topicAuthorizationFailed() {
            return nonRetriableWrap(protocolError('TOPIC_AUTHORIZATION_FAILED'));
        },
        requestTimeout(broker = '127.0.0.1:9092') {
            const now = Date.now();
            return new K.KafkaJSRequestTimeoutError('Request Produce(key: 0, version: 7) timed out', {
                broker, correlationId: 1, createdAt: now - 30000, sentAt: now - 30000, pendingDuration: 0
            });
        },
        /** consumer without group ACL (GROUP_AUTHORIZATION_FAILED is hidden by kafkajs) */
        groupCoordinatorNotFound(groupId = 'test-group') {
            return new K.KafkaJSGroupCoordinatorNotFound(`Failed to find group coordinator for group ${groupId}`);
        },
        /** consumer receiving a codec kafkajs does not have */
        notImplemented(codec = 'Snappy') {
            return new K.KafkaJSNotImplemented(`${codec} compression not implemented`);
        },
        /** the real TypeError kafkajs' record encoder throws for a value of the wrong type (kafkajs frames in its stack) */
        invalidArgType(value = 42) {
            try {
                encodeRecord({ key: null, value, headers: {} });
            } catch (e) {
                return e;
            }
            throw new Error(`fake-kafkajs: ${typeof value} is a valid message value`);
        }
    };
}

/**
 * A plain Error with a custom name and extra properties.
 *
 * @param {string} name
 * @param {string} message
 * @param {object} [extra] e.g. { retriable, code, type, cause }
 * @returns {Error}
 */
function makeError(name, message, extra = {}) {
    const err = new Error(message);
    err.name = name;
    return Object.assign(err, extra);
}

// --------------------------------------------------------------------- fake

/**
 * Create a fresh fake kafkajs module (independent state per call).
 *
 * @returns {object} fake, see the file header
 */
function createFakeKafkajs() {
    const fake = {
        ...ERROR_CLASSES,
        logLevel: { ...LEVELS },
        CompressionTypes: { ...realKafkajs.CompressionTypes },
        CompressionCodecs: {},
        Partitioners: { DefaultPartitioner: () => () => 0, LegacyPartitioner: () => () => 0 },
        PartitionAssigners: realKafkajs.PartitionAssigners,
        ConfigResourceTypes: realKafkajs.ConfigResourceTypes,
        ConfigSource: realKafkajs.ConfigSource,
        AclResourceTypes: realKafkajs.AclResourceTypes,
        AclOperationTypes: realKafkajs.AclOperationTypes,
        AclPermissionTypes: realKafkajs.AclPermissionTypes,
        ResourcePatternTypes: realKafkajs.ResourcePatternTypes,
        /** every Kafka instance created, in order (see its `.options` and `.logCreator`) */
        instances: [],
        producers: [],
        consumers: [],
        admins: [],
        /** every message accepted by a fake producer, in order */
        sent: [],
        behaviour: {},
        errors: createErrorFactories(),
        makeError,
        _offsets: new Map()
    };

    fake._partitionCount = (topic) => {
        const v = fake.behaviour.partitionCount;
        let n;
        if (typeof v === 'function') n = v(topic);
        else if (v !== null && typeof v === 'object') n = v[topic];
        else n = v;
        return n === undefined || n === null ? Infinity : Number(n);
    };

    fake._clusterTopics = () => {
        const v = readKnob(fake, 'clusterTopics', undefined, null);
        return Array.isArray(v) ? v : DEFAULT_TOPICS;
    };

    fake._appendOffsets = (topic, partition, n) => {
        const key = `${topic}\u0000${partition}`;
        const base = fake._offsets.get(key) || 0;
        fake._offsets.set(key, base + n);
        return base;
    };

    fake.Kafka = class Kafka {
        /**
         * @param {object} options kafkajs client options (recorded as `this.options`)
         */
        constructor(options) {
            // same parameter destructuring as kafkajs: new Kafka() throws its TypeError
            const { logLevel = LEVELS.INFO, logCreator } = destructureClientOptions(options);
            // option values are also copied onto the instance for convenience (instance.brokers...)
            Object.assign(this, options);
            this.options = options;
            this.logCreator = logCreator;
            const creator = logCreator === undefined ? () => () => {} : logCreator;
            if (typeof creator !== 'function') throw new TypeError('logCreator is not a function');
            this._level = logLevel;
            /** the function returned by logCreator(logLevel), called once like kafkajs */
            this.logFn = creator(logLevel);
            this.producers = [];
            this.consumers = [];
            this.admins = [];
            this._warnedPartitioner = false;
            fake.instances.push(this);
        }

        _createLogger(namespace) {
            const make = (label, level) => (message, extra = {}) => {
                if (level > this._level) return; // a string logLevel lets everything through, as in kafkajs
                this.logFn({
                    namespace,
                    level,
                    label,
                    log: Object.assign({ timestamp: new Date().toISOString(), logger: 'kafkajs', message }, extra)
                });
            };
            return {
                info: make('INFO', LEVELS.INFO),
                error: make('ERROR', LEVELS.ERROR),
                warn: make('WARN', LEVELS.WARN),
                debug: make('DEBUG', LEVELS.DEBUG),
                namespace: (ns) => this._createLogger(ns),
                setLogLevel: (level) => {
                    this._level = level;
                }
            };
        }

        /** kafkajs `kafka.logger()`: log through the configured logCreator. */
        logger() {
            return this._createLogger(undefined);
        }

        producer(options) {
            const { createPartitioner, retry, idempotent = false } = options === undefined ? {} : options;
            failIfKnob(fake, 'producerFactoryError', options, this);
            checkClusterOptions(this.options);
            if (createPartitioner == null && process.env.KAFKAJS_NO_PARTITIONER_WARNING == null && !this._warnedPartitioner) {
                this._warnedPartitioner = true;
                this.logger().warn(PARTITIONER_WARNING);
            }
            const effectiveRetry = { ...this.options.retry, ...retry };
            if (idempotent && effectiveRetry.retries < 1) {
                throw new K.KafkaJSNonRetriableError('Idempotent producer must allow retries to protect against transient errors');
            }
            if (idempotent && effectiveRetry.retries < Number.MAX_SAFE_INTEGER) {
                this._createLogger('Producer').warn('Limiting retries for the idempotent producer may invalidate EoS guarantees');
            }
            const factory = createPartitioner === undefined ? fake.Partitioners.DefaultPartitioner : createPartitioner;
            if (typeof factory !== 'function') throw new TypeError('createPartitioner is not a function');
            const partitioner = factory();
            const producer = new FakeProducer(fake, this, options, { partitioner, idempotent: !!idempotent, retry: effectiveRetry });
            this.producers.push(producer);
            fake.producers.push(producer);
            return producer;
        }

        consumer(options) {
            const {
                groupId, retry = { retries: 5 }, sessionTimeout = 30000, heartbeatInterval = 3000
            } = options === undefined ? {} : options;
            failIfKnob(fake, 'consumerFactoryError', options, this);
            checkClusterOptions(this.options);
            if (!groupId) throw new K.KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.');
            // compared as given: '5000' >= '30000' is true for strings (the upstream string-config bug)
            if (heartbeatInterval >= sessionTimeout) {
                throw new K.KafkaJSNonRetriableError(
                    `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (${sessionTimeout}). ` +
                    'It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.');
            }
            const consumer = new FakeConsumer(fake, this, options, { groupId, retry: { ...this.options.retry, ...retry } });
            this.consumers.push(consumer);
            fake.consumers.push(consumer);
            return consumer;
        }

        admin(options) {
            failIfKnob(fake, 'adminFactoryError', options, this);
            checkClusterOptions(this.options);
            const admin = new FakeAdmin(fake, this, options);
            this.admins.push(admin);
            fake.admins.push(admin);
            return admin;
        }
    };

    /**
     * Forget every client, message and knob (arrays and the behaviour object
     * keep their identity). Pending consumer restarts are cancelled and old
     * clients stop emitting asynchronous events.
     */
    fake.reset = () => {
        for (const client of [...fake.producers, ...fake.consumers, ...fake.admins]) client._dispose();
        fake.instances.length = 0;
        fake.producers.length = 0;
        fake.consumers.length = 0;
        fake.admins.length = 0;
        fake.sent.length = 0;
        fake._offsets.clear();
        for (const key of Object.keys(fake.behaviour)) delete fake.behaviour[key];
        for (const key of Object.keys(fake.CompressionCodecs)) delete fake.CompressionCodecs[key];
    };

    return fake;
}

/** kafkajs Client constructor parameter destructuring (same TypeError for undefined/null). */
function destructureClientOptions({ brokers, logLevel, logCreator }) {
    return { brokers, logLevel, logCreator };
}

module.exports = {
    createFakeKafkajs,
    PRODUCER_EVENTS,
    CONSUMER_EVENTS,
    ADMIN_EVENTS,
    ADMIN_METHODS
};
