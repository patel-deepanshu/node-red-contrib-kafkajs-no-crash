'use strict';

/**
 * The fake kafkajs must behave like kafkajs 2.2.4 wherever the nodes depend
 * on it. Part of this file compares the fake with the REAL kafkajs, offline
 * (no broker needed): factory validation, argument checks, "not running"
 * errors and event names must be identical.
 */

const assert = require('node:assert/strict');
const real = require('kafkajs');
const K = require('kafkajs/src/errors');
const { createFakeKafkajs, PRODUCER_EVENTS, CONSUMER_EVENTS, ADMIN_EVENTS, ADMIN_METHODS } = require('../helpers/fake-kafkajs');
const { sleep, waitFor } = require('../helpers/common');

const silent = () => () => {};
const NOT_INITIALIZED = 'Consumer group was not initialized, consumer#run must be called first';

/** Run fn and describe the outcome: 'ok', 'THROW Name: msg' or 'REJECT Name: msg'. */
async function outcome(fn) {
    let result;
    try {
        result = fn();
    } catch (e) {
        return `THROW ${e.name}: ${e.message}`;
    }
    if (result && typeof result.then === 'function') {
        try {
            await result;
            return 'ok';
        } catch (e) {
            return `REJECT ${e.name}: ${e.message}`;
        }
    }
    return 'ok';
}

function isPending(promise, ms = 30) {
    return Promise.race([promise.then(() => false, () => false), sleep(ms).then(() => true)]);
}

/** Collect every event (type + payload) a client emits. */
function recordEvents(client) {
    const seen = [];
    for (const type of Object.values(client.events)) {
        client.on(type, (event) => {
            seen.push(event);
        });
    }
    return seen;
}

describe('test/helpers/fake-kafkajs', function () {
    let fake;
    let kafka;

    beforeEach(function () {
        fake = createFakeKafkajs();
        kafka = new fake.Kafka({ clientId: 'test', brokers: ['localhost:9092'], logCreator: silent });
    });

    afterEach(function () {
        fake.reset();
    });

    const newProducer = (opts = {}) => kafka.producer({ createPartitioner: fake.Partitioners.DefaultPartitioner, ...opts });
    const newConsumer = (opts = {}) => kafka.consumer({ groupId: 'g1', ...opts });

    describe('module shape', function () {
        it('exposes the kafkajs constants the nodes use', function () {
            assert.deepEqual(fake.logLevel, { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 });
            assert.deepEqual(fake.logLevel, real.logLevel);
            assert.deepEqual(fake.CompressionTypes, { None: 0, GZIP: 1, Snappy: 2, LZ4: 3, ZSTD: 4 });
            assert.deepEqual(fake.CompressionCodecs, {});
            assert.equal(typeof fake.Partitioners.DefaultPartitioner, 'function');
            assert.equal(fake.Partitioners.DefaultPartitioner()({ topic: 't', partitionMetadata: [], message: {} }), 0);
            assert.equal(fake.Partitioners.LegacyPartitioner()({ topic: 't', partitionMetadata: [], message: {} }), 0);
            assert.equal(fake.ConfigResourceTypes, real.ConfigResourceTypes);
        });

        it('exports the real kafkajs error classes, like require("kafkajs")', function () {
            for (const name of ['KafkaJSError', 'KafkaJSNonRetriableError', 'KafkaJSProtocolError', 'KafkaJSConnectionError',
                'KafkaJSNumberOfRetriesExceeded', 'KafkaJSSASLAuthenticationError', 'KafkaJSRequestTimeoutError']) {
                assert.equal(fake[name], real[name], name);
            }
        });

        it('uses the kafkajs event names (SPEC §18)', function () {
            assert.deepEqual(PRODUCER_EVENTS, {
                CONNECT: 'producer.connect',
                DISCONNECT: 'producer.disconnect',
                REQUEST: 'producer.network.request',
                REQUEST_TIMEOUT: 'producer.network.request_timeout',
                REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size'
            });
            assert.deepEqual(CONSUMER_EVENTS, {
                HEARTBEAT: 'consumer.heartbeat',
                COMMIT_OFFSETS: 'consumer.commit_offsets',
                GROUP_JOIN: 'consumer.group_join',
                FETCH: 'consumer.fetch',
                FETCH_START: 'consumer.fetch_start',
                START_BATCH_PROCESS: 'consumer.start_batch_process',
                END_BATCH_PROCESS: 'consumer.end_batch_process',
                CONNECT: 'consumer.connect',
                DISCONNECT: 'consumer.disconnect',
                STOP: 'consumer.stop',
                CRASH: 'consumer.crash',
                REBALANCING: 'consumer.rebalancing',
                RECEIVED_UNSUBSCRIBED_TOPICS: 'consumer.received_unsubscribed_topics',
                REQUEST: 'consumer.network.request',
                REQUEST_TIMEOUT: 'consumer.network.request_timeout',
                REQUEST_QUEUE_SIZE: 'consumer.network.request_queue_size'
            });
            assert.deepEqual(ADMIN_EVENTS, {
                CONNECT: 'admin.connect',
                DISCONNECT: 'admin.disconnect',
                REQUEST: 'admin.network.request',
                REQUEST_TIMEOUT: 'admin.network.request_timeout',
                REQUEST_QUEUE_SIZE: 'admin.network.request_queue_size'
            });
            const realKafka = new real.Kafka({ brokers: ['127.0.0.1:1'], logCreator: silent });
            assert.deepEqual(newProducer().events, realKafka.producer({ createPartitioner: real.Partitioners.DefaultPartitioner }).events);
            assert.deepEqual(newConsumer().events, realKafka.consumer({ groupId: 'g' }).events);
            assert.deepEqual(kafka.admin().events, realKafka.admin().events);
        });

        it('creates independent fakes', function () {
            const other = createFakeKafkajs();
            other.behaviour.connectError = new Error('x');
            new other.Kafka({ brokers: ['a:1'] });
            assert.equal(fake.behaviour.connectError, undefined);
            assert.equal(fake.instances.length, 1);
            assert.equal(other.instances.length, 1);
            assert.notEqual(other.CompressionCodecs, fake.CompressionCodecs);
        });
    });

    describe('parity with the real kafkajs (offline)', function () {
        const opts = { brokers: ['127.0.0.1:1'], logCreator: silent };
        const prod = (M, extra) => new M.Kafka(opts).producer({ createPartitioner: M.Partitioners.DefaultPartitioner, ...extra });
        const cons = (M) => new M.Kafka(opts).consumer({ groupId: 'g' });
        const admin = (M) => new M.Kafka(opts).admin();
        const cases = [
            ['new Kafka()', (M) => new M.Kafka()],
            ['new Kafka(null)', (M) => new M.Kafka(null)],
            ['non-function logCreator', (M) => new M.Kafka({ brokers: ['a:1'], logCreator: 'x' })],
            ['producer() with string requestTimeout', (M) => new M.Kafka({ ...opts, requestTimeout: '30000' }).producer()],
            ['producer() with null requestTimeout', (M) => new M.Kafka({ ...opts, requestTimeout: null }).producer()],
            ['consumer() with string requestTimeout', (M) => new M.Kafka({ ...opts, requestTimeout: '1' }).consumer({ groupId: 'g' })],
            ['admin() with string requestTimeout', (M) => new M.Kafka({ ...opts, requestTimeout: '1' }).admin()],
            ['producer() with invalid createPartitioner', (M) => new M.Kafka(opts).producer({ createPartitioner: 'x' })],
            ['producer() with null createPartitioner', (M) => new M.Kafka(opts).producer({ createPartitioner: null })],
            ['idempotent producer with retries 0', (M) => prod(M, { idempotent: true, retry: { retries: 0 } })],
            ['producer(null)', (M) => new M.Kafka(opts).producer(null)],
            ['consumer() without groupId', (M) => new M.Kafka(opts).consumer()],
            ['consumer() heartbeat >= session as strings', (M) => new M.Kafka(opts).consumer({ groupId: 'g', heartbeatInterval: '5000', sessionTimeout: '30000' })],
            ['consumer() heartbeat < session as strings', (M) => new M.Kafka(opts).consumer({ groupId: 'g', heartbeatInterval: '3000', sessionTimeout: '30000' })],
            ['consumer() heartbeat == session', (M) => new M.Kafka(opts).consumer({ groupId: 'g', heartbeatInterval: 30000, sessionTimeout: 30000 })],
            ['producer.on(invalid)', (M) => prod(M).on('bogus', () => {})],
            ['consumer.on(invalid)', (M) => cons(M).on('bogus', () => {})],
            ['admin.on(invalid)', (M) => admin(M).on('bogus', () => {})],
            ['send before connect', (M) => prod(M).send({ topic: 't', messages: [{ value: 'a' }] })],
            ['send with empty topic', (M) => prod(M).send({ topic: '', messages: [{ value: 'a' }] })],
            ['send with null messages', (M) => prod(M).send({ topic: 't', messages: null })],
            ['send with a message without value', (M) => prod(M).send({ topic: 't', messages: [{ key: 'k' }] })],
            ['send without argument', (M) => prod(M).send()],
            ['sendBatch with a missing topic', (M) => prod(M).sendBatch({ topicMessages: [{ messages: [] }] })],
            ['producer disconnect before connect', (M) => prod(M).disconnect()],
            ['pause before run', (M) => cons(M).pause([{ topic: 't' }])],
            ['pause with invalid topic', (M) => cons(M).pause([{}])],
            ['pause with invalid partitions', (M) => cons(M).pause([{ topic: 't', partitions: ['x'] }])],
            ['resume before run', (M) => cons(M).resume([{ topic: 't' }])],
            ['seek before run', (M) => cons(M).seek({ topic: 't', partition: 0, offset: '5' })],
            ['seek with invalid offset', (M) => cons(M).seek({ topic: 't', partition: 0, offset: 'abc' })],
            ['seek with negative offset', (M) => cons(M).seek({ topic: 't', partition: 0, offset: '-5' })],
            ['seek to -1 (latest) before run', (M) => cons(M).seek({ topic: 't', partition: 0, offset: '-1' })],
            ['seek without topic', (M) => cons(M).seek({ partition: 0, offset: '1' })],
            ['commitOffsets before run', (M) => cons(M).commitOffsets([{ topic: 't', partition: 0, offset: '1' }])],
            ['commitOffsets with invalid offset', (M) => cons(M).commitOffsets([{ topic: 't', partition: 0, offset: 'x' }])],
            ['paused() before run', (M) => cons(M).paused().length],
            ['subscribe without topics', (M) => cons(M).subscribe({})],
            ['subscribe with non-array topics', (M) => cons(M).subscribe({ topics: 't' })],
            ['subscribe with a number topic', (M) => cons(M).subscribe({ topics: [5] })],
            ['consumer disconnect before connect', (M) => cons(M).disconnect()],
            ['consumer stop before run', (M) => cons(M).stop()],
            ['createTopics with a string', (M) => admin(M).createTopics({ topics: 'x' })],
            ['createTopics with duplicates', (M) => admin(M).createTopics({ topics: [{ topic: 'a' }, { topic: 'a' }] })],
            ['deleteTopics with a number', (M) => admin(M).deleteTopics({ topics: [1] })],
            ['createPartitions with an empty array', (M) => admin(M).createPartitions({ topicPartitions: [] })],
            ['fetchTopicOffsets without topic', (M) => admin(M).fetchTopicOffsets()],
            ['fetchTopicOffsetsByTimestamp without topic', (M) => admin(M).fetchTopicOffsetsByTimestamp(undefined, 1)],
            ['fetchOffsets without groupId', (M) => admin(M).fetchOffsets({})],
            ['resetOffsets without topic', (M) => admin(M).resetOffsets({ groupId: 'g' })],
            ['setOffsets without partitions', (M) => admin(M).setOffsets({ groupId: 'g', topic: 't' })],
            ['describeGroups with a string', (M) => admin(M).describeGroups('g')],
            ['deleteGroups with a number', (M) => admin(M).deleteGroups([1])],
            ['describeConfigs with no resources', (M) => admin(M).describeConfigs({ resources: [] })],
            ['describeConfigs with an invalid type', (M) => admin(M).describeConfigs({ resources: [{ type: 99, name: 'x' }] })],
            ['fetchTopicMetadata with an empty topic', (M) => admin(M).fetchTopicMetadata({ topics: [''] })]
        ];
        for (const [name, fn] of cases) {
            it(name, async function () {
                const expected = await outcome(() => fn(real));
                const actual = await outcome(() => fn(fake));
                assert.equal(actual, expected);
            });
        }

        it('builds instrumentation events with the same shape ({id, type, timestamp, payload})', async function () {
            const realProducer = prod(real);
            const realEvents = [];
            realProducer.on(realProducer.events.DISCONNECT, (e) => realEvents.push(e));
            await realProducer.disconnect();
            const fakeProducer = prod(fake);
            const fakeEvents = [];
            fakeProducer.on(fakeProducer.events.DISCONNECT, (e) => fakeEvents.push(e));
            await fakeProducer.disconnect();
            assert.equal(realEvents.length, 1);
            assert.equal(fakeEvents.length, 1);
            assert.deepEqual(Object.keys(fakeEvents[0]), Object.keys(realEvents[0]));
            assert.deepEqual(Object.keys(fakeEvents[0]), ['id', 'type', 'timestamp', 'payload']);
            assert.equal(fakeEvents[0].type, realEvents[0].type);
            assert.equal(typeof fakeEvents[0].id, 'number');
            assert.equal(typeof fakeEvents[0].timestamp, 'number');
            assert.equal(fakeEvents[0].payload, undefined);
        });
    });

    describe('Kafka client', function () {
        it('records every instance with its options', function () {
            const options = { clientId: 'c2', brokers: ['b:9092'], requestTimeout: 1000, logCreator: silent };
            const k2 = new fake.Kafka(options);
            assert.equal(fake.instances.length, 2);
            assert.equal(fake.instances[1], k2);
            assert.equal(k2.options, options);
            assert.deepEqual(k2.brokers, ['b:9092'], 'option values are also readable on the instance');
            assert.equal(k2.logCreator, silent);
            assert.ok(k2 instanceof fake.Kafka);
        });

        it('calls logCreator once with the log level and routes logs through it, filtering by level', function () {
            const entries = [];
            const levels = [];
            const logCreator = (level) => {
                levels.push(level);
                return (entry) => entries.push(entry);
            };
            const k = new fake.Kafka({ brokers: ['a:1'], logLevel: fake.logLevel.WARN, logCreator });
            assert.deepEqual(levels, [fake.logLevel.WARN]);
            k.logger().error('boom', { broker: 'a:1' });
            k.logger().warn('careful');
            k.logger().info('hidden');
            k.logger().namespace('Connection').debug('hidden too');
            k.logger().namespace('Connection').error('from a namespace');
            assert.equal(entries.length, 3);
            assert.deepEqual(entries.map((e) => [e.level, e.label, e.namespace, e.log.message]), [
                [1, 'ERROR', undefined, 'boom'],
                [2, 'WARN', undefined, 'careful'],
                [1, 'ERROR', 'Connection', 'from a namespace']
            ]);
            assert.equal(entries[0].log.broker, 'a:1');
            assert.equal(entries[0].log.logger, 'kafkajs');
            assert.equal(typeof entries[0].log.timestamp, 'string');
            assert.equal(levels.length, 1, 'logCreator is called only once');
        });

        it('defaults to INFO and, like kafkajs, lets everything through with a string logLevel', function () {
            const entries = [];
            const k1 = new fake.Kafka({ brokers: ['a:1'], logCreator: () => (e) => entries.push(e) });
            k1.logger().info('info');
            k1.logger().debug('debug');
            assert.deepEqual(entries.map((e) => e.log.message), ['info']);
            entries.length = 0;
            const k2 = new fake.Kafka({ brokers: ['a:1'], logLevel: 'warn', logCreator: () => (e) => entries.push(e) });
            k2.logger().debug('debug');
            assert.deepEqual(entries.map((e) => e.log.message), ['debug']);
        });

        it('lets a throwing logCreator function throw (kafkajs does not protect itself)', function () {
            const k = new fake.Kafka({ brokers: ['a:1'], logCreator: () => () => { throw new Error('log boom'); } });
            assert.throws(() => k.logger().error('x'), /log boom/);
        });

        it('warns about the default partitioner only when createPartitioner is missing', function () {
            const entries = [];
            const k = new fake.Kafka({ brokers: ['a:1'], logCreator: () => (e) => entries.push(e) });
            const saved = process.env.KAFKAJS_NO_PARTITIONER_WARNING;
            delete process.env.KAFKAJS_NO_PARTITIONER_WARNING;
            try {
                k.producer({ createPartitioner: fake.Partitioners.LegacyPartitioner });
                assert.equal(entries.length, 0);
                k.producer();
                assert.equal(entries.length, 1);
                assert.match(entries[0].log.message, /switched default partitioner/);
                assert.equal(entries[0].label, 'WARN');
            } finally {
                if (saved !== undefined) process.env.KAFKAJS_NO_PARTITIONER_WARNING = saved;
            }
        });
    });

    describe('producer', function () {
        it('connects, emits CONNECT and records options and counts', async function () {
            const options = { createPartitioner: fake.Partitioners.DefaultPartitioner, allowAutoTopicCreation: false };
            const p = kafka.producer(options);
            assert.equal(fake.producers[0], p);
            assert.equal(kafka.producers[0], p);
            assert.equal(p.options, options);
            assert.equal(p.state, 'disconnected');
            const events = recordEvents(p);
            await p.connect();
            assert.equal(p.state, 'connected');
            assert.equal(p.connectCount, 1);
            assert.equal(events.length, 1);
            assert.equal(events[0].type, 'producer.connect');
            assert.deepEqual(Object.keys(events[0]), ['id', 'type', 'timestamp', 'payload']);
        });

        it('send() resolves RecordMetadata per partition and records the messages in fake.sent', async function () {
            const p = newProducer();
            await p.connect();
            const r1 = await p.send({
                topic: 't1', acks: -1, timeout: 30000, compression: fake.CompressionTypes.GZIP,
                messages: [{ key: 'k', value: 'v1', headers: { h: 'x' }, timestamp: '1700000000000' }, { value: Buffer.from('v2') }]
            });
            assert.deepEqual(r1, [{
                topicName: 't1', partition: 0, errorCode: 0, baseOffset: '0', logAppendTime: '-1', logStartOffset: '0'
            }]);
            const r2 = await p.send({ topic: 't1', messages: [{ value: 'v3' }, { value: 'v4', partition: 2 }] });
            assert.deepEqual(r2.map((r) => [r.partition, r.baseOffset]), [[0, '2'], [2, '0']]);
            assert.equal(fake.sent.length, 4);
            assert.deepEqual(fake.sent[0], {
                topic: 't1', key: 'k', value: 'v1', headers: { h: 'x' }, partition: 0, timestamp: '1700000000000',
                acks: -1, compression: 1, offset: '0'
            });
            assert.deepEqual(fake.sent.map((m) => [m.partition, m.offset]), [[0, '0'], [0, '1'], [0, '2'], [2, '0']]);
            assert.equal(fake.sent[2].acks, -1, 'kafkajs default acks');
            assert.deepEqual(p.sendCalls.map((c) => c.method), ['send', 'send']);
        });

        it('sendBatch() merges topics and returns metadata for each', async function () {
            const p = newProducer();
            await p.connect();
            const result = await p.sendBatch({
                acks: 1,
                topicMessages: [{ topic: 'a', messages: [{ value: '1' }] }, { topic: 'b', messages: [{ value: '2' }] }, { topic: 'a', messages: [{ value: '3' }] }]
            });
            assert.deepEqual(result.map((r) => [r.topicName, r.baseOffset]), [['a', '0'], ['b', '0']]);
            assert.deepEqual(fake.sent.map((m) => `${m.topic}:${m.value}:${m.offset}`), ['a:1:0', 'a:3:1', 'b:2:0']);
            assert.equal(p.sendCalls[0].method, 'sendBatch');
        });

        it('acks 0 resolves [] but the messages are written', async function () {
            const p = newProducer();
            await p.connect();
            assert.deepEqual(await p.send({ topic: 't', acks: 0, messages: [{ value: 'x' }] }), []);
            assert.equal(fake.sent.length, 1);
            assert.equal(fake.sent[0].acks, 0);
        });

        it('silently drops messages for partitions that do not exist (kafkajs resolves [])', async function () {
            fake.behaviour.partitionCount = 3;
            const p = newProducer();
            await p.connect();
            assert.deepEqual(await p.send({ topic: 't', messages: [{ value: 'x', partition: 99 }] }), []);
            assert.equal(fake.sent.length, 0);
            const mixed = await p.send({ topic: 't', messages: [{ value: 'a', partition: 1 }, { value: 'b', partition: 5 }, { value: 'c', partition: '2' }] });
            assert.deepEqual(mixed.map((r) => r.partition).sort(), [1, 2]);
            assert.deepEqual(fake.sent.map((m) => m.value), ['a', 'c']);

            fake.behaviour.partitionCount = { small: 1 };
            assert.deepEqual(await p.send({ topic: 'small', messages: [{ value: 'x', partition: 1 }] }), []);
            assert.equal((await p.send({ topic: 'other', messages: [{ value: 'x', partition: 7 }] })).length, 1, 'unlisted topics: all partitions exist');
            fake.behaviour.partitionCount = (topic) => (topic === 'one' ? 1 : 10);
            assert.deepEqual(await p.send({ topic: 'one', messages: [{ value: 'x', partition: 1 }] }), []);
        });

        it('uses the configured partitioner for messages without a partition', async function () {
            const seen = [];
            const p = kafka.producer({
                createPartitioner: () => ({ topic, partitionMetadata, message }) => {
                    seen.push({ topic, partitions: partitionMetadata.length, value: message.value });
                    return 1;
                }
            });
            await p.connect();
            const r = await p.send({ topic: 't', messages: [{ value: 'x' }, { value: 'y', partition: 0 }] });
            assert.deepEqual(r.map((m) => m.partition).sort(), [0, 1]);
            assert.deepEqual(seen, [{ topic: 't', partitions: 1, value: 'x' }]);
        });

        it('rejects invalid message types with the errors of kafkajs\' own encoder', async function () {
            const p = newProducer();
            await p.connect();
            const send = (message, extra = {}) => p.send({ topic: 't', messages: [message], ...extra });
            // thrown inside kafkajs' encoder, so the stack has kafkajs frames (lib/errors isKafkaJsError relies on that)
            await assert.rejects(send({ value: 42 }), (e) => e.name === 'TypeError' && e.code === 'ERR_INVALID_ARG_TYPE' &&
                /[\\/]kafkajs[\\/]src[\\/]/.test(e.stack));
            await assert.rejects(send({ value: { a: 1 } }), (e) => e.code === 'ERR_INVALID_ARG_TYPE');
            await assert.rejects(send({ key: { a: 1 }, value: 'v' }), (e) => e.code === 'ERR_INVALID_ARG_TYPE');
            await assert.rejects(send({ value: 'v', headers: { h: 1 } }), (e) => e.code === 'ERR_INVALID_ARG_TYPE');
            await assert.rejects(send({ value: 'v', headers: { h: null } }), (e) => e.code === 'ERR_INVALID_ARG_TYPE');
            await assert.rejects(send({ value: 'v', headers: null }), TypeError);
            await assert.rejects(send({ value: 'v', timestamp: 'abc' }), { name: 'SyntaxError', message: 'Cannot convert abc to a BigInt' });
            await assert.rejects(send({ value: 'v' }, { compression: 99 }), { name: 'TypeError', message: 'Cannot read properties of null (reading \'compress\')' });
            await assert.rejects(send({ value: 'v' }, { compression: 'gzip' }), /reading 'compress'/);
            assert.equal(fake.sent.length, 0);
            // valid: tombstone, Buffer key, header arrays, numeric timestamp, every known codec
            await send({ key: Buffer.from('k'), value: null, headers: { h: ['a', Buffer.from('b')] }, timestamp: 1700000000000 });
            for (const compression of Object.values(fake.CompressionTypes)) await send({ value: 'v' }, { compression });
            assert.equal(fake.sent.length, 1 + Object.keys(fake.CompressionTypes).length);
            const kafkajsError = fake.errors.invalidArgType(42);
            await assert.rejects(send({ value: 42 }), { name: kafkajsError.name, message: kafkajsError.message });
        });

        it('rejects sends after disconnect() and while disconnecting', async function () {
            fake.behaviour.disconnectDelayMs = 30;
            const p = newProducer();
            const events = recordEvents(p);
            await p.connect();
            const disconnecting = p.disconnect();
            assert.equal(p.state, 'disconnecting');
            await assert.rejects(p.send({ topic: 't', messages: [{ value: 'x' }] }),
                { name: 'KafkaJSNonRetriableError', message: /The producer is disconnecting/ });
            await disconnecting;
            assert.equal(p.state, 'disconnected');
            assert.equal(p.disconnectCount, 1);
            assert.deepEqual(events.map((e) => e.type), ['producer.connect', 'producer.disconnect']);
            await assert.rejects(p.send({ topic: 't', messages: [{ value: 'x' }] }),
                (e) => e instanceof K.KafkaJSError && e.message === 'The producer is disconnected' && e.retriable === true);
        });

        it('validates idempotent acks like kafkajs', async function () {
            const p = newProducer({ idempotent: true });
            assert.equal(p.isIdempotent(), true);
            await p.connect();
            await assert.rejects(p.send({ topic: 't', acks: 1, messages: [{ value: 'x' }] }), /invalidates the idempotent producer/);
            await p.send({ topic: 't', acks: -1, messages: [{ value: 'x' }] });
        });

        it('fails connect() like kafkajs when authenticationTimeout is a string', async function () {
            const k = new fake.Kafka({ brokers: ['a:1'], authenticationTimeout: '1000', logCreator: silent });
            const p = k.producer({ createPartitioner: fake.Partitioners.DefaultPartitioner });
            await assert.rejects(p.connect(), { name: 'TypeError', message: '\'timeout\' is not a number, received \'string\'' });
            const ok = new fake.Kafka({ brokers: ['a:1'], connectionTimeout: '1000', logCreator: silent });
            await ok.producer({ createPartitioner: fake.Partitioners.DefaultPartitioner }).connect();
        });

        describe('behaviour knobs', function () {
            it('connectError as a value and as a function of (args, client)', async function () {
                const err = fake.errors.connectionRefused();
                fake.behaviour.connectError = err;
                const p = newProducer();
                await assert.rejects(p.connect(), (e) => e === err);
                assert.equal(p.state, 'disconnected');
                let calls = 0;
                fake.behaviour.connectError = (args, client) => {
                    assert.equal(client, p);
                    calls++;
                    return calls === 1 ? fake.errors.dnsFailure() : null;
                };
                await assert.rejects(p.connect(), /ENOTFOUND/);
                await p.connect();
                assert.equal(p.state, 'connected');
                fake.behaviour.connectError = () => {
                    throw new Error('knob threw');
                };
                await assert.rejects(newProducer().connect(), /knob threw/);
            });

            it('producerFactoryError throws synchronously from kafka.producer()', function () {
                fake.behaviour.producerFactoryError = new Error('factory boom');
                assert.throws(() => newProducer(), /factory boom/);
                assert.equal(fake.producers.length, 0);
                fake.behaviour.producerFactoryError = (args, client) => (args.idempotent && client === kafka ? new Error('no idempotence') : null);
                assert.throws(() => newProducer({ idempotent: true }), /no idempotence/);
                assert.ok(newProducer());
            });

            it('sendError per call, sendResult override, and the send is still recorded', async function () {
                const p = newProducer();
                await p.connect();
                fake.behaviour.sendError = (args) => (args.topic === 'bad' ? fake.errors.messageTooLarge() : null);
                await assert.rejects(p.send({ topic: 'bad', messages: [{ value: 'x' }] }), { type: 'MESSAGE_TOO_LARGE' });
                await p.send({ topic: 'good', messages: [{ value: 'x' }] });
                assert.deepEqual(fake.sent.map((m) => m.topic), ['good']);
                fake.behaviour.sendResult = [];
                assert.deepEqual(await p.send({ topic: 'good', messages: [{ value: 'y' }] }), []);
                fake.behaviour.sendResult = (args) => [{ topicName: args.topic, partition: 9 }];
                assert.deepEqual(await p.send({ topic: 'good', messages: [{ value: 'z' }] }), [{ topicName: 'good', partition: 9 }]);
                assert.equal(fake.sent.length, 3);
            });

            it('sendDelayMs keeps sends in flight (inFlight / maxInFlight)', async function () {
                const p = newProducer();
                await p.connect();
                fake.behaviour.sendDelayMs = 30;
                const a = p.send({ topic: 't', messages: [{ value: '1' }] });
                const b = p.send({ topic: 't', messages: [{ value: '2' }] });
                await sleep(5);
                assert.equal(p.inFlight, 2);
                assert.equal(fake.sent.length, 0);
                await Promise.all([a, b]);
                assert.equal(p.inFlight, 0);
                assert.equal(p.maxInFlight, 2);
                assert.equal(fake.sent.length, 2);
            });

            it('connectDelayMs: disconnect() does not cancel an in-flight connect (zombie client)', async function () {
                fake.behaviour.connectDelayMs = 40;
                const p = newProducer();
                const connecting = p.connect();
                await sleep(5);
                await p.disconnect(); // resolves at once, like kafkajs
                assert.equal(p.state, 'disconnected');
                await connecting;
                assert.equal(p.state, 'connected', 'the connect still completes');
                await p.send({ topic: 't', messages: [{ value: 'zombie' }] });
                assert.equal(fake.sent.length, 1);
            });

            it('a delay of Infinity never settles (hung broker)', async function () {
                fake.behaviour.connectDelayMs = Infinity;
                assert.equal(await isPending(newProducer().connect()), true);
                fake.behaviour.connectDelayMs = (args, client) => (client.kind === 'producer' ? 0 : Infinity);
                await newProducer().connect();
            });

            it('disconnectError rejects disconnect()', async function () {
                fake.behaviour.disconnectError = new Error('disconnect boom');
                const p = newProducer();
                await p.connect();
                await assert.rejects(p.disconnect(), /disconnect boom/);
            });
        });

        describe('listeners (kafkajs wrapper semantics)', function () {
            it('returns a remover and counts listeners', async function () {
                const p = newProducer();
                let n = 0;
                const remove = p.on(p.events.CONNECT, () => n++);
                assert.equal(typeof remove, 'function');
                assert.equal(p.listenerCount(p.events.CONNECT), 1);
                await p.connect();
                remove();
                assert.equal(p.listenerCount(p.events.CONNECT), 0);
                await p.connect();
                assert.equal(n, 1);
            });

            it('emit() helper delivers payloads synchronously and reports whether anyone listened', function () {
                const p = newProducer();
                assert.equal(p.emit(p.events.REQUEST_TIMEOUT, { broker: 'b' }), false);
                const seen = [];
                p.on(p.events.REQUEST_TIMEOUT, (e) => seen.push(e));
                assert.equal(p.emit(p.events.REQUEST_TIMEOUT, { broker: 'b', correlationId: 3 }), true);
                assert.equal(seen.length, 1);
                assert.equal(seen[0].type, 'producer.network.request_timeout');
                assert.deepEqual(seen[0].payload, { broker: 'b', correlationId: 3 });
                assert.throws(() => p.emit('consumer.crash', {}), /unknown event/);
            });

            it('a listener that throws synchronously makes emit() throw (kafkajs would crash the process)', async function () {
                const p = newProducer();
                p.on(p.events.REQUEST, () => {
                    throw new Error('sync listener boom');
                });
                assert.throws(() => p.emit(p.events.REQUEST, {}), /sync listener boom/);
                p.on(p.events.CONNECT, () => {
                    throw new Error('connect listener boom');
                });
                await assert.rejects(p.connect(), /connect listener boom/);
            });

            it('an async listener rejection is caught, logged and recorded', async function () {
                const entries = [];
                const k = new fake.Kafka({ brokers: ['a:1'], logCreator: () => (e) => entries.push(e) });
                const p = k.producer({ createPartitioner: fake.Partitioners.DefaultPartitioner });
                p.on(p.events.CONNECT, async () => {
                    throw new Error('async listener boom');
                });
                await p.connect();
                await waitFor(() => p.listenerErrors.length === 1);
                assert.match(p.listenerErrors[0].message, /async listener boom/);
                assert.ok(entries.some((e) => e.namespace === 'Producer' && /Failed to execute listener: async listener boom/.test(e.log.message)));
            });
        });
    });

    describe('consumer', function () {
        async function runningConsumer(opts = {}, runConfig = {}) {
            const c = newConsumer(opts);
            await c.connect();
            await c.subscribe({ topics: ['t1'], fromBeginning: true });
            await c.run({ eachMessage: async () => {}, ...runConfig });
            return c;
        }

        it('records options, the effective retry, subscriptions and the run config', async function () {
            const restartOnFailure = async () => false;
            const k = new fake.Kafka({ brokers: ['a:1'], retry: { retries: 3, initialRetryTime: 100 }, logCreator: silent });
            const options = { groupId: 'g', retry: { restartOnFailure } };
            const c = k.consumer(options);
            assert.equal(c.options, options);
            assert.equal(c.groupId, 'g');
            assert.deepEqual(c.retry, { retries: 3, initialRetryTime: 100, restartOnFailure });
            assert.deepEqual(k.consumer({ groupId: 'g2' }).retry, { retries: 5, initialRetryTime: 100 });
            await c.subscribe({ topic: 'a' });
            await c.subscribe({ topics: ['b', /^test-/], fromBeginning: true });
            assert.deepEqual(c.subscriptions, [
                { topics: ['a'], fromBeginning: false, resolved: ['a'] },
                { topics: ['b', /^test-/], fromBeginning: true, resolved: ['b', 'test-topic'] }
            ]);
            const runConfig = { autoCommit: false, eachMessage: async () => {} };
            await c.run(runConfig);
            assert.equal(c.runConfig, runConfig);
            assert.deepEqual(Object.keys(c.paused()), []);
        });

        it('resolves RegExp subscriptions once, against behaviour.clusterTopics', async function () {
            fake.behaviour.clusterTopics = ['orders', 'orders-dlq', 'audit'];
            const c = newConsumer();
            await c.subscribe({ topics: [/^orders/] });
            assert.deepEqual(c.subscriptions[0].resolved, ['orders', 'orders-dlq']);
        });

        it('emits GROUP_JOIN asynchronously after run(), which resolves after the join', async function () {
            const c = newConsumer();
            const events = recordEvents(c);
            await c.connect();
            await c.subscribe({ topics: ['t1', 't2'] });
            const running = c.run({ eachMessage: async () => {} });
            assert.equal(events.filter((e) => e.type === c.events.GROUP_JOIN).length, 0, 'not emitted synchronously');
            assert.equal(c.running, false);
            await running;
            assert.equal(c.running, true);
            assert.equal(c.state, 'connected');
            assert.deepEqual(events.map((e) => e.type), ['consumer.connect', 'consumer.connect', 'consumer.group_join']);
            const join = events[2];
            assert.deepEqual(Object.keys(join.payload).sort(),
                ['duration', 'groupId', 'groupProtocol', 'isLeader', 'leaderId', 'memberAssignment', 'memberId']);
            assert.equal(join.payload.groupId, 'g1');
            assert.deepEqual(join.payload.memberAssignment, { t1: [0], t2: [0] });
            assert.equal(c.joinCount, 1);
        });

        it('assigns partitions 0..n-1 per behaviour.partitionCount', async function () {
            fake.behaviour.partitionCount = 3;
            const c = await runningConsumer();
            assert.equal(c.running, true);
            c.pause([{ topic: 't1' }]);
            assert.deepEqual(c.paused(), [{ topic: 't1', partitions: [0, 1, 2] }]);
        });

        it('pause/resume/seek throw synchronously and commitOffsets rejects when not running', async function () {
            const c = newConsumer();
            assert.throws(() => c.pause([{ topic: 't1' }]), { name: 'KafkaJSNonRetriableError', message: NOT_INITIALIZED });
            assert.throws(() => c.resume([{ topic: 't1' }]), { message: NOT_INITIALIZED });
            assert.throws(() => c.seek({ topic: 't1', partition: 0, offset: '1' }), { message: NOT_INITIALIZED });
            const commit = c.commitOffsets([{ topic: 't1', partition: 0, offset: '1' }]);
            assert.ok(commit instanceof Promise, 'commitOffsets is async in kafkajs: it rejects, it does not throw');
            await assert.rejects(commit, { message: NOT_INITIALIZED });
            assert.deepEqual(c.paused(), []);
        });

        it('pause/resume/seek/commitOffsets work while running and are recorded', async function () {
            fake.behaviour.partitionCount = 2;
            const c = await runningConsumer();
            const events = recordEvents(c);
            c.pause([{ topic: 't1', partitions: [1] }]);
            assert.deepEqual(c.paused(), [{ topic: 't1', partitions: [1] }]);
            c.resume([{ topic: 't1', partitions: [1] }]);
            assert.deepEqual(c.paused(), []);
            c.seek({ topic: 't1', partition: 0, offset: 42 });
            c.seek({ topic: 't1', partition: 0, offset: '-2' });
            await c.commitOffsets([{ topic: 't1', partition: 1, offset: '11' }]);
            assert.deepEqual(c.pauses, [[{ topic: 't1', partitions: [1] }]]);
            assert.deepEqual(c.resumes, [[{ topic: 't1', partitions: [1] }]]);
            assert.deepEqual(c.seeks, [{ topic: 't1', partition: 0, offset: '42' }, { topic: 't1', partition: 0, offset: '-2' }]);
            assert.deepEqual(c.committed, [{ topic: 't1', partition: 1, offset: '11', metadata: null }]);
            const commitEvent = events.find((e) => e.type === c.events.COMMIT_OFFSETS);
            assert.deepEqual(commitEvent.payload.topics, [{ topic: 't1', partitions: [{ partition: 1, offset: '11' }] }]);
            assert.equal(commitEvent.payload.groupId, 'g1');
            fake.behaviour.commitError = fake.errors.protocol('REBALANCE_IN_PROGRESS');
            await assert.rejects(c.commitOffsets([{ topic: 't1', partition: 1, offset: '12' }]), { type: 'REBALANCE_IN_PROGRESS' });
            assert.equal(c.committed.length, 1);
        });

        describe('deliver()', function () {
            it('awaits eachMessage and passes a kafkajs-like payload', async function () {
                const received = [];
                let finished = false;
                const c = await runningConsumer({}, {
                    eachMessage: async (payload) => {
                        received.push(payload);
                        await sleep(30);
                        finished = true;
                    }
                });
                const before = Date.now();
                assert.equal(await c.deliver({ topic: 't1', message: { key: 'k', value: 'hello', headers: { h: 'x', many: ['a', 'b'] } } }), true);
                assert.equal(finished, true, 'deliver() resolved only after eachMessage finished');
                const { topic, partition, message, heartbeat, pause } = received[0];
                assert.equal(topic, 't1');
                assert.equal(partition, 0);
                assert.equal(typeof heartbeat, 'function');
                assert.equal(typeof pause, 'function');
                assert.ok(Buffer.isBuffer(message.key) && message.key.toString() === 'k');
                assert.ok(Buffer.isBuffer(message.value) && message.value.toString() === 'hello');
                assert.ok(Buffer.isBuffer(message.headers.h) && message.headers.h.toString() === 'x');
                assert.deepEqual(message.headers.many.map(String), ['a', 'b']);
                assert.equal(message.offset, '0');
                assert.equal(typeof message.timestamp, 'string');
                assert.ok(Number(message.timestamp) >= before - 1);
                assert.deepEqual(c.delivered, [{ topic: 't1', partition: 0, offset: '0' }]);
            });

            it('applies defaults, auto-increments offsets per partition and honours explicit values', async function () {
                const messages = [];
                const c = await runningConsumer({}, { eachMessage: async ({ partition, message }) => { messages.push({ partition, message }); } });
                await c.deliver({ topic: 't1' });
                await c.deliver({ topic: 't1' });
                await c.deliver({ topic: 't1', partition: 1 });
                await c.deliver({ topic: 't1', message: { offset: '100', timestamp: 5, value: null } });
                await c.deliver({ topic: 't1' });
                const first = messages[0].message;
                assert.equal(first.key, null);
                assert.ok(Buffer.isBuffer(first.value));
                assert.deepEqual(first.headers, {});
                assert.deepEqual(messages.map((m) => `${m.partition}:${m.message.offset}`), ['0:0', '0:1', '1:0', '0:100', '0:101']);
                assert.equal(messages[3].message.value, null, 'tombstone');
                assert.equal(messages[3].message.timestamp, '5');
            });

            it('wraps each message in START_BATCH_PROCESS / END_BATCH_PROCESS like the kafkajs runner', async function () {
                const order = [];
                const c = await runningConsumer({}, { eachMessage: async ({ message }) => { order.push(`msg ${message.offset}`); } });
                c.on(c.events.START_BATCH_PROCESS, (e) => order.push(e.payload));
                c.on(c.events.END_BATCH_PROCESS, (e) => order.push(`end ${e.payload.lastOffset} ${typeof e.payload.duration}`));
                await c.deliver({ topic: 't1', message: { offset: '7' } });
                await c.deliver({ topic: 't1', message: { offset: '8' }, highWatermark: 20 });
                assert.deepEqual(order, [
                    { topic: 't1', partition: 0, highWatermark: '8', offsetLag: '0', offsetLagLow: '0', batchSize: 1, firstOffset: '7', lastOffset: '7' },
                    'msg 7',
                    'end 7 number',
                    { topic: 't1', partition: 0, highWatermark: '20', offsetLag: '11', offsetLagLow: '11', batchSize: 1, firstOffset: '8', lastOffset: '8' },
                    'msg 8',
                    'end 8 number'
                ]);
            });

            it('skips paused partitions, and the payload pause() pauses the partition', async function () {
                let calls = 0;
                const c = await runningConsumer({}, {
                    eachMessage: async ({ pause }) => {
                        calls++;
                        pause();
                    }
                });
                assert.equal(await c.deliver({ topic: 't1' }), true);
                assert.deepEqual(c.paused(), [{ topic: 't1', partitions: [0] }]);
                assert.equal(await c.deliver({ topic: 't1' }), false);
                assert.equal(calls, 1);
            });

            it('rejects when not running, for unsubscribed topics, and when eachMessage throws', async function () {
                const c = newConsumer();
                await c.subscribe({ topics: ['t1'] });
                await assert.rejects(c.deliver({ topic: 't1' }), /needs a running consumer/);
                await c.run({
                    eachMessage: async ({ message }) => {
                        if (message.value.toString() === 'poison') throw new TypeError('bad message');
                    }
                });
                await assert.rejects(c.deliver({ topic: 'other' }), /not subscribed/);
                await assert.rejects(c.deliver({ topic: 't1', message: { value: 'poison' } }), /bad message/);
                assert.equal(c.eachMessageErrors.length, 1);
                const noHandler = newConsumer();
                await noHandler.subscribe({ topics: ['t1'] });
                await noHandler.run({ eachBatch: async () => {} });
                await assert.rejects(noHandler.deliver({ topic: 't1' }), /eachMessage/);
            });
        });

        describe('crash()', function () {
            it('emits STOP, DISCONNECT, CRASH in this order and marks the consumer not running', async function () {
                const c = await runningConsumer({ retry: { restartOnFailure: async () => false } });
                const events = recordEvents(c);
                const error = fake.errors.connectionRefused();
                await c.crash(error);
                assert.deepEqual(events.map((e) => e.type), ['consumer.stop', 'consumer.disconnect', 'consumer.crash']);
                assert.deepEqual(events[2].payload, { error, groupId: 'g1', restart: false });
                assert.equal(c.running, false);
                assert.equal(c.state, 'disconnected');
                assert.deepEqual(c.crashes, [{ error, groupId: 'g1', restart: false }]);
                assert.throws(() => c.pause([{ topic: 't1' }]), { message: NOT_INITIALIZED });
                await assert.rejects(c.deliver({ topic: 't1' }), /needs a running consumer/);
            });

            it('decides restart like kafkajs and calls restartOnFailure only for retriable errors', async function () {
                const seen = [];
                const c = await runningConsumer({ retry: { restartOnFailure: async (e) => { seen.push(e.name); return false; } } });
                await c.crash(fake.errors.saslAuth()); // non-retriable: restartOnFailure not consulted
                assert.deepEqual(seen, []);
                assert.equal(c.crashes[0].restart, false);
                await c.crash(fake.errors.connectionRefused()); // NumberOfRetriesExceeded: consulted
                assert.deepEqual(seen, ['KafkaJSNumberOfRetriesExceeded']);
                await c.crash(fake.errors.requestTimeout()); // root cause retriable: consulted
                assert.equal(seen.length, 2);
                await c.crash(fake.errors.connectionRefused(), true); // forced
                assert.equal(seen.length, 2);
                assert.equal(c.crashes[3].restart, true);
                c.stop(); // cancel the forced restart
            });

            it('restarts by itself when restart is true (no restartOnFailure): rejoins and loses pauses', async function () {
                fake.behaviour.restartDelayMs = 5;
                const c = await runningConsumer();
                c.pause([{ topic: 't1' }]);
                const events = recordEvents(c);
                await c.crash(fake.errors.connectionRefused());
                assert.equal(c.crashes[0].restart, true);
                await waitFor(() => c.running);
                assert.equal(c.joinCount, 2);
                assert.deepEqual(c.paused(), [], 'kafkajs loses pause state on restart');
                assert.ok(events.some((e) => e.type === c.events.GROUP_JOIN));
            });

            it('defaults to restarting when restartOnFailure rejects (logged)', async function () {
                fake.behaviour.restartDelayMs = 1000;
                const c = await runningConsumer({ retry: { restartOnFailure: async () => { throw new Error('nope'); } } });
                await c.crash(fake.errors.connectionRefused());
                assert.equal(c.crashes[0].restart, true);
                await c.stop(); // clears the pending restart
            });

            it('breaks like kafkajs with a non-async restartOnFailure', async function () {
                const c = await runningConsumer({ retry: { restartOnFailure: () => false } });
                await assert.rejects(c.crash(fake.errors.connectionRefused()), /\.catch is not a function/);
            });

            it('propagates a synchronous throw of a CRASH listener (unhandled rejection in kafkajs)', async function () {
                const c = await runningConsumer({ retry: { restartOnFailure: async () => false } });
                c.on(c.events.CRASH, () => {
                    throw new Error('crash listener boom');
                });
                await assert.rejects(c.crash(new Error('x')), /crash listener boom/);
            });

            it('groupJoinError: run() resolves and the failure arrives as CRASH', async function () {
                fake.behaviour.groupJoinError = fake.errors.groupCoordinatorNotFound();
                const c = newConsumer({ retry: { restartOnFailure: async () => false } });
                const events = recordEvents(c);
                await c.subscribe({ topics: ['t1'] });
                await c.run({ eachMessage: async () => {} });
                assert.equal(c.running, false);
                assert.equal(c.joinCount, 0);
                assert.deepEqual(events.map((e) => e.type), ['consumer.connect', 'consumer.stop', 'consumer.disconnect', 'consumer.crash']);
                assert.equal(events[3].payload.error.name, 'KafkaJSGroupCoordinatorNotFound');
                assert.equal(events[3].payload.restart, false);
            });

            it('a GROUP_JOIN listener that throws synchronously crashes the consumer', async function () {
                const c = newConsumer({ retry: { restartOnFailure: async () => false } });
                c.on(c.events.GROUP_JOIN, () => {
                    throw new TypeError('join listener boom');
                });
                await c.subscribe({ topics: ['t1'] });
                await c.run({ eachMessage: async () => {} });
                assert.equal(c.running, false);
                assert.equal(c.crashes[0].error.message, 'join listener boom');
            });
        });

        it('rejects subscribe while running and ignores a second run()', async function () {
            const entries = [];
            const k = new fake.Kafka({ brokers: ['a:1'], logCreator: () => (e) => entries.push(e) });
            const c = k.consumer({ groupId: 'g' });
            await c.subscribe({ topics: ['t1'] });
            await c.run({ eachMessage: async () => {} });
            await assert.rejects(c.subscribe({ topics: ['t2'] }), /Cannot subscribe to topic while consumer is running/);
            await c.run({ eachMessage: async () => {} });
            assert.equal(c.runCount, 1);
            assert.equal(c.joinCount, 1);
            assert.ok(entries.some((e) => /consumer is already running/.test(e.log.message)));
        });

        it('stop() emits STOP and keeps the connection; disconnect() emits STOP then DISCONNECT', async function () {
            const c = await runningConsumer();
            const events = recordEvents(c);
            await c.stop();
            assert.equal(c.running, false);
            assert.equal(c.state, 'connected');
            await c.run({ eachMessage: async () => {} });
            assert.equal(c.running, true);
            await c.disconnect();
            assert.deepEqual(events.map((e) => e.type),
                ['consumer.stop', 'consumer.connect', 'consumer.group_join', 'consumer.stop', 'consumer.disconnect']);
            assert.equal(c.state, 'disconnected');
        });

        it('does not join when disconnected while joining', async function () {
            const c = newConsumer();
            await c.subscribe({ topics: ['t1'] });
            const running = c.run({ eachMessage: async () => {} });
            await c.disconnect();
            await running;
            assert.equal(c.joinCount, 0);
            assert.equal(c.running, false);
        });

        it('knobs: consumerFactoryError, consumerConnectError, subscribeError, runError', async function () {
            fake.behaviour.consumerFactoryError = (options) => (options.groupId === 'bad' ? new Error('factory') : null);
            assert.throws(() => kafka.consumer({ groupId: 'bad' }), /factory/);
            const c = newConsumer();
            fake.behaviour.consumerConnectError = fake.errors.saslAuth();
            await assert.rejects(c.connect(), { name: 'KafkaJSSASLAuthenticationError' });
            fake.behaviour.subscribeError = (args) => (args.topics.includes('denied') ? fake.errors.protocol('TOPIC_AUTHORIZATION_FAILED') : null);
            await assert.rejects(c.subscribe({ topics: ['denied'] }), { type: 'TOPIC_AUTHORIZATION_FAILED' });
            await c.subscribe({ topics: ['ok'] });
            fake.behaviour.runError = new Error('run boom');
            await assert.rejects(c.run({ eachMessage: async () => {} }), /run boom/);
            assert.equal(c.running, false);
        });

        it('knobs: subscribeDelayMs and runDelayMs (a slow group join; stop() while joining prevents the join)', async function () {
            const c = newConsumer();
            await c.connect();
            fake.behaviour.subscribeDelayMs = (args) => (args.topics.includes('slow') ? 80 : 0);
            let t = Date.now();
            await c.subscribe({ topics: ['slow'] });
            assert.ok(Date.now() - t >= 70, 'subscribe should take subscribeDelayMs');
            fake.behaviour.runDelayMs = 80;
            t = Date.now();
            await c.run({ eachMessage: async () => {} });
            assert.ok(Date.now() - t >= 70, 'run() resolves after the slow join');
            assert.equal(c.joinCount, 1);
            assert.equal(c.running, true);

            const d = newConsumer();
            await d.connect();
            await d.subscribe({ topics: ['t1'] });
            const running = d.run({ eachMessage: async () => {} });
            await sleep(20); // joining
            assert.equal(d.running, false);
            await d.disconnect();
            await running;
            assert.equal(d.joinCount, 0);
            assert.equal(d.running, false);
        });
    });

    describe('admin', function () {
        it('connects/disconnects with events and adminConnectError', async function () {
            const a = kafka.admin();
            assert.equal(fake.admins[0], a);
            const events = recordEvents(a);
            fake.behaviour.adminConnectError = fake.errors.connectionRefused();
            await assert.rejects(a.connect(), { name: 'KafkaJSNumberOfRetriesExceeded' });
            delete fake.behaviour.adminConnectError;
            await a.connect();
            await a.disconnect();
            assert.deepEqual(events.map((e) => e.type), ['admin.connect', 'admin.disconnect']);
            assert.equal(a.connectCount, 1);
            assert.equal(a.disconnectCount, 1);
        });

        it('implements every SPEC §17 method with kafkajs-shaped canned results', async function () {
            const a = kafka.admin();
            assert.deepEqual(await a.listTopics(), ['test-topic']);
            assert.equal(await a.createTopics({ topics: [{ topic: 'x', numPartitions: 3 }] }), true);
            assert.equal(await a.deleteTopics({ topics: ['x'] }), undefined);
            assert.equal(await a.createPartitions({ topicPartitions: [{ topic: 'x', count: 4 }] }), undefined);
            const md = await a.fetchTopicMetadata({ topics: ['x'] });
            assert.equal(md.topics[0].name, 'x');
            assert.equal(md.topics[0].partitions[0].partitionId, 0);
            assert.deepEqual((await a.fetchTopicMetadata()).topics.map((t) => t.name), ['test-topic']);
            assert.deepEqual(await a.fetchTopicOffsets('x'), [{ partition: 0, offset: '0', high: '0', low: '0' }]);
            assert.deepEqual(await a.fetchTopicOffsetsByTimestamp('x', 1700000000000), [{ partition: 0, offset: '0' }]);
            assert.deepEqual(await a.fetchOffsets({ groupId: 'g', topics: ['x'] }), [{ topic: 'x', partitions: [{ partition: 0, offset: '-1', metadata: null }] }]);
            assert.equal(await a.resetOffsets({ groupId: 'g', topic: 'x', earliest: true }), undefined);
            assert.equal(await a.setOffsets({ groupId: 'g', topic: 'x', partitions: [{ partition: 0, offset: '5' }] }), undefined);
            assert.deepEqual(await a.listGroups(), { groups: [{ groupId: 'test-group', protocolType: 'consumer' }] });
            assert.equal((await a.describeGroups(['g'])).groups[0].groupId, 'g');
            assert.deepEqual(await a.deleteGroups(['g']), [{ groupId: 'g', errorCode: 0 }]);
            const cluster = await a.describeCluster();
            assert.deepEqual(Object.keys(cluster).sort(), ['brokers', 'clusterId', 'controller']);
            const configs = await a.describeConfigs({ resources: [{ type: fake.ConfigResourceTypes.TOPIC, name: 'x' }], includeSynonyms: false });
            assert.equal(configs.resources[0].resourceName, 'x');
            assert.equal((await a.alterConfigs({ resources: [{ type: fake.ConfigResourceTypes.TOPIC, name: 'x', configEntries: [] }] })).resources.length, 1);
            assert.deepEqual(a.calls.map((c) => c.method), [
                'listTopics', 'createTopics', 'deleteTopics', 'createPartitions', 'fetchTopicMetadata', 'fetchTopicMetadata',
                'fetchTopicOffsets', 'fetchTopicOffsetsByTimestamp', 'fetchOffsets', 'resetOffsets', 'setOffsets', 'listGroups',
                'describeGroups', 'deleteGroups', 'describeCluster', 'describeConfigs', 'alterConfigs'
            ]);
            assert.deepEqual(a.calls[7].args, { topic: 'x', timestamp: 1700000000000 });
            for (const method of ADMIN_METHODS) assert.equal(typeof a[method], 'function', method);
        });

        it('adminErrors / adminResults / adminDelayMs as values and functions', async function () {
            const a = kafka.admin();
            const err = fake.errors.protocol('TOPIC_ALREADY_EXISTS');
            fake.behaviour.adminErrors = {
                createTopics: err,
                deleteTopics: (args, admin) => (admin === a && args.topics.includes('locked') ? new Error('locked') : null)
            };
            fake.behaviour.adminResults = {
                listTopics: ['a', 'b'],
                fetchTopicOffsets: (topic) => [{ partition: 0, offset: '7', high: '7', low: '0', topic }]
            };
            await assert.rejects(a.createTopics({ topics: [{ topic: 'x' }] }), (e) => e === err);
            await assert.rejects(a.deleteTopics({ topics: ['locked'] }), /locked/);
            await a.deleteTopics({ topics: ['free'] });
            assert.deepEqual(await a.listTopics(), ['a', 'b']);
            assert.equal((await a.fetchTopicOffsets('q'))[0].topic, 'q');
            fake.behaviour.adminDelayMs = { listGroups: Infinity };
            assert.equal(await isPending(a.listGroups()), true);
            assert.ok(await a.describeCluster());
        });

        it('uses behaviour.clusterTopics for listTopics/fetchTopicMetadata defaults', async function () {
            fake.behaviour.clusterTopics = ['one', 'two'];
            const a = kafka.admin();
            assert.deepEqual(await a.listTopics(), ['one', 'two']);
            assert.deepEqual((await a.fetchTopicMetadata({})).topics.map((t) => t.name), ['one', 'two']);
        });
    });

    describe('errors (shapes observed from kafkajs, 5-kafkajs-internals.md §1.2-1.3)', function () {
        const E = () => fake.errors;

        it('connectionRefused: NumberOfRetriesExceeded <- ConnectionError(ECONNREFUSED)', function () {
            const e = E().connectionRefused();
            assert.ok(e instanceof K.KafkaJSNumberOfRetriesExceeded);
            assert.equal(e.name, 'KafkaJSNumberOfRetriesExceeded');
            assert.equal(e.retriable, false);
            assert.equal(e.code, undefined, 'the wrapper has no code');
            assert.equal(e.message, 'Connection error: connect ECONNREFUSED 127.0.0.1:9092');
            assert.equal(typeof e.retryCount, 'number');
            assert.equal(typeof e.retryTime, 'number');
            assert.ok(e.cause instanceof K.KafkaJSConnectionError);
            assert.equal(e.cause.name, 'KafkaJSConnectionError');
            assert.equal(e.cause.code, 'ECONNREFUSED');
            assert.equal(e.cause.broker, '127.0.0.1:9092');
            assert.equal(e.cause.retriable, true);
            assert.notEqual(E().connectionRefused(), E().connectionRefused(), 'a new instance per call');
        });

        it('dnsFailure / connectionTimeout / connectionClosed / tlsSelfSigned', function () {
            const dns = E().dnsFailure('nope.invalid');
            assert.equal(dns.name, 'KafkaJSNumberOfRetriesExceeded');
            assert.equal(dns.cause.code, 'ENOTFOUND');
            assert.equal(dns.message, 'Connection error: getaddrinfo ENOTFOUND nope.invalid');
            const timeout = E().connectionTimeout();
            assert.equal(timeout.cause.message, 'Connection timeout');
            assert.equal(timeout.cause.code, undefined);
            const closed = E().connectionClosed('h', 1);
            assert.equal(closed.cause.name, 'KafkaJSConnectionClosedError');
            assert.equal(closed.cause.broker, 'h:1');
            const tls = E().tlsSelfSigned();
            assert.equal(tls.cause.code, 'DEPTH_ZERO_SELF_SIGNED_CERT');
            assert.equal(tls.cause.retriable, true);
        });

        it('sendDuringOutage / nonRetriableWrap: NonRetriable whose cause is the ConnectionError', function () {
            const e = E().sendDuringOutage();
            assert.equal(e.name, 'KafkaJSNonRetriableError');
            assert.equal(e.retriable, false);
            assert.equal(e.cause.name, 'KafkaJSConnectionError', 'retry/index.js passes e.cause || e');
            assert.equal(e.cause.code, 'ECONNREFUSED');
            const plain = new Error('inner');
            assert.equal(E().nonRetriableWrap(plain).cause, plain);
            assert.equal(E().nonRetriableWrap(plain).message, 'inner');
            const wrapped = E().retriesExceeded(plain, { retryCount: 2, retryTime: 10 });
            assert.equal(wrapped.cause, plain);
            assert.equal(wrapped.retryCount, 2);
        });

        it('saslAuth / saslMissingCredentials: raw, non-retriable, no cause', function () {
            for (const e of [E().saslAuth(), E().saslAuth('SCRAM-SHA-256'), E().saslMissingCredentials()]) {
                assert.ok(e instanceof K.KafkaJSSASLAuthenticationError);
                assert.equal(e.name, 'KafkaJSSASLAuthenticationError');
                assert.equal(e.retriable, false);
                assert.equal(e.cause, undefined);
            }
            assert.match(E().saslAuth().message, /^SASL PLAIN authentication failed/);
            assert.equal(E().saslMissingCredentials().message, 'SASL Plain: Invalid username or password');
        });

        it('producerDisconnected / producerDisconnecting', function () {
            const e = E().producerDisconnected();
            assert.equal(e.name, 'KafkaJSError');
            assert.equal(e.message, 'The producer is disconnected');
            assert.equal(e.retriable, true);
            assert.equal(E().producerDisconnecting().retriable, false);
        });

        it('protocol errors: messageTooLarge (raw), unknownTopic and topicAuthorizationFailed (wrapped as for send)', function () {
            const big = E().messageTooLarge();
            assert.ok(big instanceof K.KafkaJSProtocolError);
            assert.equal(big.name, 'KafkaJSProtocolError');
            assert.equal(big.type, 'MESSAGE_TOO_LARGE');
            assert.equal(big.code, 10);
            assert.equal(big.retriable, false);
            const unknown = E().unknownTopic();
            assert.equal(unknown.name, 'KafkaJSNumberOfRetriesExceeded');
            assert.equal(unknown.type, undefined, 'type only on the cause');
            assert.equal(unknown.cause.type, 'UNKNOWN_TOPIC_OR_PARTITION');
            assert.equal(unknown.cause.code, 3);
            assert.equal(unknown.cause.retriable, true);
            const denied = E().topicAuthorizationFailed();
            assert.equal(denied.name, 'KafkaJSNonRetriableError');
            assert.equal(denied.cause.type, 'TOPIC_AUTHORIZATION_FAILED');
            assert.equal(denied.cause.code, 29);
            assert.equal(E().protocol(27).type, 'REBALANCE_IN_PROGRESS');
            assert.throws(() => E().protocol('NOT_A_TYPE'), /unknown Kafka protocol error/);
        });

        it('requestTimeout / groupCoordinatorNotFound / notImplemented / invalidArgType', function () {
            const t = E().requestTimeout('b:1');
            assert.equal(t.name, 'KafkaJSRequestTimeoutError');
            assert.equal(t.retriable, true);
            assert.equal(t.broker, 'b:1');
            assert.equal(E().groupCoordinatorNotFound().name, 'KafkaJSGroupCoordinatorNotFound');
            assert.equal(E().groupCoordinatorNotFound().retriable, false);
            const ni = E().notImplemented('LZ4');
            assert.equal(ni.name, 'KafkaJSNotImplemented');
            assert.equal(ni.message, 'LZ4 compression not implemented');
            const arg = E().invalidArgType({ a: 1 });
            assert.equal(arg.name, 'TypeError');
            assert.equal(arg.code, 'ERR_INVALID_ARG_TYPE');
            assert.match(arg.stack, /[\\/]kafkajs[\\/]src[\\/]/);
            assert.throws(() => E().invalidArgType('a string is valid'), /is a valid message value/);
        });

        it('makeError(name, message, extra)', function () {
            const e = fake.makeError('KafkaJSConnectionError', 'Connection error: x', { retriable: true, code: 'ECONNRESET' });
            assert.ok(e instanceof Error);
            assert.equal(e.name, 'KafkaJSConnectionError');
            assert.equal(e.message, 'Connection error: x');
            assert.equal(e.retriable, true);
            assert.equal(e.code, 'ECONNRESET');
            assert.equal(fake.makeError('X', 'y').name, 'X');
        });
    });

    describe('reset()', function () {
        it('forgets clients, messages, offsets, knobs and codecs but keeps object identity', async function () {
            const { instances, producers, consumers, admins, sent, behaviour, CompressionCodecs } = fake;
            const p = newProducer();
            await p.connect();
            await p.send({ topic: 't', messages: [{ value: 'x' }] });
            newConsumer();
            kafka.admin();
            fake.behaviour.connectError = new Error('x');
            fake.CompressionCodecs[2] = () => ({});
            fake.reset();
            assert.equal(fake.instances, instances);
            assert.equal(fake.behaviour, behaviour);
            for (const list of [instances, producers, consumers, admins, sent]) assert.equal(list.length, 0);
            assert.deepEqual(behaviour, {});
            assert.deepEqual(CompressionCodecs, {});
            const k = new fake.Kafka({ brokers: ['a:1'], logCreator: silent });
            const p2 = k.producer({ createPartitioner: fake.Partitioners.DefaultPartitioner });
            await p2.connect();
            const [meta] = await p2.send({ topic: 't', messages: [{ value: 'y' }] });
            assert.equal(meta.baseOffset, '0', 'offsets start again from 0');
        });

        it('cancels pending consumer restarts and joins of old consumers', async function () {
            fake.behaviour.restartDelayMs = 10;
            const restarting = newConsumer();
            await restarting.subscribe({ topics: ['t1'] });
            await restarting.run({ eachMessage: async () => {} });
            await restarting.crash(fake.errors.connectionRefused()); // restart scheduled
            const joining = newConsumer();
            await joining.subscribe({ topics: ['t1'] });
            const running = joining.run({ eachMessage: async () => {} });
            const joins = [];
            restarting.on(restarting.events.GROUP_JOIN, () => joins.push('restarting'));
            joining.on(joining.events.GROUP_JOIN, () => joins.push('joining'));
            fake.reset();
            await running;
            await sleep(30);
            assert.deepEqual(joins, []);
            assert.equal(restarting.running, false);
            assert.equal(joining.running, false);
        });
    });
});
