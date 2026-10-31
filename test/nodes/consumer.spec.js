'use strict';

/**
 * kafkajs-nc-consumer: subscription options, output messages, the connection
 * supervisor (retries, crashes, auth failures, zombie prevention), control
 * actions, bounded close, and the editor definition. Uses the fake kafkajs;
 * no Kafka needed. The crash trap in test/helpers/setup.js fails any test
 * that leaves an unhandled rejection or uncaught exception behind.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const brokerNode = require('../../nodes/kafka-broker.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const factory = require('../../lib/kafka-factory');
const logger = require('../../lib/logger');
const { createFakeKafkajs, CONSUMER_EVENTS } = require('../helpers/fake-kafkajs');
const { tmpDir, sleep, waitFor, nextStatus, statusMark } = require('../helpers/common');

helper.init(require.resolve('node-red'));

const { parseConfig, parseSeek, seekOffset, parseCommit, parseTopicRegex, fmtDelay } = consumerNode._internals;
const SAVED_TIMEOUTS = { ...consumerNode.TIMEOUTS };

/** Short backoffs so reconnect tests run fast (authRetryMin cannot go below 1 s). */
const FAST = { reconnectMin: '100', reconnectMax: '200', authRetryMin: '1000' };

function flow(consumerProps = {}, brokerProps = {}) {
    return [
        Object.assign({ id: 'b1', type: 'kafkajs-nc-broker', name: 'B', brokers: 'localhost:9092' }, FAST, brokerProps),
        Object.assign({
            id: 'k1', type: 'kafkajs-nc-consumer', name: 'K', broker: 'b1', topics: 'test-topic', wires: [['out']]
        }, consumerProps),
        { id: 'out', type: 'helper' },
        { id: 'c1', type: 'catch', scope: ['k1'], uncaught: false, wires: [['caught']] },
        { id: 'caught', type: 'helper' },
        { id: 'cp1', type: 'complete', scope: ['k1'], uncaught: false, wires: [['completed']] },
        { id: 'completed', type: 'helper' }
    ];
}

/** Calls of a Node.prototype spy (error/warn/log/debug) made by this node only, as strings. */
function callsOf(node, method) {
    const spy = node[method];
    return spy.args.filter((_, i) => spy.thisValues[i] === node).map((args) => String(args[0] && args[0].message ? args[0].message : args[0]));
}

/** Collect every message a helper node receives. */
function collect(id) {
    const list = [];
    helper.getNode(id).on('input', (msg) => list.push(msg));
    return list;
}

let testId = 0;

/**
 * Send a control message to k1 and wait until it completes (complete node)
 * or fails (catch node). Resolves { ok: msg } or { error: msg }.
 */
function control(props, timeout = 3000) {
    testId++;
    const msg = Object.assign({ _testId: testId }, props);
    return new Promise((resolve, reject) => {
        const completed = helper.getNode('completed');
        const caught = helper.getNode('caught');
        let timer = null;
        const cleanup = () => {
            clearTimeout(timer);
            completed.removeListener('input', onDone);
            caught.removeListener('input', onError);
        };
        function onDone(m) {
            if (m._testId !== msg._testId) return;
            cleanup();
            resolve({ ok: m });
        }
        function onError(m) {
            if (m._testId !== msg._testId) return;
            cleanup();
            resolve({ error: m });
        }
        timer = setTimeout(() => {
            cleanup();
            reject(new Error(`control message ${show(props)} neither completed nor failed within ${timeout} ms`));
        }, timeout);
        completed.on('input', onDone);
        caught.on('input', onError);
        helper.getNode('k1').receive(msg);
    });
}

/** JSON.stringify that copes with BigInt (used in assertion messages). */
function show(v) {
    return JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? `${x}n` : x));
}

async function expectOk(props) {
    const r = await control(props);
    assert.ok(r.ok, `expected ${show(props)} to succeed, got ${r.error && r.error.error && r.error.error.message}`);
    return r.ok;
}

async function expectError(props, re) {
    const r = await control(props);
    assert.ok(r.error, `expected ${show(props)} to fail`);
    assert.match(r.error.error.message, re);
    return r.error;
}

const isConnected = (s) => s.fill === 'green' && s.shape === 'dot';

describe('kafkajs-nc-consumer node', function () {
    let fake;

    before(function () {
        fake = createFakeKafkajs();
        factory.setKafkajs(fake);
    });

    after(function () {
        factory.setKafkajs(null);
    });

    beforeEach(function (done) {
        fake.reset();
        logger._resetDedupe();
        Object.assign(consumerNode.TIMEOUTS, SAVED_TIMEOUTS);
        helper.settings({ userDir: tmpDir('kafkajs-nc-consumer-') });
        helper.startServer(done);
    });

    afterEach(async function () {
        await helper.unload();
        await helper.stopServer();
        Object.assign(consumerNode.TIMEOUTS, SAVED_TIMEOUTS);
    });

    /** Load the flow and wait until the consumer has joined its group. */
    async function loadConnected(consumerProps, brokerProps, credentials) {
        await helper.load([brokerNode, consumerNode], flow(consumerProps, brokerProps), credentials || {});
        const k1 = helper.getNode('k1');
        await nextStatus(k1, isConnected);
        await waitFor(() => fake.consumers.length > 0 && fake.consumers[fake.consumers.length - 1].running);
        return { k1, consumer: fake.consumers[fake.consumers.length - 1] };
    }

    // ------------------------------------------------------------------ subscription

    describe('subscription and options', function () {
        it('subscribes to a topic list with fromBeginning and a stable default group id', async function () {
            const { k1, consumer } = await loadConnected({ topics: ' orders, payments\n audit,orders ', fromBeginning: true });
            assert.equal(fake.consumers.length, 1);
            assert.equal(fake.instances.length, 1);
            assert.deepEqual(consumer.subscriptions, [{
                topics: ['orders', 'payments', 'audit'], fromBeginning: true, resolved: ['orders', 'payments', 'audit']
            }]);
            assert.equal(consumer.groupId, 'node-red-k1');
            assert.equal(k1.consumerRuntime.cfg.groupId, 'node-red-k1');
            const status = await nextStatus(k1, isConnected);
            assert.equal(status.text, 'connected · orders, payments, audit');

            // kafkajs must never restart a consumer itself: an ASYNC function returning false
            const restart = consumer.options.retry.restartOnFailure;
            assert.equal(restart.constructor.name, 'AsyncFunction');
            assert.equal(await restart(new Error('x')), false);
            // the client retry settings of the broker node are inherited
            assert.equal(consumer.retry.retries, 5);

            // a redeploy (new node instance, same id) uses the same group id
            await helper.unload();
            await helper.load([brokerNode, consumerNode], flow({ topics: 'orders' }));
            await waitFor(() => fake.consumers.length === 2);
            assert.equal(fake.consumers[1].groupId, 'node-red-k1');
        });

        it('subscribes with a regular expression (flags kept, g/y dropped)', async function () {
            fake.behaviour.clusterTopics = ['test-topic', 'TEST-other', 'prod-x'];
            const { k1, consumer } = await loadConnected({ topics: '/^test-.*$/gi', topicIsRegex: true });
            const sub = consumer.subscriptions[0];
            assert.equal(sub.topics.length, 1);
            assert.ok(sub.topics[0] instanceof RegExp);
            assert.equal(sub.topics[0].source, '^test-.*$');
            assert.equal(sub.topics[0].flags, 'i');
            assert.deepEqual(sub.resolved, ['test-topic', 'TEST-other']);
            assert.equal(sub.fromBeginning, false);
            assert.equal((await nextStatus(k1, isConnected)).text, 'connected · /^test-.*$/i');
        });

        it('accepts a plain regular expression source', async function () {
            fake.behaviour.clusterTopics = ['a.1', 'a.2', 'b.1'];
            const { consumer } = await loadConnected({ topics: '^a\\.', topicIsRegex: 'true' });
            assert.deepEqual(consumer.subscriptions[0].resolved, ['a.1', 'a.2']);
        });

        it('passes a custom group id and coerces numeric strings to numbers (clamped)', async function () {
            const { consumer } = await loadConnected({
                groupId: ' my-group ',
                sessionTimeout: '20000',
                heartbeatInterval: '2000',
                rebalanceTimeout: '40000',
                maxBytesPerPartition: '2048',
                maxBytes: '4096',
                maxWaitTimeInMs: '100',
                partitionsConsumedConcurrently: '500',
                autoCommitInterval: '1000',
                autoCommitThreshold: '50',
                allowAutoTopicCreation: 'true'
            });
            const o = consumer.options;
            assert.equal(o.groupId, 'my-group');
            assert.equal(o.sessionTimeout, 20000);
            assert.equal(o.heartbeatInterval, 2000);
            assert.equal(o.rebalanceTimeout, 40000);
            assert.equal(o.maxBytesPerPartition, 2048);
            assert.equal(o.maxBytes, 4096);
            assert.equal(o.maxWaitTimeInMs, 100);
            assert.equal(o.allowAutoTopicCreation, true);
            const r = consumer.runConfig;
            assert.equal(r.autoCommit, true);
            assert.equal(r.autoCommitInterval, 1000);
            assert.equal(r.autoCommitThreshold, 50);
            assert.equal(r.partitionsConsumedConcurrently, 100);
            assert.equal(typeof r.eachMessage, 'function');
        });

        it('uses the defaults for blank or invalid numbers and leaves 0 commit settings to kafkajs', async function () {
            const { consumer } = await loadConnected({
                sessionTimeout: '', heartbeatInterval: 'abc', autoCommitInterval: '0', autoCommitThreshold: '0',
                commitMode: 'manual'
            });
            assert.equal(consumer.options.sessionTimeout, 30000);
            assert.equal(consumer.options.heartbeatInterval, 3000);
            assert.equal(consumer.options.rebalanceTimeout, 60000);
            assert.equal(consumer.options.maxBytesPerPartition, 1048576);
            assert.equal(consumer.options.maxBytes, 10485760);
            assert.equal(consumer.options.maxWaitTimeInMs, 1000); // not kafkajs' 5000: a deploy waits for the fetch in flight
            assert.equal(consumer.options.allowAutoTopicCreation, false);
            assert.equal(consumer.runConfig.autoCommit, false);
            assert.equal(consumer.runConfig.autoCommitInterval, undefined);
            assert.equal(consumer.runConfig.autoCommitThreshold, undefined);
            assert.equal(consumer.runConfig.partitionsConsumedConcurrently, 1);
        });

        it('corrects heartbeatInterval >= sessionTimeout (kafkajs would throw) and warns', async function () {
            const { k1, consumer } = await loadConnected({ sessionTimeout: '9000', heartbeatInterval: '10000' });
            assert.equal(consumer.options.sessionTimeout, 9000);
            assert.equal(consumer.options.heartbeatInterval, 3000);
            const warnings = callsOf(k1, 'warn');
            assert.ok(warnings.some((w) => /heartbeat interval \(10000 ms\) must be lower than the session timeout \(9000 ms\): using 3000 ms/.test(w)),
                warnings.join('\n'));
        });

        it('does not start with autoStart off until msg.action "start"', async function () {
            await helper.load([brokerNode, consumerNode], flow({ autoStart: false }));
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.text === 'stopped');
            assert.equal(s.fill, 'grey');
            await sleep(150);
            assert.equal(fake.consumers.length, 0);
            await expectOk({ action: 'start' });
            await nextStatus(k1, isConnected);
            assert.equal(fake.consumers.length, 1);
        });
    });

    describe('configuration problems (never crash, never connect)', function () {
        const cases = [
            ['no topics', { topics: '  ' }, 'no topic', /No topic configured/],
            ['an invalid regular expression', { topics: '([', topicIsRegex: true }, 'invalid topic regex', /Invalid topic regular expression/],
            ['an invalid topic name', { topics: 'ok, sensors/temp' }, 'config error', /Invalid topic name "sensors\/temp"/],
            ['an unset environment variable', { topics: '${KAFKAJS_NC_TEST_UNSET_TOPIC}' }, 'config error',
                /environment variable KAFKAJS_NC_TEST_UNSET_TOPIC is not set/],
            ['an unset environment variable as group id', { groupId: '${KAFKAJS_NC_TEST_UNSET_GROUP}' }, 'config error',
                /Group ID: environment variable KAFKAJS_NC_TEST_UNSET_GROUP is not set/],
            // Node-RED only replaces a reference that is the whole value: a set variable stays literal here,
            // so "is not set" would send the user after the wrong problem
            ['an environment variable inside a topic list', { topics: 'orders, ${KAFKAJS_NC_TEST_SET}' }, 'config error',
                /Topic: \$\{KAFKAJS_NC_TEST_SET\} is only replaced by Node-RED when it is the whole value/],
            ['an environment variable with a space around it', { topics: ' ${KAFKAJS_NC_TEST_SET}' }, 'config error',
                /Topic: \$\{KAFKAJS_NC_TEST_SET\} is only replaced by Node-RED when it is the whole value/],
            ['an environment variable with a space around it as regex', { topics: '${KAFKAJS_NC_TEST_SET} ', topicIsRegex: true },
                'config error', /Topic: \$\{KAFKAJS_NC_TEST_SET\} is only replaced by Node-RED when it is the whole value/],
            ['an environment variable inside the group id', { groupId: 'g-${KAFKAJS_NC_TEST_SET}' }, 'config error',
                /Group ID: \$\{KAFKAJS_NC_TEST_SET\} is only replaced by Node-RED when it is the whole value/]
        ];
        before(function () {
            process.env.KAFKAJS_NC_TEST_SET = 'set-topic';
        });
        after(function () {
            delete process.env.KAFKAJS_NC_TEST_SET;
        });
        for (const [what, props, statusText, logRe] of cases) {
            it(`reports ${what}`, async function () {
                await helper.load([brokerNode, consumerNode], flow(props));
                const k1 = helper.getNode('k1');
                const s = await nextStatus(k1, (st) => st.fill === 'red');
                assert.equal(s.text, statusText);
                assert.equal(s.shape, 'ring');
                await waitFor(() => callsOf(k1, 'error').some((e) => logRe.test(e)));
                await sleep(50);
                assert.equal(fake.consumers.length, 0);
                await expectError({ action: 'start' }, /Invalid Kafka consumer configuration/);
            });
        }

        it('missing broker config: red status, error log, control messages fail, nothing created', async function () {
            await helper.load([brokerNode, consumerNode], flow({ broker: 'does-not-exist' }));
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            assert.equal(s.text, 'no broker config');
            await waitFor(() => callsOf(k1, 'error').includes('No Kafka broker configuration selected'));
            await expectError({ action: 'pause' }, /No Kafka broker configuration selected/);
            await expectError({ action: 'start' }, /No Kafka broker configuration selected/);
            assert.equal(fake.instances.length, 0);
            assert.equal(fake.consumers.length, 0);
        });

        it('invalid broker config (SASL without password): red status, no client', async function () {
            await helper.load([brokerNode, consumerNode], flow({}, { authMechanism: 'plain' }), { b1: { username: 'alice' } });
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            assert.equal(s.text, 'config error');
            await waitFor(() => callsOf(k1, 'error').some((e) => /Invalid Kafka broker configuration: .*SASL password is required/.test(e)));
            await sleep(50);
            assert.equal(fake.instances.length, 0);
        });
    });

    // ------------------------------------------------------------------ output

    describe('output messages', function () {
        it('string formats: payload, key, headers (arrays, null), offset string, timestamp number, msg.kafka', async function () {
            const { consumer } = await loadConnected();
            const out = collect('out');
            // the fake wraps every delivery in START_BATCH_PROCESS (highWatermark from the batch), like kafkajs
            assert.equal(await consumer.deliver({
                topic: 'test-topic',
                highWatermark: '50',
                message: {
                    key: 'k-1', value: 'héllo', offset: '41', timestamp: '1700000000000',
                    headers: { a: 'x', multi: ['1', Buffer.from('2'), null], empty: null }
                }
            }), true);
            await waitFor(() => out.length === 1);
            const m = out[0];
            assert.equal(m.payload, 'héllo');
            assert.equal(m.topic, 'test-topic');
            assert.equal(m.key, 'k-1');
            assert.equal(m.partition, 0);
            assert.equal(m.offset, '41');
            assert.equal(m.timestamp, 1700000000000);
            assert.deepEqual(m.headers, { a: 'x', multi: ['1', '2', null], empty: null });
            assert.deepEqual(m.kafka, {
                groupId: 'node-red-k1', topic: 'test-topic', partition: 0, offset: '41', timestamp: 1700000000000,
                highWatermark: '50'
            });
            // a message without key/headers
            await consumer.deliver({ topic: 'test-topic', message: { value: 'second' } });
            await waitFor(() => out.length === 2);
            assert.equal(out[1].key, null);
            assert.deepEqual(out[1].headers, {});
            assert.equal(out[1].offset, '42');
            assert.equal(out[1].kafka.highWatermark, '43');
            assert.equal(typeof out[1].timestamp, 'number');
        });

        it('buffer formats: private copies of the payload, key and header buffers', async function () {
            const { consumer } = await loadConnected({ payloadFormat: 'buffer', keyFormat: 'buffer', headersFormat: 'buffer' });
            const out = collect('out');
            const value = Buffer.from([0, 1, 2, 255]);
            await consumer.deliver({
                topic: 'test-topic',
                message: { key: Buffer.from('key'), value, headers: { h: 'v', list: ['a', null], n: null } }
            });
            await waitFor(() => out.length === 1);
            const m = out[0];
            assert.ok(Buffer.isBuffer(m.payload));
            assert.deepEqual([...m.payload], [0, 1, 2, 255]);
            assert.notEqual(m.payload, value, 'payload must be a copy, not a slice of the fetch buffer');
            assert.ok(Buffer.isBuffer(m.key));
            assert.equal(m.key.toString(), 'key');
            assert.ok(Buffer.isBuffer(m.headers.h));
            assert.equal(m.headers.h.toString(), 'v');
            assert.ok(Buffer.isBuffer(m.headers.list[0]));
            assert.equal(m.headers.list[1], null);
            assert.equal(m.headers.n, null);
        });

        it('json format: parsed payload', async function () {
            const { consumer } = await loadConnected({ payloadFormat: 'json' });
            const out = collect('out');
            await consumer.deliver({ topic: 'test-topic', message: { value: '{"a":1,"b":[true,null]}' } });
            await consumer.deliver({ topic: 'test-topic', message: { value: '42' } });
            await waitFor(() => out.length === 2);
            assert.deepEqual(out[0].payload, { a: 1, b: [true, null] });
            assert.equal(out[1].payload, 42);
        });

        for (const format of ['string', 'buffer', 'json']) {
            it(`tombstone (null value) => payload null (${format})`, async function () {
                const { consumer } = await loadConnected({ payloadFormat: format });
                const out = collect('out');
                await consumer.deliver({ topic: 'test-topic', message: { key: 'gone', value: null } });
                await waitFor(() => out.length === 1);
                assert.equal(out[0].payload, null);
                assert.equal(out[0].key, 'gone');
            });
        }

        it('invalid JSON goes to Catch with the raw text and the consumer keeps going', async function () {
            const { k1, consumer } = await loadConnected({ payloadFormat: 'json' });
            const out = collect('out');
            const caught = collect('caught');
            assert.equal(await consumer.deliver({ topic: 'test-topic', message: { key: 'k', value: 'not json {', offset: '7' } }), true);
            await waitFor(() => caught.length === 1);
            assert.equal(caught[0].payload, 'not json {');
            assert.equal(caught[0].topic, 'test-topic');
            assert.equal(caught[0].offset, '7');
            assert.equal(caught[0].key, 'k');
            assert.match(caught[0].error.message, /Invalid JSON in message test-topic\[0\] offset 7/);
            assert.equal(caught[0].error.source.id, 'k1');
            assert.equal(out.length, 0);
            // empty value: not JSON either
            await consumer.deliver({ topic: 'test-topic', message: { value: '' } });
            await waitFor(() => caught.length === 2);
            // still consuming
            await consumer.deliver({ topic: 'test-topic', message: { value: '{"ok":true}' } });
            await waitFor(() => out.length === 1);
            assert.deepEqual(out[0].payload, { ok: true });
            assert.equal(consumer.eachMessageErrors.length, 0);
            assert.equal(consumer.running, true);
            assert.equal(consumer.crashes.length, 0);
            assert.equal(k1.consumerRuntime.state, 'connected');
        });

        it('a throwing node.send never escapes eachMessage (reported to Catch instead)', async function () {
            const { k1, consumer } = await loadConnected();
            const out = collect('out');
            const caught = collect('caught');
            k1.send = () => {
                throw new Error('downstream boom');
            };
            try {
                assert.equal(await consumer.deliver({ topic: 'test-topic', message: { value: 'v1' } }), true);
            } finally {
                delete k1.send; // back to the prototype method
            }
            await waitFor(() => caught.length === 1);
            assert.match(caught[0].error.message, /downstream boom/);
            assert.equal(caught[0].payload, 'v1');
            assert.equal(consumer.eachMessageErrors.length, 0);
            await consumer.deliver({ topic: 'test-topic', message: { value: 'v2' } });
            await waitFor(() => out.length === 1);
            assert.equal(out[0].payload, 'v2');
        });

        it('eachMessage resolves even for malformed input from kafkajs', async function () {
            const { consumer } = await loadConnected({ payloadFormat: 'json', headersFormat: 'buffer' });
            const each = consumer.runConfig.eachMessage;
            await each(undefined);
            await each({});
            await each({ topic: 'test-topic', partition: 0, message: null });
            await each({ topic: 'test-topic', partition: 0, message: { value: 12, headers: 'nope', key: {}, offset: {} } });
            const caught = collect('caught');
            await each({ topic: 'test-topic', partition: 0, message: { get value() { throw new Error('getter boom'); } } });
            await waitFor(() => caught.some((m) => /getter boom/.test(m.error.message)));
            assert.equal(consumer.running, true);
        });

        it('does not send after the node is closed', async function () {
            const { k1, consumer } = await loadConnected();
            const each = consumer.runConfig.eachMessage;
            await helper.unload();
            let sent = 0;
            k1.send = () => {
                sent++;
            };
            await each({ topic: 'test-topic', partition: 0, message: { value: Buffer.from('late'), offset: '1' } });
            assert.equal(sent, 0);
        });
    });

    // ------------------------------------------------------------------ supervisor

    describe('connection supervisor', function () {
        it('connect failure at startup: retries with backoff until it succeeds, without a redeploy', async function () {
            fake.behaviour.consumerConnectError = fake.errors.connectionRefused('127.0.0.1:9092');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const waiting = await nextStatus(k1, (s) => s.fill === 'yellow' && / · retry /.test(s.text));
            assert.match(waiting.text, /^ECONNREFUSED 127\.0\.0\.1:9092 · retry 0\.\ds$/);
            await waitFor(() => fake.consumers.length >= 3, { timeout: 3000 });
            // every attempt uses a brand-new client; failed consumers are disconnected
            assert.equal(fake.instances.length, fake.consumers.length);
            await waitFor(() => fake.consumers.slice(0, -1).every((c) => c.disconnectCount >= 1));
            // the first failure is an error, repeats are debug only
            const errors = callsOf(k1, 'error').filter((e) => /Kafka connection failed/.test(e));
            assert.equal(errors.length, 1, errors.join('\n'));
            assert.match(errors[0], /ECONNREFUSED.*retrying in/);
            assert.ok(callsOf(k1, 'debug').some((d) => /Kafka connection failed/.test(d)));

            delete fake.behaviour.consumerConnectError; // the broker is back
            await nextStatus(k1, isConnected, 3000);
            const last = fake.consumers[fake.consumers.length - 1];
            assert.equal(last.running, true);
            assert.ok(callsOf(k1, 'log').some((l) => /^Connected to Kafka after \d+ attempts$/.test(l)));
            const created = fake.consumers.length;
            await sleep(300);
            assert.equal(fake.consumers.length, created, 'no more attempts once connected');
        });

        it('backoff grows between attempts (bounded by reconnectMax)', async function () {
            fake.behaviour.consumerConnectError = fake.errors.connectionRefused();
            await helper.load([brokerNode, consumerNode], flow({}, { reconnectMin: '100', reconnectMax: '400' }));
            const k1 = helper.getNode('k1');
            const delays = [];
            await waitFor(() => {
                const d = k1.consumerRuntime.retryInMs;
                if (d && delays[delays.length - 1] !== d) delays.push(d);
                return fake.consumers.length >= 4;
            }, { timeout: 4000, interval: 5 });
            assert.ok(delays[0] >= 80 && delays[0] <= 120, `first delay ${delays[0]}`);
            assert.ok(delays.some((d) => d >= 300), `delays should grow: ${delays}`);
            assert.ok(delays.every((d) => d <= 400), `delays bounded: ${delays}`);
        });

        it('CRASH (restart=false): new consumer after backoff, old one disconnected, pause re-applied after GROUP_JOIN', async function () {
            const { k1, consumer: first } = await loadConnected();
            await expectOk({ action: 'pause' });
            assert.deepEqual(first.paused(), [{ topic: 'test-topic', partitions: [0] }]);
            assert.equal((await nextStatus(k1, (s) => /^paused/.test(s.text))).text, 'paused · test-topic');

            await first.crash(fake.errors.connectionRefused(), false);
            assert.equal(first.state, 'disconnected');
            await nextStatus(k1, (s) => s.fill === 'yellow' && / · retry /.test(s.text), { since: 0 });
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            const second = fake.consumers[1];
            assert.ok(first.disconnectCount >= 1);
            assert.equal(first.joinCount, 1, 'the old consumer is never revived');
            // kafkajs forgot the pause with the old consumer; the node re-applied it
            assert.deepEqual(second.pauses, [[{ topic: 'test-topic' }]]);
            assert.deepEqual(second.paused(), [{ topic: 'test-topic', partitions: [0] }]);
            assert.equal(await second.deliver({ topic: 'test-topic', message: { value: 'held' } }), false);
            assert.equal((await nextStatus(k1, (s) => s.fill === 'yellow' && s.shape === 'dot')).text, 'paused · test-topic');
            assert.ok(callsOf(k1, 'error').some((e) => /^Kafka consumer failed: .*ECONNREFUSED/.test(e)));

            const out = collect('out');
            await expectOk({ action: 'resume' });
            assert.deepEqual(second.resumes, [[{ topic: 'test-topic' }]]);
            assert.equal(await second.deliver({ topic: 'test-topic', message: { value: 'flows' } }), true);
            await waitFor(() => out.length === 1);
            assert.equal((await nextStatus(k1, isConnected)).text, 'connected · test-topic');
        });

        it('CRASH with restart=true: the kafkajs restart of the old consumer is cancelled (no zombie)', async function () {
            fake.behaviour.restartDelayMs = 50;
            const { consumer: first } = await loadConnected();
            await first.crash(fake.errors.connectionRefused(), true);
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            await sleep(250); // well past the kafkajs restart delay
            assert.equal(first.joinCount, 1);
            assert.equal(first.running, false);
            assert.equal(first.state, 'disconnected');
            assert.equal(fake.consumers.length, 2);
        });

        it('a stale consumer that joins its group anyway is disconnected (zombie killer)', async function () {
            const { k1, consumer: first } = await loadConnected();
            await expectOk({ action: 'restart' });
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            const before = first.disconnectCount;
            first.emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: 'node-red-k1', memberId: 'zombie', memberAssignment: { 'test-topic': [0] } });
            await waitFor(() => first.disconnectCount > before);
            assert.equal(k1.consumerRuntime.consumer, fake.consumers[1]);
            assert.equal(k1.consumerRuntime.state, 'connected');
        });

        it('a failure to join the group (CRASH from run()) is retried', async function () {
            let attempts = 0;
            fake.behaviour.groupJoinError = () => (++attempts <= 2 ? fake.errors.connectionRefused() : null);
            const { k1 } = await loadConnected();
            assert.equal(attempts, 3);
            assert.equal(fake.consumers.length, 3);
            assert.ok(fake.consumers.slice(0, 2).every((c) => c.state === 'disconnected' && c.joinCount === 0));
            assert.equal(k1.consumerRuntime.state, 'connected');
        });

        it('SASL failure: slow retry (authRetryMin) and an auth status', async function () {
            fake.behaviour.consumerConnectError = fake.errors.saslAuth('PLAIN');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            assert.equal(s.text, 'auth failed · retry 1s');
            assert.equal(s.shape, 'ring');
            await sleep(600); // the normal backoff (100 ms) would have retried several times by now
            assert.equal(fake.consumers.length, 1);
            await waitFor(() => fake.consumers.length === 2, { timeout: 2000 });
            assert.ok(callsOf(k1, 'error').some((e) => /Kafka connection failed: KafkaJSSASLAuthenticationError/.test(e)));
        });

        it('a missing topic on subscribe uses the normal backoff (it may be created soon) with a hint in the log', async function () {
            fake.behaviour.subscribeError = () => fake.errors.protocol('UNKNOWN_TOPIC_OR_PARTITION');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'yellow' && / · retry /.test(st.text));
            assert.match(s.text, /^unknown topic\/partition · retry /);
            // usually temporary (e.g. a producer is about to create it): a warning, not an error
            await waitFor(() => callsOf(k1, 'warn').some((e) => /Kafka connection failed: .*check that the topic exists/.test(e)));
            assert.deepEqual(callsOf(k1, 'error').filter((e) => /UNKNOWN_TOPIC|topic exists/.test(e)), []);
            assert.equal(fake.consumers[0].state, 'disconnected');
            // not the slow auth retry (authRetryMin is 1 s here): the next attempt comes quickly
            await waitFor(() => fake.consumers.length >= 2, { timeout: 700 });
            // once the topic exists the consumer joins without a redeploy
            fake.behaviour.subscribeError = null;
            await nextStatus(k1, (st) => st.fill === 'green', 3000);
        });

        it('group coordinator not found is retried with the normal backoff, with a hint naming both causes', async function () {
            // kafkajs reports an unavailable coordinator (broker starting, __consumer_offsets offline) and a
            // missing group ACL the same way: waiting authRetryMin would delay recovery from the first
            fake.behaviour.groupJoinError = () => fake.errors.groupCoordinatorNotFound('node-red-k1');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => / · retry /.test(st.text));
            assert.equal(s.fill, 'yellow');
            assert.match(s.text, /^group coordinator not found · retry /);
            assert.equal(k1.consumerRuntime.lastKind, 'transient');
            assert.ok(k1.consumerRuntime.retryInMs < 1000, `normal backoff, not authRetryMin: ${k1.consumerRuntime.retryInMs}`);
            await waitFor(() => callsOf(k1, 'error').some((e) =>
                /group coordinator is not available .*or the user is not authorised for the consumer group/.test(e)));
            // and it recovers at the next retry once the coordinator is there
            await waitFor(() => fake.consumers.length >= 2, { timeout: 700 });
            fake.behaviour.groupJoinError = null;
            await nextStatus(k1, isConnected, 3000);
        });

        it('a red status shows the whole reason when it fits (not cut at 28 characters, as the producer)', async function () {
            fake.behaviour.consumerConnectError = () => fake.errors.protocol('ILLEGAL_SASL_STATE');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            assert.equal(s.text, 'SASL not expected by listener · retry 1s');
        });

        it('10 identical connection failures: one error, then a "still failing" warning (MAINT-10)', async function () {
            fake.behaviour.consumerConnectError = () => fake.errors.connectionRefused();
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            await waitFor(() => fake.consumers.length >= 11, { timeout: 5000, message: '11 attempts' });
            assert.equal(callsOf(k1, 'error').filter((e) => /Kafka connection failed/.test(e)).length, 1);
            assert.equal(callsOf(k1, 'warn').filter((e) => /Kafka still failing after 10 attempts: .*ECONNREFUSED/.test(e)).length, 1);
        });

        it('kafka.consumer() throwing synchronously is a config error, retried slowly', async function () {
            fake.behaviour.consumerFactoryError = new TypeError("'timeout' is not a number, received 'string'");
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            // same status as the producer and the admin node for a factory throw
            assert.equal(s.text, 'config error · retry 1s');
            assert.equal(fake.consumers.length, 0);
            assert.equal(fake.instances.length, 1);
            assert.ok(callsOf(k1, 'error').some((e) => /Could not create the Kafka consumer: 'timeout' is not a number/.test(e)));
        });

        it('a protocol error about message content while joining is retried with the normal backoff (not as config)', async function () {
            fake.behaviour.groupJoinError = () => fake.errors.protocol('CORRUPT_MESSAGE');
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => / · retry /.test(st.text));
            assert.equal(s.fill, 'yellow');
            assert.equal(k1.consumerRuntime.lastKind, 'unknown');
            assert.ok(k1.consumerRuntime.retryInMs < 1000, 'normal backoff, not the auth/config minimum');
        });

        it('a codec error from the running consumer (e.g. zstd) is reported with a hint', async function () {
            const { k1, consumer } = await loadConnected();
            await consumer.crash(fake.errors.notImplemented('ZSTD'), false);
            const s = await nextStatus(k1, (st) => st.fill === 'red');
            assert.equal(s.text, 'compression codec missing · retry 1s');
            assert.ok(callsOf(k1, 'error').some((e) => /zstd is not supported/.test(e)));
        });

        it('a consumer that crashes right after joining keeps backing off (no tight loop)', async function () {
            const { k1 } = await loadConnected();
            for (let i = 0; i < 3; i++) {
                const c = fake.consumers[fake.consumers.length - 1];
                await c.crash(fake.errors.connectionRefused(), false);
                await waitFor(() => fake.consumers.length === i + 2 && fake.consumers[i + 1].running);
            }
            const rt = k1.consumerRuntime;
            assert.equal(rt.failures, 3);
            assert.ok(rt.retryInMs >= 160, `backoff should have grown: ${rt.retryInMs}`);
            // only the first crash is logged as an error, and recovery is logged once
            assert.equal(callsOf(k1, 'error').filter((e) => /Kafka consumer failed/.test(e)).length, 1);
            assert.equal(callsOf(k1, 'log').filter((l) => /Connected to Kafka after/.test(l)).length, 1);
        });

        it('a consumer that stayed joined long enough starts the next outage from the minimum backoff', async function () {
            consumerNode.TIMEOUTS.stable = 50;
            const { k1 } = await loadConnected();
            await fake.consumers[0].crash(fake.errors.connectionRefused(), false);
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            await sleep(80);
            await fake.consumers[1].crash(fake.errors.connectionRefused(), false);
            const rt = k1.consumerRuntime;
            assert.equal(rt.failures, 1);
            assert.ok(rt.retryInMs <= 120, `backoff restarted: ${rt.retryInMs}`);
            assert.equal(callsOf(k1, 'error').filter((e) => /Kafka consumer failed/.test(e)).length, 2);
        });

        it('rebalancing shows a status and the next GROUP_JOIN shows connected again', async function () {
            const { k1, consumer } = await loadConnected();
            consumer.emit(CONSUMER_EVENTS.REBALANCING, { groupId: 'node-red-k1', memberId: 'm' });
            assert.equal((await nextStatus(k1, (s) => s.text === 'rebalancing…')).fill, 'yellow');
            consumer.emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: 'node-red-k1', memberId: 'm', memberAssignment: { 'test-topic': [0] } });
            await nextStatus(k1, isConnected);
        });

        it('a topic regex that matches no topic: yellow "no partitions" status and one warning, not a green status', async function () {
            fake.behaviour.clusterTopics = ['unrelated'];
            await helper.load([brokerNode, consumerNode], flow({ topics: '^orders-', topicIsRegex: true }));
            const k1 = helper.getNode('k1');
            await waitFor(() => fake.consumers.length === 1 && fake.consumers[0].running);
            const s = await nextStatus(k1, (st) => st.shape === 'dot');
            assert.deepEqual([s.fill, s.text], ['yellow', 'connected · no partitions']);
            const warned = () => callsOf(k1, 'warn').filter((w) => /No partition is assigned to this consumer: the topic pattern \/\^orders-\/ matches no topic/.test(w));
            await waitFor(() => warned().length === 1);
            assert.match(warned()[0], /only picked up after a restart \(msg\.action "restart"\) or a redeploy/);
            // logged once per run of empty assignments, not at every rejoin
            fake.consumers[0].emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: 'node-red-k1', memberId: 'm', memberAssignment: {} });
            await sleep(50);
            assert.equal(warned().length, 1);
            // a restart after the topic was created picks it up
            fake.behaviour.clusterTopics = ['unrelated', 'orders-eu'];
            await expectOk({ action: 'restart' });
            await nextStatus(k1, (st) => isConnected(st) && st.text === 'connected · /^orders-/', 3000);
        });

        it('a list subscription without partitions (a standby member) shows it in the status, without a warning', async function () {
            fake.behaviour.partitionCount = 0;
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            const s = await nextStatus(k1, (st) => st.shape === 'dot');
            assert.deepEqual([s.fill, s.text], ['yellow', 'connected · no partitions']);
            await sleep(50);
            assert.deepEqual(callsOf(k1, 'warn'), []);
            fake.consumers[0].emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: 'node-red-k1', memberId: 'm', memberAssignment: { 'test-topic': [0] } });
            await nextStatus(k1, (st) => isConnected(st) && st.text === 'connected · test-topic');
        });
    });

    // ------------------------------------------------------------------ control

    describe('control actions', function () {
        it('a done(err) that throws (Catch cannot clone the msg) is logged, never thrown', async function () {
            const { k1 } = await loadConnected();
            const msg = { action: 'bogus' };
            // Node-RED deep-clones msg for Catch nodes; a throwing getter makes that clone throw
            Object.defineProperty(msg, 'evil', { enumerable: true, get() { throw new Error('getter boom'); } });
            assert.doesNotThrow(() => k1.receive(msg));
            await waitFor(() => callsOf(k1, 'error').some((e) => /Could not complete the message: Unknown msg.action "bogus".*getter boom/.test(e)),
                { message: `logged: ${JSON.stringify(callsOf(k1, 'error'))}` });
        });

        it('pause and resume while running', async function () {
            const { k1, consumer } = await loadConnected({ topics: 'test-topic, other' });
            const out = collect('out');
            await expectOk({ action: 'pause' });
            assert.deepEqual(consumer.pauses, [[{ topic: 'test-topic' }, { topic: 'other' }]]);
            assert.equal(await consumer.deliver({ topic: 'other', message: { value: 'x' } }), false);
            assert.equal(k1.consumerRuntime.pauseWanted, true);
            await expectOk({ action: ' Resume ' }); // trimmed, case-insensitive
            assert.deepEqual(consumer.resumes, [[{ topic: 'test-topic' }, { topic: 'other' }]]);
            assert.equal(await consumer.deliver({ topic: 'other', message: { value: 'y' } }), true);
            await waitFor(() => out.length === 1);
        });

        it('pause before the consumer runs is remembered and applied after GROUP_JOIN', async function () {
            fake.behaviour.connectDelayMs = 150;
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            await waitFor(() => fake.consumers.length === 1);
            assert.equal(k1.consumerRuntime.state, 'connecting');
            await expectOk({ action: 'pause' });
            assert.equal(fake.consumers[0].pauses.length, 0);
            await waitFor(() => fake.consumers[0].running);
            assert.deepEqual(fake.consumers[0].pauses, [[{ topic: 'test-topic' }]]);
            assert.equal(await fake.consumers[0].deliver({ topic: 'test-topic', message: { value: 'held' } }), false);
            assert.equal((await nextStatus(k1, (s) => s.shape === 'dot')).text, 'paused · test-topic');
        });

        it('pause with a regex subscription uses the topics of the assignment and pauses new ones as they appear', async function () {
            fake.behaviour.clusterTopics = ['t-a', 't-b'];
            const { consumer } = await loadConnected({ topics: '^t-', topicIsRegex: true });
            await expectOk({ action: 'pause' });
            assert.deepEqual(consumer.pauses, [[{ topic: 't-a' }, { topic: 't-b' }]]);
            // a message of a topic not seen before (e.g. after a metadata change) pauses it as well
            await consumer.runConfig.eachMessage({ topic: 't-new', partition: 0, message: { value: Buffer.from('x'), offset: '0' } });
            assert.deepEqual(consumer.pauses[1].map((tp) => tp.topic), ['t-a', 't-b', 't-new']);
        });

        it('stop disconnects and does not reconnect; start connects again', async function () {
            const { k1, consumer } = await loadConnected();
            await expectOk({ action: 'stop' });
            assert.equal(consumer.state, 'disconnected');
            const s = await nextStatus(k1, (st) => st.text === 'stopped');
            assert.equal(s.fill, 'grey');
            assert.equal(s.shape, 'ring');
            // a crash event of the stopped consumer changes nothing
            await consumer.crash(fake.errors.connectionRefused(), false);
            await sleep(300);
            assert.equal(fake.consumers.length, 1);
            assert.equal(k1.consumerRuntime.state, 'stopped');
            await expectError({ action: 'seek', payload: { partition: 0, offset: 1 } }, /Cannot seek: the Kafka consumer is not running \(stopped\)/);
            await expectOk({ action: 'stop' }); // idempotent
            await expectOk({ action: 'start' });
            await nextStatus(k1, isConnected);
            assert.equal(fake.consumers.length, 2);
            await expectOk({ action: 'start' }); // already running: no-op
            await sleep(100);
            assert.equal(fake.consumers.length, 2);
        });

        it('stop while waiting to reconnect cancels the retry', async function () {
            fake.behaviour.consumerConnectError = fake.errors.connectionRefused();
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            await nextStatus(k1, (s) => / · retry /.test(s.text));
            await expectOk({ action: 'stop' });
            const n = fake.consumers.length;
            await sleep(350);
            assert.equal(fake.consumers.length, n);
            delete fake.behaviour.consumerConnectError;
            await expectOk({ action: 'start' });
            await nextStatus(k1, isConnected);
        });

        it('restart: the old consumer leaves first, then a new one joins', async function () {
            const { k1, consumer } = await loadConnected();
            const mark = statusMark(k1);
            await expectOk({ action: 'restart' });
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            assert.equal(consumer.state, 'disconnected');
            assert.ok(fake.consumers[1].connectCount >= 1);
            await nextStatus(k1, isConnected, { since: mark });
        });

        it('restart from the stopped state starts the consumer', async function () {
            const { k1 } = await loadConnected();
            await expectOk({ action: 'stop' });
            await expectOk({ action: 'restart' });
            await nextStatus(k1, isConnected);
            assert.equal(k1.consumerRuntime.stopped, false);
        });

        it('seek: explicit or default topic, numeric/string offsets; earliest/latest become real offsets', async function () {
            const { consumer } = await loadConnected();
            fake.behaviour.adminResults = { fetchTopicOffsets: [{ partition: 0, offset: '10', high: '10', low: '3' }] };
            await expectOk({ action: 'seek', payload: { partition: 0, offset: 5 } });
            await expectOk({ action: 'seek', payload: { topic: 'test-topic', partition: '0', offset: '10' } });
            // kafkajs does not seek to its -2/-1 markers (it applies fromBeginning instead): the node resolves them
            await expectOk({ action: 'seek', payload: { partition: 0, offset: 'earliest' } });
            await expectOk({ action: 'seek', payload: { partition: 0, offset: ' Latest ' } });
            await expectOk({ action: 'seek', payload: { partition: 0, offset: -2 } });
            assert.deepEqual(consumer.seeks, [
                { topic: 'test-topic', partition: 0, offset: '5' },
                { topic: 'test-topic', partition: 0, offset: '10' },
                { topic: 'test-topic', partition: 0, offset: '3' },
                { topic: 'test-topic', partition: 0, offset: '10' },
                { topic: 'test-topic', partition: 0, offset: '3' }
            ]);
            // one short-lived admin client per seek, always disconnected again
            assert.equal(fake.admins.length, 5);
            for (const admin of fake.admins) {
                assert.deepEqual(admin.calls, [{ method: 'fetchTopicOffsets', args: 'test-topic' }]);
                await waitFor(() => admin.state === 'disconnected');
            }
        });

        it('seek: an offset outside the partition\'s offsets is refused (kafkajs would commit its default position)', async function () {
            const { consumer } = await loadConnected({ commitMode: 'manual' });
            fake.behaviour.adminResults = { fetchTopicOffsets: [{ partition: 0, offset: '10', high: '10', low: '3' }] };
            await expectError({ action: 'seek', payload: { partition: 0, offset: 11 } },
                /seek: offset 11 is outside the offsets of test-topic\[0\] \(3 to 10\)/);
            await expectError({ action: 'seek', payload: { partition: 0, offset: '2' } }, /offset 2 is outside/);
            await expectError({ action: 'seek', payload: { partition: 0, offset: '9223372036854775807' } }, /is outside/);
            assert.deepEqual(consumer.seeks, []);
        });

        it('seek: only for a partition assigned to this consumer, never while rebalancing', async function () {
            fake.behaviour.partitionCount = { 'test-topic': 2, other: 1 };
            const { k1, consumer } = await loadConnected({ topics: 'test-topic, other' });
            fake.behaviour.adminResults = { fetchTopicOffsets: [{ partition: 0, high: '4', low: '0' }, { partition: 1, high: '4', low: '0' }] };
            await expectOk({ action: 'seek', payload: { topic: 'test-topic', partition: 1, offset: 2 } });
            // a later rebalance gives this member only partition 0 of test-topic
            consumer.emit(CONSUMER_EVENTS.REBALANCING, { groupId: 'node-red-k1', memberId: 'm' });
            await nextStatus(k1, (s) => s.text === 'rebalancing…');
            await expectError({ action: 'seek', payload: { topic: 'test-topic', partition: 0, offset: 1 } }, /Cannot seek: the consumer group is rebalancing/);
            consumer.emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: 'node-red-k1', memberId: 'm', memberAssignment: { 'test-topic': [0], other: [] } });
            await nextStatus(k1, isConnected);
            // kafkajs would keep this seek and apply it whenever partition 1 is assigned again (maybe hours later)
            const e = await expectError({ action: 'seek', payload: { topic: 'test-topic', partition: 1, offset: 0 } },
                /seek: partition 1 of topic "test-topic" is not assigned to this consumer \(assigned: test-topic\[0\]\)/);
            assert.equal(e.error.source.id, 'k1');
            await expectError({ action: 'seek', payload: { topic: 'nope', partition: 0, offset: 0 } }, /partition 0 of topic "nope" is not assigned/);
            await expectOk({ action: 'seek', payload: { topic: 'test-topic', partition: 0, offset: 'latest' } });
            assert.deepEqual(consumer.seeks, [
                { topic: 'test-topic', partition: 1, offset: '2' },
                { topic: 'test-topic', partition: 0, offset: '4' }
            ]);
            assert.equal(fake.admins.length, 2, 'refused seeks never connect');
        });

        it('seek: the offset lookup is bounded and its client is disconnected; a restart meanwhile fails the seek', async function () {
            consumerNode.TIMEOUTS.offsetLookup = 150;
            const { consumer } = await loadConnected();
            fake.behaviour.adminDelayMs = { fetchTopicOffsets: Infinity };
            await expectError({ action: 'seek', payload: { partition: 0, offset: 'earliest' } },
                /Cannot seek: looking up the offsets of topic "test-topic" timed out after 150ms/);
            await waitFor(() => fake.admins[0].state === 'disconnected');
            // a connect that completes after the timeout is disconnected again (no leftover connection)
            fake.behaviour.adminDelayMs = null;
            fake.behaviour.connectDelayMs = (args, client) => (client.kind === 'admin' ? 300 : 0);
            await expectError({ action: 'seek', payload: { partition: 0, offset: 'earliest' } }, /timed out after 150ms/);
            await waitFor(() => fake.admins[1].connectCount === 1 && fake.admins[1].state === 'disconnected', { timeout: 2000 });
            assert.equal(fake.admins[1].calls.length, 0);
            // the consumer restarts while the offsets are looked up: that seek must not reach the new consumer
            fake.behaviour.connectDelayMs = (args, client) => (client.kind === 'admin' ? 100 : 0);
            consumerNode.TIMEOUTS.offsetLookup = 2000;
            const pending = control({ action: 'seek', payload: { partition: 0, offset: 'earliest' } });
            await waitFor(() => fake.admins.length === 3);
            await expectOk({ action: 'restart' });
            const r = await pending;
            assert.match(r.error.error.message, /Cannot seek: the Kafka consumer restarted while the offsets were looked up/);
            assert.deepEqual(consumer.seeks, []);
            await waitFor(() => fake.consumers.length === 2 && fake.consumers[1].running);
            assert.deepEqual(fake.consumers[1].seeks, []);
        });

        it('seek: closing the node during the offset lookup disconnects the lookup client', async function () {
            await loadConnected();
            fake.behaviour.adminDelayMs = { fetchTopicOffsets: Infinity };
            helper.getNode('k1').receive({ action: 'seek', payload: { partition: 0, offset: 'latest' } });
            await waitFor(() => fake.admins.length === 1 && fake.admins[0].calls.length === 1);
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started < 1000);
            assert.equal(fake.admins[0].state, 'disconnected');
        });

        it('seek: invalid parameters fail with a helpful error', async function () {
            await loadConnected({ topics: 'a, b' });
            await expectError({ action: 'seek', payload: { partition: 0, offset: 1 } }, /msg.payload.topic is required because the node subscribes to several topics/);
            await expectError({ action: 'seek', payload: 'x' }, /seek: msg.payload must be an object/);
            await expectError({ action: 'seek', payload: { topic: 'a', offset: 1 } }, /partition is required/);
            await expectError({ action: 'seek', payload: { topic: 'a', partition: -1, offset: 1 } }, /partition must be a whole number/);
            await expectError({ action: 'seek', payload: { topic: 'a', partition: 0, offset: -5 } }, /offset must be a whole number >= 0, "earliest" or "latest"/);
            await expectError({ action: 'seek', payload: { topic: 'a', partition: 0, offset: 'soon' } }, /offset must be/);
            await expectError({ action: 'seek', payload: { topic: 'a', partition: 0, offset: '9223372036854775808' } }, /offset must be/);
            assert.equal(fake.admins.length, 0, 'invalid requests never connect');
        });

        it('commit (manual mode) commits offset + 1 as a string, from msg.kafka or msg', async function () {
            const { consumer } = await loadConnected({ commitMode: 'manual' });
            assert.equal(consumer.runConfig.autoCommit, false);
            const out = collect('out');
            await consumer.deliver({ topic: 'test-topic', message: { value: 'job', offset: '41' } });
            await waitFor(() => out.length === 1);
            // the processed message is sent back with action "commit"
            await expectOk(Object.assign({}, out[0], { action: 'commit' }));
            const groupId = 'node-red-k1';
            await expectOk({ action: 'commit', kafka: { groupId, topic: 'test-topic', partition: 0, offset: '9007199254740993' } });
            await expectOk({ action: 'commit', topic: 'test-topic', partition: '0', offset: 7 });
            await expectOk({ action: 'commit', kafka: { groupId, partition: 0, offset: 1n } }); // topic defaults to the only one
            assert.deepEqual(consumer.committed, [
                { topic: 'test-topic', partition: 0, offset: '42', metadata: null },
                { topic: 'test-topic', partition: 0, offset: '9007199254740994', metadata: null },
                { topic: 'test-topic', partition: 0, offset: '8', metadata: null },
                { topic: 'test-topic', partition: 0, offset: '2', metadata: null }
            ]);
        });

        it('commit after a producer node (msg.kafka replaced by its delivery report) is refused, never committed', async function () {
            const { consumer } = await loadConnected({ commitMode: 'manual', topics: 'in' });
            const out = collect('out');
            await consumer.deliver({ topic: 'in', message: { value: 'job', offset: '41' } });
            await waitFor(() => out.length === 1);
            // what a kafkajs-nc-producer (topic "out") sends on: the input record's msg with msg.kafka = its report
            const afterProducer = Object.assign({}, out[0], {
                action: 'commit', kafka: { status: 'sent', topic: 'out', partition: 2, offset: '7' }
            });
            const e = await expectError(afterProducer, /commit: msg\.kafka is not a message of this consumer \(it has no groupId\)\. A producer node replaces msg\.kafka/);
            assert.equal(e.error.source.id, 'k1');
            // nor the fields of msg mixed with a partial msg.kafka (the old code took the topic from one, the offset from the other)
            await expectError({ action: 'commit', topic: 'in', partition: 0, offset: 41, kafka: { status: 'queued', topic: 'out' } }, /has no groupId/);
            await expectError({ action: 'commit', kafka: { groupId: 'someone-else', topic: 'in', partition: 0, offset: '41' } },
                /msg\.kafka is a message of consumer group "someone-else", not of this consumer's group "node-red-k1"/);
            await expectError({ action: 'commit', kafka: { groupId: 'node-red-k1', topic: 'out', partition: 0, offset: '41' } },
                /this consumer does not subscribe to topic "out"/);
            await expectError({ action: 'commit', topic: 'out', partition: 0, offset: 41 }, /does not subscribe to topic "out"/);
            assert.deepEqual(consumer.committed, []);
            // keeping a copy of the consumer's msg.kafka (as the help describes) works
            await expectOk(Object.assign({}, afterProducer, { kafka: out[0].kafka }));
            assert.deepEqual(consumer.committed, [{ topic: 'in', partition: 0, offset: '42', metadata: null }]);
        });

        it('commit: a regex subscription accepts the matching topics only', async function () {
            fake.behaviour.clusterTopics = ['t-a', 't-b', 'other'];
            await loadConnected({ commitMode: 'manual', topics: '^t-', topicIsRegex: true });
            await expectOk({ action: 'commit', kafka: { groupId: 'node-red-k1', topic: 't-b', partition: 0, offset: '3' } });
            await expectError({ action: 'commit', kafka: { groupId: 'node-red-k1', topic: 'other', partition: 0, offset: '3' } },
                /does not subscribe to topic "other"/);
        });

        it('commit fails when kafkajs skips it because its consumer is stopping (no false success)', async function () {
            const { consumer } = await loadConnected({ commitMode: 'manual' });
            // kafkajs runner.commitOffsets(): "consumer not running, exiting" resolves undefined without committing
            // while a crash or stop is tearing the consumer down (the node still says connected then)
            const real = consumer.commitOffsets;
            consumer.commitOffsets = async () => undefined;
            await expectError({ action: 'commit', kafka: { groupId: 'node-red-k1', topic: 'test-topic', partition: 0, offset: '5' } },
                /Commit not stored: the Kafka consumer was stopping/);
            consumer.commitOffsets = real;
            await expectOk({ action: 'commit', kafka: { groupId: 'node-red-k1', topic: 'test-topic', partition: 0, offset: '5' } });
            assert.equal(helper.getNode('k1').consumerRuntime.commitWaiters.size, 0, 'no waiter left behind');
        });

        it('commit failures and invalid commits go to Catch', async function () {
            const { consumer } = await loadConnected({ commitMode: 'manual' });
            const kafka = (props) => Object.assign({ groupId: 'node-red-k1' }, props);
            await expectError({ action: 'commit' }, /commit: msg\.partition is required/);
            await expectError({ action: 'commit', kafka: kafka({ partition: 0 }) }, /commit: msg\.kafka\.offset is required/);
            await expectError({ action: 'commit', kafka: kafka({ partition: 0, offset: 'x' }) }, /offset must be a whole number/);
            fake.behaviour.commitError = fake.errors.protocol('REBALANCE_IN_PROGRESS');
            await expectError({ action: 'commit', kafka: kafka({ partition: 0, offset: 3 }) }, /rebalancing/i);
            delete fake.behaviour.commitError;
            // commitOffsets() rejecting (not running in kafkajs terms) also ends in done(err)
            consumer.stop();
            await expectError({ action: 'commit', kafka: kafka({ partition: 0, offset: 3 }) }, /not initialized/);
            await expectOk({ action: 'stop' });
            await expectError({ action: 'commit', kafka: kafka({ partition: 0, offset: 3 }) }, /Cannot commit: the Kafka consumer is not running/);
        });

        it('a pause that kafkajs rejects while running fails the control message but stays wanted', async function () {
            const { k1, consumer } = await loadConnected();
            consumer.stop(); // kafkajs: "not initialized" from now on
            await expectError({ action: 'pause' }, /Consumer group was not initialized/);
            assert.equal(k1.consumerRuntime.pauseWanted, true);
        });

        it('unknown and missing actions fail with the list of valid actions', async function () {
            await loadConnected();
            const e1 = await expectError({ action: 'explode' }, /Unknown msg.action "explode"/);
            assert.match(e1.error.message, /pause, resume, stop, start, restart, seek, commit/);
            await expectError({ payload: 'just data' }, /msg.action is required/);
            await expectError({ action: 42 }, /Unknown msg.action 42/);
            await expectError({ action: '  ' }, /msg.action is required/);
            assert.equal(fake.consumers.length, 1);
        });
    });

    // ------------------------------------------------------------------ close

    describe('close', function () {
        it('close during connect: the late connect is disconnected and nothing else is created', async function () {
            fake.behaviour.connectDelayMs = 300;
            await helper.load([brokerNode, consumerNode], flow());
            await waitFor(() => fake.consumers.length === 1);
            const consumer = fake.consumers[0];
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started < 1000, 'close must not wait for the connect');
            await waitFor(() => consumer.connectCount === 1 && consumer.state === 'disconnected', { timeout: 2000 });
            await sleep(300);
            assert.equal(consumer.state, 'disconnected');
            assert.equal(consumer.subscriptions.length, 0);
            assert.equal(consumer.runCount, 0);
            assert.equal(fake.consumers.length, 1);
        });

        /** Make one step of the created consumer slow (the fake has no delay knob for it). */
        /** Load with a slow subscribe() or a slow group join (run()) and wait until the node is in that step. */
        async function slowStep(method, ms) {
            let called = false;
            const knob = method === 'subscribe' ? 'subscribeDelayMs' : 'runDelayMs';
            fake.behaviour[knob] = () => {
                called = true;
                return ms;
            };
            await helper.load([brokerNode, consumerNode], flow());
            await waitFor(() => called);
            assert.equal(fake.consumers.length, 1);
            return fake.consumers[0];
        }

        it('close during subscribe: the consumer is disconnected and never runs', async function () {
            const consumer = await slowStep('subscribe', 150);
            await helper.unload();
            await sleep(300);
            assert.equal(consumer.state, 'disconnected');
            assert.equal(consumer.runCount, 0);
            assert.equal(consumer.joinCount, 0);
            assert.equal(fake.consumers.length, 1);
        });

        it('close while joining: the disconnect waits for the join, then the member leaves (no zombie)', async function () {
            const consumer = await slowStep('run', 150);
            const runtime = helper.getNode('k1').consumerRuntime;
            assert.ok(runtime.runPending.has(consumer));
            const started = Date.now();
            const unloading = helper.unload(); // close waits (bounded) for the gated disconnect
            await sleep(60);
            // kafkajs would forget the not-yet-started runner if disconnected now
            assert.equal(consumer.disconnectCount, 0, 'no disconnect while run() is still joining');
            await unloading;
            assert.ok(Date.now() - started < 2000);
            assert.equal(consumer.joinCount, 1);
            await waitFor(() => consumer.state === 'disconnected' && !consumer.running);
            assert.ok(consumer.disconnectCount >= 1);
            await sleep(100);
            assert.equal(consumer.running, false);
            assert.equal(fake.consumers.length, 1);
        });

        it('close while joining is still bounded when the join takes long', async function () {
            consumerNode.TIMEOUTS.disconnect = 200;
            const consumer = await slowStep('run', 1000);
            const started = Date.now();
            await helper.unload();
            const took = Date.now() - started;
            assert.ok(took < 800, `close took ${took} ms`);
            // the late join is shut down in the background
            await waitFor(() => consumer.joinCount === 1 && consumer.state === 'disconnected' && !consumer.running, { timeout: 3000 });
        });

        it('close while waiting to reconnect: no consumer is created afterwards', async function () {
            fake.behaviour.consumerConnectError = fake.errors.connectionRefused();
            await helper.load([brokerNode, consumerNode], flow());
            const k1 = helper.getNode('k1');
            await nextStatus(k1, (s) => / · retry /.test(s.text));
            const n = fake.consumers.length;
            await helper.unload();
            await sleep(400);
            assert.equal(fake.consumers.length, n);
        });

        it('close disconnects a running consumer (it leaves the group)', async function () {
            const { consumer } = await loadConnected();
            await helper.unload();
            assert.equal(consumer.state, 'disconnected');
            assert.equal(consumer.running, false);
            assert.equal(consumer.disconnectCount, 1);
        });

        it('close calls done quickly even if disconnect hangs', async function () {
            consumerNode.TIMEOUTS.disconnect = 200;
            await loadConnected();
            fake.behaviour.disconnectDelayMs = Infinity; // a hung broker
            const started = Date.now();
            await helper.unload();
            const took = Date.now() - started;
            assert.ok(took >= 150 && took < 1500, `close took ${took} ms`);
        });

        it('the default bounds keep close well under the Node-RED timeout', function () {
            assert.ok(SAVED_TIMEOUTS.disconnect <= 5000);
            assert.ok(SAVED_TIMEOUTS.closeTotal <= 10000);
        });

        it('close calls done via the hard stop even if everything else hangs', async function () {
            consumerNode.TIMEOUTS.disconnect = 60000;
            consumerNode.TIMEOUTS.closeTotal = 300;
            await loadConnected();
            fake.behaviour.disconnectDelayMs = Infinity;
            const started = Date.now();
            await helper.unload();
            const took = Date.now() - started;
            assert.ok(took >= 250 && took < 1500, `close took ${took} ms`);
        });

        it('a control message after close is ignored (done without error)', async function () {
            await loadConnected();
            const k1 = helper.getNode('k1');
            const rt = k1.consumerRuntime;
            await helper.unload();
            let doneArgs = null;
            rt.input({ action: 'start' }, (...args) => {
                doneArgs = args;
            });
            assert.deepEqual(doneArgs, []);
            await sleep(150);
            assert.equal(fake.consumers.length, 1);
        });
    });

    // ------------------------------------------------------------------ pure helpers

    describe('helpers', function () {
        it('parseConfig: defaults and stable group id', function () {
            const { cfg, problems, warnings } = parseConfig({ topics: 'a' }, 'ab.cd/ef');
            assert.deepEqual(problems, []);
            assert.deepEqual(warnings, []);
            assert.equal(cfg.groupId, 'node-red-ab.cd_ef');
            assert.equal(cfg.payloadFormat, 'string');
            assert.equal(cfg.keyFormat, 'string');
            assert.equal(cfg.headersFormat, 'string');
            assert.equal(cfg.commitMode, 'auto');
            assert.equal(cfg.autoCommitInterval, 5000);
            assert.equal(cfg.autoCommitThreshold, undefined);
            assert.equal(cfg.fromBeginning, false);
            assert.equal(cfg.autoStart, true);
            assert.equal(cfg.allowAutoTopicCreation, false);
        });

        it('parseConfig: unknown formats fall back to the defaults; never throws on garbage', function () {
            const { cfg } = parseConfig({ topics: ['x', 'y'], payloadFormat: 'XML', keyFormat: 'BUFFER', commitMode: 'Manual' }, 'n');
            assert.deepEqual(cfg.topics, ['x', 'y']);
            assert.equal(cfg.payloadFormat, 'string');
            assert.equal(cfg.keyFormat, 'buffer');
            assert.equal(cfg.commitMode, 'manual');
            for (const bad of [undefined, null, 42, 'str', { topics: { toString() { throw new Error('x'); } } }]) {
                const r = parseConfig(bad, 'n');
                assert.ok(r.problems.length > 0);
            }
        });

        it('parseTopicRegex', function () {
            assert.equal(String(parseTopicRegex('^a')), '/^a/');
            assert.equal(String(parseTopicRegex('/^a$/gimy')), '/^a$/im');
            assert.throws(() => parseTopicRegex('(['), SyntaxError);
        });

        it('parseSeek / seekOffset / parseCommit', function () {
            const cfg = parseConfig({ topics: 'only, t' }, 'n').cfg;
            const single = parseConfig({ topics: 'only' }, 'n').cfg;
            assert.deepEqual(parseSeek({ partition: 1, offset: '0' }, single), { topic: 'only', partition: 1, offset: '0' });
            assert.deepEqual(parseSeek({ partition: 1, offset: 'Beginning' }, single), { topic: 'only', partition: 1, offset: 'earliest' });
            assert.deepEqual(parseSeek({ partition: 1, offset: '-1' }, single), { topic: 'only', partition: 1, offset: 'latest' });
            assert.deepEqual(parseSeek({ topic: ' t ', partition: 1, offset: 3n }, single), { topic: 't', partition: 1, offset: '3' });
            assert.throws(() => parseSeek({ partition: 1.5, offset: 0 }, single), /partition must be a whole number/);
            assert.throws(() => parseSeek({ partition: 0, offset: 1.5 }, single), /offset must be/);
            assert.throws(() => parseSeek({ topic: 5, partition: 0, offset: 0 }, single), /topic must be a string/);

            const offsets = [{ partition: 0, offset: '9', low: '2', high: '9' }, { partition: 1, offset: '0', low: '0', high: '0' }];
            const target = (offset, partition = 0) => ({ topic: 'only', partition, offset });
            assert.equal(seekOffset(target('earliest'), offsets), '2');
            assert.equal(seekOffset(target('latest'), offsets), '9');
            assert.equal(seekOffset(target('2'), offsets), '2');
            assert.equal(seekOffset(target('9'), offsets), '9');
            assert.equal(seekOffset(target('latest', 1), offsets), '0');
            assert.throws(() => seekOffset(target('1'), offsets), /offset 1 is outside the offsets of only\[0\] \(2 to 9\)/);
            assert.throws(() => seekOffset(target('10'), offsets), /offset 10 is outside/);
            assert.throws(() => seekOffset(target('0', 7), offsets), /partition 7 of topic "only" does not exist/);
            assert.throws(() => seekOffset(target('0'), [{ partition: 0, low: 'x', high: '1' }]), /unusable offsets/);

            const mine = { groupId: 'node-red-n' };
            assert.deepEqual(parseCommit({ kafka: { ...mine, topic: 't', partition: 2, offset: '99' } }, cfg), { topic: 't', partition: 2, offset: '100' });
            assert.deepEqual(parseCommit({ topic: 'only', partition: 0, offset: 0, kafka: null }, cfg), { topic: 'only', partition: 0, offset: '1' });
            assert.deepEqual(parseCommit({ partition: 0, offset: 0 }, single), { topic: 'only', partition: 0, offset: '1' });
            assert.throws(() => parseCommit({ kafka: { topic: 't', partition: 2, offset: '99' } }, cfg), /has no groupId/);
            assert.throws(() => parseCommit({ kafka: { groupId: 'x', topic: 't', partition: 2, offset: '99' } }, cfg), /consumer group "x"/);
            assert.throws(() => parseCommit({ kafka: { ...mine, topic: 'z', partition: 2, offset: '99' } }, cfg), /does not subscribe to topic "z"/);
            // never mixed: a msg.kafka without offset does not take msg.offset
            assert.throws(() => parseCommit({ offset: 5, kafka: { ...mine, topic: 't', partition: 0 } }, cfg), /msg\.kafka\.offset is required/);
            const regexCfg = parseConfig({ topics: '^x', topicIsRegex: true }, 'n').cfg;
            assert.throws(() => parseCommit({ partition: 0, offset: 0 }, regexCfg),
                /msg\.topic is required because the node subscribes to a topic pattern/);
            assert.deepEqual(parseCommit({ topic: 'xy', partition: 0, offset: 0 }, regexCfg).topic, 'xy');
            // 2^63-1 is the last valid offset; committing it would need 2^63
            assert.throws(() => parseCommit({ partition: 0, offset: 9223372036854775807n }, single), /offset 9223372036854775807 is too large/);
            assert.deepEqual(parseCommit({ partition: 0, offset: '9223372036854775806' }, single).offset, '9223372036854775807');
        });

        it('fmtDelay', function () {
            assert.equal(fmtDelay(100), '0.1s');
            assert.equal(fmtDelay(1000), '1s');
            assert.equal(fmtDelay(60000), '60s');
            assert.equal(fmtDelay(300000), '5m');
        });
    });

    // ------------------------------------------------------------------ editor

    describe('editor definition (kafka-consumer.html)', function () {
        const html = fs.readFileSync(path.join(__dirname, '../../nodes/kafka-consumer.html'), 'utf8');

        function scriptBlock(type) {
            const re = new RegExp(`<script type="${type}"[^>]*>([\\s\\S]*?)</script>`);
            const m = re.exec(html);
            assert.ok(m, `no ${type} block`);
            return m[1];
        }

        /** Run the editor script with a stub RED and jQuery and return the registered definition. */
        function definition() {
            let registered = null;
            const jq = () => ({ length: 0, is: () => false });
            const sandbox = {
                RED: {
                    nodes: {
                        registerType(type, def) {
                            registered = { type, def };
                        }
                    },
                    validators: { number: () => () => true }
                },
                $: jq
            };
            vm.runInNewContext(scriptBlock('text/javascript'), sandbox, { filename: 'kafka-consumer.html.js' });
            assert.ok(registered, 'registerType was not called');
            return registered;
        }

        it('registers kafkajs-nc-consumer with every runtime setting as a default', function () {
            const { type, def } = definition();
            assert.equal(type, 'kafkajs-nc-consumer');
            assert.equal(def.category, 'kafka');
            assert.equal(def.color, '#c7d8e6');
            assert.equal(def.icon, 'kafka.svg');
            assert.equal(def.paletteLabel, 'kafka consumer');
            assert.equal(def.inputs, 1);
            assert.equal(def.outputs, 1);
            assert.equal(def.defaults.broker.type, 'kafkajs-nc-broker');
            const keys = Object.keys(def.defaults).sort();
            assert.deepEqual(keys, [
                'allowAutoTopicCreation', 'autoCommitInterval', 'autoCommitThreshold', 'autoStart', 'broker', 'commitMode',
                'fromBeginning', 'groupId', 'headersFormat', 'heartbeatInterval', 'keyFormat', 'maxBytes',
                'maxBytesPerPartition', 'maxWaitTimeInMs', 'name', 'partitionsConsumedConcurrently', 'payloadFormat',
                'rebalanceTimeout', 'sessionTimeout', 'topicIsRegex', 'topics'
            ]);
            // the editor defaults are the runtime defaults
            const runtime = parseConfig({ topics: 'x' }, 'n').cfg;
            for (const k of ['sessionTimeout', 'heartbeatInterval', 'rebalanceTimeout', 'maxBytesPerPartition', 'maxBytes',
                'maxWaitTimeInMs', 'partitionsConsumedConcurrently', 'autoCommitInterval', 'payloadFormat', 'keyFormat',
                'headersFormat', 'commitMode', 'fromBeginning', 'allowAutoTopicCreation', 'autoStart']) {
                assert.equal(def.defaults[k].value, runtime[k], k);
            }
            assert.equal(def.defaults.autoCommitThreshold.value, 0);
            assert.equal(def.label.call({ name: '', topics: 'orders' }), 'kafka: orders');
            assert.equal(def.label.call({ name: 'N', topics: 'orders' }), 'N');
            assert.equal(def.label.call({}), 'kafka consumer');
        });

        it('validators: topics required and checked, numbers in range, env references allowed', function () {
            const { def } = definition();
            const topics = def.defaults.topics.validate;
            assert.equal(topics.call({}, '', {}), 'at least one topic is required');
            assert.equal(topics.call({}, 'a, b.c_d-e', {}), true);
            assert.match(topics.call({}, 'a, x/y', {}), /invalid topic name "x\/y"/);
            assert.equal(topics.call({}, '${TOPICS}', {}), true);
            // Node-RED only replaces a whole value without spaces: anything else stays literal at runtime
            for (const v of [' ${TOPICS}', '${TOPICS} ', 'orders, ${MORE}', '${A}, ${B}']) {
                assert.match(String(topics.call({}, v, {})), /only replaced when it is the whole value/, v);
            }
            assert.match(String(topics.call({ topicIsRegex: true }, ' ${RX}', {})), /only replaced when it is the whole value/);
            assert.equal(topics.call({ topicIsRegex: true }, '${RX}', {}), true);
            assert.equal(def.defaults.sessionTimeout.validate(' ${SESSION}', undefined), false);
            assert.equal(topics.call({ topicIsRegex: true }, '^a/b.*', {}), true);
            assert.match(topics.call({ topicIsRegex: true }, '([', {}), /invalid regular expression/);
            assert.equal(topics.call({ topicIsRegex: true }, '/^a$/i', {}), true);
            assert.equal(topics.call({}, '', undefined), false);
            const session = def.defaults.sessionTimeout.validate;
            assert.equal(session('30000', {}), true);
            assert.equal(session('', {}), true);
            assert.equal(session('${SESSION}', {}), true);
            assert.match(session('500', {}), /from 1000 to 3600000/);
            assert.equal(session('1.5', undefined), false);
            assert.equal(def.defaults.partitionsConsumedConcurrently.validate('101', undefined), false);
        });

        /** Run oneditprepare with chainable jQuery/RED stubs; returns the placeholder set on the Group ID field. */
        function groupPlaceholder(node, subflows) {
            let placeholder = null;
            const chain = (selector) => {
                const self = new Proxy({}, {
                    get(_, name) {
                        if (name === 'length') return 0;
                        if (name === 'is') return () => false;
                        if (name === 'val') return () => '';
                        if (name === 'attr') {
                            return (attr, value) => {
                                if (selector === '#node-input-groupId' && attr === 'placeholder') placeholder = value;
                                return self;
                            };
                        }
                        return () => self;
                    }
                });
                return self;
            };
            let registered = null;
            const sandbox = {
                RED: {
                    nodes: {
                        registerType(type, def) {
                            registered = def;
                        },
                        subflow: (id) => (subflows.includes(id) ? { id, type: 'subflow' } : null)
                    },
                    tabs: { create: () => ({ addTab() {}, resize() {} }) },
                    popover: { tooltip() {} }
                },
                $: chain,
                setTimeout: () => 0
            };
            vm.runInNewContext(scriptBlock('text/javascript'), sandbox, { filename: 'kafka-consumer.html.js' });
            registered.oneditprepare.call(node);
            return placeholder;
        }

        it('the Group ID placeholder shows the runtime default, also inside a subflow', function () {
            assert.equal(groupPlaceholder({ id: 'abc.12', z: 'flow1' }, []), 'node-red-abc.12');
            // the runtime id of a node in a subflow instance is "<instance id>-<node id>"
            assert.equal(groupPlaceholder({ id: 'abc.12', z: 'sf1' }, ['sf1']),
                'node-red-<subflow instance id>-abc.12 (one group per subflow instance)');
        });

        it('help property lines have no extra ":" (Node-RED\'s help renderer drops the text before it)', function () {
            const help = scriptBlock('text/markdown');
            assert.deepEqual(help.split('\n').filter((l) => /^: .*\(.*\) : .*:/.test(l)), []);
        });

        it('the text/javascript block is valid JavaScript (node --check)', function () {
            const file = path.join(tmpDir('kafkajs-nc-consumer-html-'), 'editor.js');
            fs.writeFileSync(file, scriptBlock('text/javascript'));
            const { execFileSync } = require('node:child_process');
            execFileSync(process.execPath, ['--check', file], { stdio: 'pipe' });
        });

        it('has markdown help (no leading whitespace) documenting the important topics', function () {
            const help = scriptBlock('text/markdown');
            assert.match(html, /<script type="text\/markdown" data-help-name="kafkajs-nc-consumer">/);
            const lines = help.split('\n').filter((l) => l.trim() !== '');
            assert.ok(lines.every((l) => !/^\s/.test(l) || /^\s{2}\S/.test(l)), 'markdown lines must not be indented (except list continuations)');
            for (const re of [/### Inputs/, /### Outputs/, /### Details/, /### References/, /at least once/,
                /offset \+ 1/, /node-red-<node id>/, /topics that\s+exist when the consumer starts/, /zstd is not supported/,
                /snappy/, /lz4/, /`pause`/, /`resume`/, /`stop`/, /`start`/, /`restart`/, /`seek`/, /`commit`/, /highWatermark/,
                /A producer node replaces `msg\.kafka`/, /at the next retry once the topic exists/, /connected · no partitions/,
                /a number outside them is refused/, /<subflow instance id>-<node id>/]) {
                assert.match(help, re);
            }
            // automatic commits re-deliver what was handed over since the last commit: not "at most once"
            assert.doesNotMatch(html, /at most once/);
            assert.match(html, /data-template-name="kafkajs-nc-consumer"/);
            for (const id of ['topics', 'topicIsRegex', 'groupId', 'fromBeginning', 'autoStart', 'payloadFormat', 'keyFormat',
                'headersFormat', 'commitMode', 'autoCommitInterval', 'autoCommitThreshold', 'sessionTimeout', 'heartbeatInterval',
                'rebalanceTimeout', 'maxBytesPerPartition', 'maxBytes', 'maxWaitTimeInMs', 'partitionsConsumedConcurrently',
                'allowAutoTopicCreation', 'broker', 'name']) {
                assert.match(html, new RegExp(`id="node-input-${id}"`), id);
            }
        });
    });
});
