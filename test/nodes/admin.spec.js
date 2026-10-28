'use strict';

/**
 * kafkajs-nc-admin: action dispatch and parameter handling, error routing to
 * Catch nodes, lazy (re)connection, timeouts, sequential processing and a
 * bounded close. Uses the fake kafkajs; no Kafka needed.
 */

const assert = require('node:assert/strict');
const helper = require('node-red-node-test-helper');

const brokerNode = require('../../nodes/kafka-broker.js');
const adminNode = require('../../nodes/kafka-admin.js');
const factory = require('../../lib/kafka-factory');
const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir, sleep, waitFor, nextStatus, statusCalls } = require('../helpers/common');

helper.init(require.resolve('node-red'));

function flow(adminProps = {}, brokerProps = {}) {
    return [
        Object.assign({ id: 'b1', type: 'kafkajs-nc-broker', name: 'B', brokers: 'localhost:9092' }, brokerProps),
        Object.assign({ id: 'a1', type: 'kafkajs-nc-admin', name: 'A', broker: 'b1', action: '', timeout: '2000', wires: [['out']] },
            adminProps),
        { id: 'out', type: 'helper' },
        { id: 'c1', type: 'catch', scope: null, uncaught: false, wires: [['caught']] },
        { id: 'caught', type: 'helper' }
    ];
}

/** Collect every message a helper node receives. */
function collect(id) {
    const list = [];
    helper.getNode(id).on('input', (msg) => list.push(msg));
    return list;
}

/** Send msg to a1 and wait for either the output or the Catch node. */
function run(msg, timeout = 3000) {
    return new Promise((resolve, reject) => {
        const out = helper.getNode('out');
        const caught = helper.getNode('caught');
        let timer = null;
        const cleanup = () => {
            clearTimeout(timer);
            out.removeListener('input', onOut);
            caught.removeListener('input', onCaught);
        };
        function onOut(m) {
            if (m._testId !== msg._testId) return;
            cleanup();
            resolve({ out: m });
        }
        function onCaught(m) {
            if (m._testId !== msg._testId) return;
            cleanup();
            resolve({ error: m });
        }
        timer = setTimeout(() => {
            cleanup();
            reject(new Error(`no result for ${JSON.stringify(msg)} within ${timeout} ms`));
        }, timeout);
        out.on('input', onOut);
        caught.on('input', onCaught);
        helper.getNode('a1').receive(msg);
    });
}

let testId = 0;
function message(props) {
    testId++;
    return Object.assign({ _testId: testId, topic: 'keep-me' }, props);
}

describe('kafkajs-nc-admin node', function () {
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
        helper.settings({ userDir: tmpDir('kafkajs-nc-admin-') });
        helper.startServer(done);
    });

    afterEach(async function () {
        await helper.unload();
        await helper.stopServer();
    });

    it('shows "ready" once started and does not connect before the first message', async function () {
        await helper.load([brokerNode, adminNode], flow());
        const a1 = helper.getNode('a1');
        await nextStatus(a1, (s) => s.text === 'ready' && s.fill === 'grey');
        assert.equal(fake.admins.length, 0);
        assert.equal(fake.instances.length, 0);
    });

    describe('actions', function () {
        const cases = [
            ['listTopics', undefined, undefined, ['test-topic']],
            ['createTopics',
                {
                    topics: [{ topic: 'orders', numPartitions: '3', replicationFactor: 1, configEntries: { 'retention.ms': 86400000 } }],
                    validateOnly: 'false', waitForLeaders: true, timeout: 5000
                },
                {
                    topics: [{ topic: 'orders', numPartitions: 3, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '86400000' }] }],
                    validateOnly: false, waitForLeaders: true, timeout: 5000
                },
                true],
            ['createTopics', 'a, b', { topics: [{ topic: 'a' }, { topic: 'b' }] }, true],
            ['createTopics', ['x', { topic: 'y', numPartitions: 2 }], { topics: [{ topic: 'x' }, { topic: 'y', numPartitions: 2 }] }, true],
            ['createTopics', { topic: 'single', replicationFactor: '-1' }, { topics: [{ topic: 'single', replicationFactor: -1 }] }, true],
            ['deleteTopics', 'orders', { topics: ['orders'] }, true],
            ['deleteTopics', { topics: ['a', 'b'], timeout: 1000 }, { topics: ['a', 'b'], timeout: 1000 }, true],
            ['createPartitions', { topicPartitions: [{ topic: 'orders', count: '6' }], validateOnly: true },
                { topicPartitions: [{ topic: 'orders', count: 6 }], validateOnly: true }, true],
            ['fetchTopicMetadata', undefined, {}, { topics: [{ name: 'test-topic', partitions: [] }] }],
            ['fetchTopicMetadata', ['t1'], { topics: ['t1'] }, null],
            ['fetchTopicOffsets', 'orders', 'orders', [{ partition: 0, offset: '0', high: '0', low: '0' }]],
            ['fetchTopicOffsets', { topic: 'orders' }, 'orders', null],
            ['fetchTopicOffsetsByTimestamp', { topic: 'orders', timestamp: '2024-01-01T00:00:00.000Z' },
                { topic: 'orders', timestamp: 1704067200000 }, [{ partition: 0, offset: '0' }]],
            ['fetchTopicOffsetsByTimestamp', { topic: 'orders', timestamp: 1700000000000 }, { topic: 'orders', timestamp: 1700000000000 }, null],
            ['fetchOffsets', { groupId: 'g', topics: ['orders'] }, { groupId: 'g', topics: ['orders'] },
                [{ topic: 'orders', partitions: [{ partition: 0, offset: '-1', metadata: null }] }]],
            ['fetchOffsets', { groupId: 'g' }, { groupId: 'g' }, null],
            ['resetOffsets', { groupId: 'g', topic: 'orders', earliest: true }, { groupId: 'g', topic: 'orders', earliest: true }, true],
            ['resetOffsets', { groupId: 'g', topic: 'orders' }, { groupId: 'g', topic: 'orders', earliest: false }, true],
            ['setOffsets', { groupId: 'g', topic: 't', partitions: [{ partition: '0', offset: 42 }, { partition: 1, offset: '7' }] },
                { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '42' }, { partition: 1, offset: '7' }] }, true],
            ['listGroups', undefined, undefined, { groups: [{ groupId: 'test-group', protocolType: 'consumer' }] }],
            ['describeGroups', 'g1', ['g1'], null],
            ['describeGroups', { groupIds: ['g1', 'g2'] }, ['g1', 'g2'], null],
            ['deleteGroups', { groupIds: ['g1', 'g2'] }, ['g1', 'g2'], [{ groupId: 'g1', errorCode: 0 }, { groupId: 'g2', errorCode: 0 }]],
            ['describeCluster', undefined, undefined,
                { brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }], controller: 1, clusterId: 'fake-cluster-id' }],
            ['describeConfigs', { resources: [{ type: 'topic', name: 'orders', configNames: ['retention.ms'] }], includeSynonyms: true },
                { resources: [{ type: 2, name: 'orders', configNames: ['retention.ms'] }], includeSynonyms: true }, null],
            ['describeConfigs', { resources: [{ type: 4, name: 1 }] }, { resources: [{ type: 4, name: '1' }] }, null]
        ];

        for (const [action, payload, expectedArgs, expectedResult] of cases) {
            it(`${action} ${JSON.stringify(payload)} calls admin.${action} with the right parameters`, async function () {
                await helper.load([brokerNode, adminNode], flow());
                const { out, error } = await run(message({ action, payload }));
                assert.equal(error, undefined, error && error.error && error.error.message);
                assert.equal(fake.admins.length, 1);
                const calls = fake.admins[0].calls;
                assert.equal(calls.length, 1);
                assert.equal(calls[0].method, action);
                assert.deepEqual(calls[0].args, expectedArgs);
                if (expectedResult !== null) {
                    if (action === 'fetchTopicMetadata') {
                        assert.deepEqual(out.payload.topics.map((t) => t.name), expectedResult.topics.map((t) => t.name));
                    } else {
                        assert.deepEqual(out.payload, expectedResult);
                    }
                }
                assert.equal(out.kafka.action, action);
                assert.equal(typeof out.kafka.durationMs, 'number');
                assert.ok(out.kafka.durationMs >= 0);
                assert.equal(out.topic, 'keep-me', 'the rest of the message is passed through');
                assert.equal(out.action, action);
            });
        }

        it('the node Action wins over msg.action; msg.action is case-insensitive', async function () {
            await helper.load([brokerNode, adminNode], flow({ action: 'listTopics' }));
            const { out } = await run(message({ action: 'deleteTopics', payload: 'x' }));
            assert.deepEqual(out.payload, ['test-topic']);
            assert.deepEqual(fake.admins[0].calls.map((c) => c.method), ['listTopics']);
            await helper.unload();
            await helper.load([brokerNode, adminNode], flow());
            const second = await run(message({ action: 'DESCRIBECLUSTER' }));
            assert.equal(second.out.kafka.action, 'describeCluster');
        });

        it('keeps one connected client for many messages and reports "ok"', async function () {
            await helper.load([brokerNode, adminNode], flow());
            const a1 = helper.getNode('a1');
            for (let i = 0; i < 3; i++) await run(message({ action: 'listTopics' }));
            assert.equal(fake.admins.length, 1);
            assert.equal(fake.admins[0].connectCount, 1);
            assert.equal(fake.instances.length, 1);
            await nextStatus(a1, (s) => s.text === 'ok · listTopics' && s.fill === 'green');
            const texts = statusCalls(a1).map((s) => s.text);
            assert.ok(texts.includes('connecting…'), texts.join(', '));
            assert.ok(texts.includes('running listTopics'), texts.join(', '));
        });
    });

    describe('invalid requests (nothing is sent to Kafka)', function () {
        const bad = [
            [{ action: 'frobnicate' }, /Unknown admin action "frobnicate"\. Valid actions: listTopics, createTopics, .*describeConfigs/],
            [{}, /No admin action: set the node's Action or msg\.action \(one of listTopics, /],
            [{ action: 42 }, /Unknown admin action "42"/],
            [{ action: 'createTopics', payload: {} }, /createTopics: msg\.payload\.topics is required/],
            [{ action: 'createTopics', payload: { topics: [{ topic: 'x', numPartitions: 'many' }] } }, /numPartitions must be an integer/],
            [{ action: 'createTopics', payload: { topics: [{ topic: 'x', numPartitions: 0 }] } }, /numPartitions must be >= 1/],
            [{ action: 'createTopics', payload: { topics: [{ topic: '' }] } }, /msg\.payload\.topics\[\]\.topic is required/],
            [{ action: 'deleteTopics', payload: 5 }, /deleteTopics: msg\.payload\.topics is required/],
            [{ action: 'deleteTopics', payload: { topics: [1, 2] } }, /every topic must be a string/],
            [{ action: 'createPartitions', payload: { topicPartitions: [{ topic: 'x' }] } }, /count \(the new total number of partitions\) is required/],
            [{ action: 'fetchTopicOffsets', payload: {} }, /fetchTopicOffsets: msg\.payload\.topic is required/],
            [{ action: 'fetchTopicOffsetsByTimestamp', payload: { topic: 't', timestamp: 'yesterday' } }, /msg\.payload\.timestamp is required/],
            [{ action: 'fetchOffsets', payload: { topics: ['t'] } }, /fetchOffsets: msg\.payload\.groupId is required/],
            [{ action: 'resetOffsets', payload: { groupId: 'g' } }, /resetOffsets: msg\.payload\.topic is required/],
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't' } }, /setOffsets: msg\.payload\.partitions is required/],
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: 'abc' }] } },
                /partitions\[\]\.offset must be an integer/],
            [{ action: 'describeGroups', payload: {} }, /describeGroups: msg\.payload\.groupIds is required/],
            [{ action: 'deleteGroups', payload: [] }, /deleteGroups: msg\.payload\.groupIds is required/],
            [{ action: 'describeConfigs', payload: {} }, /describeConfigs: msg\.payload\.resources is required/],
            [{ action: 'describeConfigs', payload: { resources: [{ type: 'TABLE', name: 'x' }] } }, /unknown resource type "TABLE"/]
        ];
        for (const [props, expected] of bad) {
            it(`${JSON.stringify(props)} => Catch gets a helpful error`, async function () {
                await helper.load([brokerNode, adminNode], flow());
                const { out, error } = await run(message(props));
                assert.equal(out, undefined);
                assert.match(error.error.message, expected);
                assert.equal(error.error.source.id, 'a1');
                assert.equal(error.topic, 'keep-me', 'Catch receives the original message');
                assert.equal(fake.instances.length, 0, 'no connection was made');
            });
        }

        it('an unknown Action in the node configuration is flagged at start and per message', async function () {
            await helper.load([brokerNode, adminNode], flow({ action: 'dropEverything' }));
            const a1 = helper.getNode('a1');
            await nextStatus(a1, (s) => s.fill === 'red' && s.text === 'unknown action');
            const { error } = await run(message({ action: 'listTopics' }));
            assert.match(error.error.message, /Unknown admin action "dropEverything"/);
            assert.equal(fake.instances.length, 0);
        });

        it('a payload getter that throws is reported, not crashed on (even though Catch cannot clone the msg)', async function () {
            await helper.load([brokerNode, adminNode], flow());
            const a1 = helper.getNode('a1');
            const payload = {};
            Object.defineProperty(payload, 'topics', { enumerable: true, get() { throw new Error('evil getter'); } });
            assert.doesNotThrow(() => a1.receive({ action: 'deleteTopics', payload }));
            await waitFor(() => helper.log().args.some((a) => /evil getter/.test(String(a[0].msg))), { message: 'error logged' });
            assert.equal(fake.instances.length, 0);
            // the node keeps working
            const { out } = await run(message({ action: 'listTopics' }));
            assert.deepEqual(out.payload, ['test-topic']);
        });
    });

    describe('Kafka errors', function () {
        it('a request error (e.g. invalid replication factor) goes to Catch and keeps the connection', async function () {
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.adminErrors = { createTopics: fake.errors.protocol('INVALID_REPLICATION_FACTOR') };
            const first = await run(message({ action: 'createTopics', payload: 'orders' }));
            assert.match(first.error.error.message, /replication/i);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && /^createTopics: /.test(s.text));
            const second = await run(message({ action: 'listTopics' }));
            assert.deepEqual(second.out.payload, ['test-topic']);
            assert.equal(fake.admins.length, 1, 'same client');
            assert.equal(fake.admins[0].disconnectCount, 0);
        });

        it('connection failure => done(err) to Catch; the next message reconnects with a new client', async function () {
            await helper.load([brokerNode, adminNode], flow());
            const refused = fake.errors.connectionRefused();
            fake.behaviour.adminConnectError = (args, admin) => (admin.index === 0 ? refused : null);
            const first = await run(message({ action: 'listTopics' }));
            assert.equal(first.out, undefined);
            assert.match(first.error.error.message, /ECONNREFUSED/);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && /^listTopics: ECONNREFUSED/.test(s.text));
            await waitFor(() => fake.admins[0].disconnectCount === 1, { message: 'failed client is disconnected' });

            const second = await run(message({ action: 'listTopics' }));
            assert.deepEqual(second.out.payload, ['test-topic']);
            assert.equal(fake.admins.length, 2, 'a new admin client');
            assert.equal(fake.instances.length, 2, 'a new kafkajs client (createKafka per connect attempt)');
            assert.equal(fake.admins[1].state, 'connected');
        });

        it('a transient failure of a running operation drops the client; the next message reconnects', async function () {
            await helper.load([brokerNode, adminNode], flow());
            await run(message({ action: 'listTopics' }));
            fake.behaviour.adminErrors = { listTopics: (args, admin) => (admin.index === 0 ? fake.errors.connectionClosed() : null) };
            const failed = await run(message({ action: 'listTopics' }));
            assert.ok(failed.error, 'error routed to Catch');
            await waitFor(() => fake.admins[0].disconnectCount === 1, { message: 'old client disconnected' });
            const ok = await run(message({ action: 'listTopics' }));
            assert.deepEqual(ok.out.payload, ['test-topic']);
            assert.equal(fake.admins.length, 2);
        });

        it('authentication failure shows "auth failed" and reconnects on the next message', async function () {
            await helper.load([brokerNode, adminNode], flow({}, { authMechanism: 'plain', tls: true }), { b1: { username: 'u', password: 'wrong' } });
            const sasl = fake.errors.saslAuth('PLAIN');
            fake.behaviour.adminConnectError = (args, admin) => (admin.index === 0 ? sasl : null);
            const first = await run(message({ action: 'listTopics' }));
            assert.ok(first.error);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && /^auth failed/.test(s.text));
            const second = await run(message({ action: 'listTopics' }));
            assert.ok(second.out);
            assert.equal(fake.admins.length, 2);
        });

        it('kafka.admin() throwing synchronously is reported per message, without crashing', async function () {
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.adminFactoryError = new TypeError("'timeout' is not a number, received 'string'");
            const first = await run(message({ action: 'listTopics' }));
            assert.match(first.error.error.message, /Could not create the Kafka admin client: 'timeout' is not a number/);
            // same status as the producer and the consumer for a factory throw
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && s.text === 'config error');
            delete fake.behaviour.adminFactoryError;
            const second = await run(message({ action: 'listTopics' }));
            assert.ok(second.out);
        });
    });

    describe('timeouts', function () {
        it('an operation that hangs times out, goes to Catch and the client is replaced', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '200' }));
            fake.behaviour.adminDelayMs = { listTopics: (args, admin) => (admin.index === 0 ? Infinity : 0) };
            const started = Date.now();
            const first = await run(message({ action: 'listTopics' }));
            const elapsed = Date.now() - started;
            assert.match(first.error.error.message, /Kafka admin listTopics timed out after 200ms/);
            // Node-RED >= 4 copies err.code into msg.error; Node-RED 3 does not
            const nrMajor = Number(require('node-red/package.json').version.split('.')[0]);
            if (nrMajor >= 4) assert.equal(first.error.error.code, 'ETIMEDOUT');
            assert.ok(elapsed >= 150 && elapsed < 1500, `took ${elapsed} ms`);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && s.text === 'listTopics timed out');
            await waitFor(() => fake.admins[0].disconnectCount === 1, { message: 'hung client is disconnected' });
            const second = await run(message({ action: 'listTopics' }));
            assert.deepEqual(second.out.payload, ['test-topic']);
            assert.equal(fake.admins.length, 2);
        });

        it('the timeout also bounds connecting; a connect that completes late is disconnected (no zombie)', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '150' }));
            fake.behaviour.connectDelayMs = (args, client) => (client.index === 0 ? 400 : 0);
            const first = await run(message({ action: 'listTopics' }));
            assert.match(first.error.error.message, /timed out after 150ms/);
            const second = await run(message({ action: 'listTopics' }));
            assert.ok(second.out, 'second message uses a fresh client');
            assert.equal(fake.admins.length, 2);
            // the first connect() resolves later; the node must disconnect that client again
            await waitFor(() => fake.admins[0].connectCount === 1 && fake.admins[0].state === 'disconnected',
                { timeout: 2000, message: 'late connect is disconnected' });
            assert.equal(fake.admins[0].calls.length, 0, 'the abandoned client was never used');
        });
    });

    describe('sequential processing', function () {
        it('runs one operation at a time, in arrival order, also when one fails', async function () {
            await helper.load([brokerNode, adminNode], flow());
            let active = 0;
            let maxActive = 0;
            fake.behaviour.adminDelayMs = {
                listTopics: () => {
                    active++;
                    maxActive = Math.max(maxActive, active);
                    return 30;
                },
                describeCluster: () => {
                    active++;
                    maxActive = Math.max(maxActive, active);
                    return 10;
                }
            };
            fake.behaviour.adminResults = {
                listTopics: () => {
                    active--;
                    return ['t'];
                }
            };
            fake.behaviour.adminErrors = {
                describeCluster: () => {
                    active--;
                    return fake.errors.protocol('INVALID_REQUEST');
                }
            };
            const outs = collect('out');
            const caught = collect('caught');
            const a1 = helper.getNode('a1');
            const msgs = [1, 2, 3, 4, 5].map((n) => message({ n, action: n === 3 ? 'describeCluster' : 'listTopics' }));
            for (const m of msgs) a1.receive(m);
            await waitFor(() => outs.length === 4 && caught.length === 1, { message: `outs ${outs.length} caught ${caught.length}` });
            assert.equal(maxActive, 1, 'never more than one operation at a time');
            assert.deepEqual(outs.map((m) => m.n), [1, 2, 4, 5]);
            assert.equal(caught[0].n, 3);
            assert.deepEqual(fake.admins[0].calls.map((c) => c.method),
                ['listTopics', 'listTopics', 'describeCluster', 'listTopics', 'listTopics']);
        });

        it('shows how many messages are queued while one is running', async function () {
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.adminDelayMs = { listTopics: 50 };
            const a1 = helper.getNode('a1');
            const outs = collect('out');
            for (let i = 0; i < 3; i++) a1.receive(message({ action: 'listTopics' }));
            await waitFor(() => outs.length === 3);
            const texts = statusCalls(a1).map((s) => s.text);
            assert.ok(texts.includes('running listTopics (+2 queued)') || texts.includes('running listTopics (+1 queued)'), texts.join(', '));
        });
    });

    describe('close', function () {
        it('disconnects the admin client', async function () {
            await helper.load([brokerNode, adminNode], flow());
            await run(message({ action: 'listTopics' }));
            await helper.unload();
            assert.equal(fake.admins[0].disconnectCount, 1);
            assert.equal(fake.admins[0].state, 'disconnected');
        });

        it('completes within about 3 s even when disconnect() hangs', async function () {
            this.timeout(8000);
            await helper.load([brokerNode, adminNode], flow());
            await run(message({ action: 'listTopics' }));
            fake.behaviour.disconnectDelayMs = Infinity;
            const started = Date.now();
            await helper.unload();
            const elapsed = Date.now() - started;
            assert.ok(elapsed < 4500, `close took ${elapsed} ms`);
        });

        it('completes when disconnect() rejects', async function () {
            await helper.load([brokerNode, adminNode], flow());
            await run(message({ action: 'listTopics' }));
            fake.behaviour.disconnectError = new Error('disconnect failed');
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started < 1000);
        });

        it('settles a running and the queued messages with an error, and disconnects a connecting client', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '5000' }));
            fake.behaviour.connectDelayMs = 300;
            const a1 = helper.getNode('a1');
            // done(err) logs through node.error(); the helper's log spy is gone during unload()
            const errors = [];
            a1.error = (err) => errors.push(String(err && err.message ? err.message : err));
            a1.receive(message({ action: 'listTopics' }));
            a1.receive(message({ action: 'listTopics' }));
            await waitFor(() => fake.admins.length === 1);
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started < 1000, 'close does not wait for the hung job');
            await sleep(450); // let the delayed connect() finish
            assert.equal(fake.admins.length, 1, 'the queued message did not connect again');
            assert.equal(fake.admins[0].state, 'disconnected', 'the late connect was disconnected');
            assert.equal(fake.admins[0].calls.length, 0);
            assert.equal(errors.length, 2, errors.join('\n'));
            assert.ok(errors.some((m) => /stopped while running listTopics/.test(m)), errors.join('\n'));
            assert.ok(errors.some((m) => /stopped before running listTopics/.test(m)), errors.join('\n'));
        });
    });

    describe('broker configuration problems', function () {
        it('no broker selected => status "no broker config" and an error for every message', async function () {
            await helper.load([brokerNode, adminNode], flow({ broker: '' }));
            const a1 = helper.getNode('a1');
            await nextStatus(a1, (s) => s.fill === 'red' && s.text === 'no broker config');
            for (let i = 0; i < 2; i++) {
                const { error } = await run(message({ action: 'listTopics' }));
                assert.match(error.error.message, /No Kafka broker configuration selected/);
            }
            assert.equal(fake.instances.length, 0);
        });

        it('a broker with a config error (SASL without password) => "config error", errors per message, no client', async function () {
            await helper.load([brokerNode, adminNode], flow({}, { authMechanism: 'plain', tls: true }), { b1: { username: 'alice' } });
            const a1 = helper.getNode('a1');
            await nextStatus(a1, (s) => s.fill === 'red' && s.text === 'config error');
            const { error } = await run(message({ action: 'listTopics' }));
            assert.match(error.error.message, /Invalid Kafka broker configuration: SASL password is required/);
            assert.equal(fake.instances.length, 0);
        });

        it('a config error found while connecting (missing tls-config node) is reported and remembered', async function () {
            await helper.load([brokerNode, adminNode], flow({}, { tls: true, tlsConfig: 'nope' }));
            const a1 = helper.getNode('a1');
            await nextStatus(a1, (s) => s.text === 'ready');
            const first = await run(message({ action: 'listTopics' }));
            assert.match(first.error.error.message, /TLS configuration node not found/);
            await nextStatus(a1, (s) => s.fill === 'red' && s.text === 'config error');
            const second = await run(message({ action: 'listTopics' }));
            assert.match(second.error.error.message, /Invalid Kafka broker configuration: TLS configuration node not found/);
            assert.equal(fake.instances.length, 0);
        });
    });
});
