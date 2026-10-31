'use strict';

/**
 * kafkajs-nc-admin: action dispatch and parameter handling, error routing to
 * Catch nodes, lazy (re)connection, timeouts, sequential processing and a
 * bounded close. Uses the fake kafkajs; no Kafka needed.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const brokerNode = require('../../nodes/kafka-broker.js');
const adminNode = require('../../nodes/kafka-admin.js');
const factory = require('../../lib/kafka-factory');
const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir, sleep, waitFor, nextStatus, statusCalls } = require('../helpers/common');

helper.init(require.resolve('node-red'));

/** Short reconnect delays (the broker's limits: reconnectMin >= 100 ms, authRetryMin >= 1 s). */
const FAST_RECONNECT = { reconnectMin: '100', reconnectMax: '200', authRetryMin: '1000' };

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
            ['fetchTopicOffsetsByTimestamp', { topic: 'orders', timestamp: new Date(0) }, { topic: 'orders', timestamp: 0 }, null],
            ['fetchTopicOffsetsByTimestamp', { topic: 'orders', timestamp: ' 1700000000000 ' }, { topic: 'orders', timestamp: 1700000000000 }, null],
            ['createTopics', { topics: [{ topic: 'r', replicaAssignment: [{ partition: '0', replicas: [1, '2'] }] }] },
                { topics: [{ topic: 'r', replicaAssignment: [{ partition: 0, replicas: [1, 2] }] }] }, true],
            ['createPartitions', { topicPartitions: [{ topic: 'orders', count: 3, assignments: [[1, 2], ['2', 3]] }] },
                { topicPartitions: [{ topic: 'orders', count: 3, assignments: [[1, 2], [2, 3]] }] }, true],
            ['fetchOffsets', { groupId: 'g', topics: ['orders'] }, { groupId: 'g', topics: ['orders'] },
                [{ topic: 'orders', partitions: [{ partition: 0, offset: '-1', metadata: null }] }]],
            ['fetchOffsets', { groupId: 'g' }, { groupId: 'g' }, null],
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

        it('resetOffsets sets the real low/high offsets, never kafkajs\' -2/-1 markers', async function () {
            // consumers read a negative committed offset as "no position" and apply their own fromBeginning
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.adminResults = {
                fetchTopicOffsets: [{ partition: 0, offset: '9', high: '9', low: '2' }, { partition: 1, offset: '5', high: '5', low: '0' }]
            };
            const earliest = await run(message({ action: 'resetOffsets', payload: { groupId: 'g', topic: 'orders', earliest: 'true' } }));
            assert.equal(earliest.out.payload, true);
            const latest = await run(message({ action: 'resetOffsets', payload: { groupId: 'g', topic: 'orders' } }));
            assert.equal(latest.out.payload, true);
            assert.deepEqual(fake.admins[0].calls, [
                { method: 'fetchTopicOffsets', args: 'orders' },
                { method: 'setOffsets', args: { groupId: 'g', topic: 'orders', partitions: [{ partition: 0, offset: '2' }, { partition: 1, offset: '0' }] } },
                { method: 'fetchTopicOffsets', args: 'orders' },
                { method: 'setOffsets', args: { groupId: 'g', topic: 'orders', partitions: [{ partition: 0, offset: '9' }, { partition: 1, offset: '5' }] } }
            ]);
            assert.ok(fake.admins[0].calls.every((c) => c.method !== 'resetOffsets'));
        });

        it('setOffsets checks the partitions and the offsets against the topic before setting them', async function () {
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.adminResults = { fetchTopicOffsets: [{ partition: 0, offset: '50', high: '50', low: '10' }] };
            const ok = await run(message({
                action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: '0', offset: 42 }] }
            }));
            assert.equal(ok.out.payload, true);
            const bounds = await run(message({
                action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '50' }] }
            }));
            assert.ok(bounds.out, 'the high offset itself is allowed');
            // kafkajs silently ignores a partition that does not exist and reports success
            const missing = await run(message({
                action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 7, offset: 1 }] }
            }));
            assert.match(missing.error.error.message, /setOffsets: partition 7 does not exist in topic "t" \(it has 1 partition\)/);
            for (const offset of ['51', 9]) {
                const outside = await run(message({
                    action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset }] }
                }));
                assert.match(outside.error.error.message, new RegExp(`offset ${offset} for partition 0 of topic "t" is outside its offsets \\(10 to 50\\)`));
            }
            assert.deepEqual(fake.admins[0].calls.filter((c) => c.method === 'setOffsets').map((c) => c.args.partitions),
                [[{ partition: 0, offset: '42' }], [{ partition: 0, offset: '50' }]]);
            assert.equal(fake.admins.length, 1, 'a refused request keeps the connection');
            assert.equal(fake.admins[0].disconnectCount, 0);
        });

        it('setOffsets and resetOffsets get at least LIMITS.groupOffsetsTimeoutMs (kafkajs needs ~5 s for them)', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '150' }));
            fake.behaviour.adminDelayMs = { setOffsets: 400 };
            const set = await run(message({ action: 'setOffsets', payload: { groupId: 'g', topic: 'test-topic', partitions: [{ partition: 0, offset: 0 }] } }));
            assert.equal(set.error, undefined, set.error && set.error.error.message);
            const reset = await run(message({ action: 'resetOffsets', payload: { groupId: 'g', topic: 'test-topic' } }));
            assert.equal(reset.error, undefined, reset.error && reset.error.error.message);
            assert.equal(adminNode.LIMITS.groupOffsetsTimeoutMs, 15000);
            // other actions keep the node's timeout
            fake.behaviour.adminDelayMs = { listTopics: 400 };
            const list = await run(message({ action: 'listTopics' }));
            assert.match(list.error.error.message, /timed out after 150ms/);
        });

        it('the node Action wins over msg.action; msg.action is case-insensitive', async function () {
            await helper.load([brokerNode, adminNode], flow({ action: 'listTopics' }));
            const { out } = await run(message({ action: 'deleteTopics', payload: 'x' }));
            assert.deepEqual(out.payload, ['test-topic']);
            assert.deepEqual(fake.admins[0].calls.map((c) => c.method), ['listTopics']);
            await helper.unload();
            await helper.load([brokerNode, adminNode], flow());
            const second = await run(message({ action: 'DESCRIBECLUSTER' }));
            assert.equal(second.out.kafka.action, 'describeCluster');
            // trimmed too, like the consumer's and the producer's msg.action
            const third = await run(message({ action: '  listtopics\n' }));
            assert.equal(third.out.kafka.action, 'listTopics');
            await helper.unload();
            await helper.load([brokerNode, adminNode], flow({ action: ' LISTTOPICS ' }));
            const fourth = await run(message({}));
            assert.equal(fourth.out.kafka.action, 'listTopics');
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
                /partitions\[\]\.offset must be a whole number from 0 to 2\^63-1/],
            // the -1/-2 markers are stored as such and read as "no position" by consumers (ADMIN-1)
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: -1 }] } }, /offset must be a whole number/],
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '-2' }] } }, /offset must be a whole number/],
            // kafkajs would wrap it to 200376420520689663 and report success
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '99999999999999999999999' }] } },
                /offset must be a whole number from 0 to 2\^63-1/],
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: 1.5 }] } }, /offset must be a whole number/],
            [{ action: 'setOffsets', payload: { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: 1 }, { partition: '0', offset: 2 }] } },
                /partition 0 is listed twice/],
            // Kafka reads -1/-2 as latest/earliest; Date.parse reads '-1' as 2001-01-01
            [{ action: 'fetchTopicOffsetsByTimestamp', payload: { topic: 't', timestamp: -1 } }, /timestamp is required \(milliseconds since the epoch >= 0/],
            [{ action: 'fetchTopicOffsetsByTimestamp', payload: { topic: 't', timestamp: '-1' } }, /timestamp is required/],
            [{ action: 'fetchTopicOffsetsByTimestamp', payload: { topic: 't', timestamp: 'abc 1' } }, /timestamp is required/],
            [{ action: 'fetchTopicOffsetsByTimestamp', payload: { topic: 't', timestamp: new Date(-5000) } }, /timestamp is required/],
            // malformed assignments would make the broker close the connection (looks like a network failure)
            [{ action: 'createPartitions', payload: { topicPartitions: [{ topic: 'x', count: 2, assignments: [['a']] }] } },
                /assignments\[0\] must contain broker ids/],
            [{ action: 'createPartitions', payload: { topicPartitions: [{ topic: 'x', count: 2, assignments: [[]] }] } },
                /assignments\[0\] must be a non-empty array of broker ids/],
            [{ action: 'createPartitions', payload: { topicPartitions: [{ topic: 'x', count: 2, assignments: '1,2' }] } },
                /assignments of topic "x" must be an array/],
            [{ action: 'createTopics', payload: { topics: [{ topic: 'x', replicaAssignment: [[1, 2]] }] } },
                /replicaAssignment\[0\] of topic "x" must be \{partition, replicas\}/],
            [{ action: 'createTopics', payload: { topics: [{ topic: 'x', replicaAssignment: [{ partition: 0, replicas: [-1] }] }] } },
                /replicaAssignment\[0\]\.replicas must contain broker ids/],
            [{ action: 'createTopics', payload: { topics: [{ topic: 'x', replicaAssignment: [{ replicas: [1] }] }] } },
                /replicaAssignment\[0\]\.partition is required/],
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

        it('connection failure => done(err) to Catch; messages fail at once for the reconnect delay, then a new client connects', async function () {
            await helper.load([brokerNode, adminNode], flow({}, FAST_RECONNECT));
            const refused = fake.errors.connectionRefused();
            fake.behaviour.adminConnectError = (args, admin) => (admin.index === 0 ? refused : null);
            const first = await run(message({ action: 'listTopics' }));
            assert.equal(first.out, undefined);
            assert.match(first.error.error.message, /ECONNREFUSED/);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && /^listTopics: ECONNREFUSED/.test(s.text));
            await waitFor(() => fake.admins[0].disconnectCount === 1, { message: 'failed client is disconnected' });

            // right after a failed attempt: no new attempt (it would most likely fail the same way, slowly)
            const fast = await run(message({ action: 'listTopics' }));
            assert.match(fast.error.error.message,
                /Kafka admin listTopics not run: the last connection attempt failed \(ECONNREFUSED .*\); the next message after \S+ connects again/);
            assert.equal(fake.admins.length, 1);

            await sleep(150); // reconnectMin 100 ms (+/- 20 % jitter)
            const second = await run(message({ action: 'listTopics' }));
            assert.deepEqual(second.out.payload, ['test-topic']);
            assert.equal(fake.admins.length, 2, 'a new admin client');
            assert.equal(fake.instances.length, 2, 'a new kafkajs client (createKafka per connect attempt)');
            assert.equal(fake.admins[1].state, 'connected');
        });

        it('messages queued behind a failing connect fail at once instead of each waiting for another attempt', async function () {
            await helper.load([brokerNode, adminNode], flow());
            fake.behaviour.connectDelayMs = 200;
            fake.behaviour.adminConnectError = fake.errors.connectionRefused();
            const a1 = helper.getNode('a1');
            const caught = collect('caught');
            const started = Date.now();
            for (let i = 0; i < 5; i++) a1.receive(message({ action: 'listTopics' }));
            await waitFor(() => caught.length === 5, { timeout: 2000 });
            assert.ok(Date.now() - started < 600, `took ${Date.now() - started} ms (one failing connect, not five)`);
            assert.equal(fake.admins.length, 1);
            assert.match(caught[0].error.message, /ECONNREFUSED/);
            for (const m of caught.slice(1)) assert.match(m.error.message, /not run: the last connection attempt failed/);
        });

        it('authentication failure shows "auth failed"; the next attempt waits authRetryMin (no lockout), then reconnects', async function () {
            this.timeout(5000);
            await helper.load([brokerNode, adminNode], flow({}, { authMechanism: 'plain', tls: true, ...FAST_RECONNECT }),
                { b1: { username: 'u', password: 'wrong' } });
            const sasl = fake.errors.saslAuth('PLAIN');
            fake.behaviour.adminConnectError = (args, admin) => (admin.index === 0 ? sasl : null);
            const first = await run(message({ action: 'listTopics' }));
            assert.ok(first.error);
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && /^auth failed/.test(s.text));
            await sleep(300); // longer than reconnectMax: an auth failure holds back for authRetryMin (1 s here)
            const held = await run(message({ action: 'listTopics' }));
            assert.match(held.error.error.message, /not run: the last connection attempt failed \(.*SASL.*\)/i);
            assert.equal(fake.admins.length, 1);
            await sleep(800);
            const second = await run(message({ action: 'listTopics' }));
            assert.ok(second.out);
            assert.equal(fake.admins.length, 2);
        });

        it('kafka.admin() throwing synchronously is reported per message, without crashing', async function () {
            this.timeout(5000);
            await helper.load([brokerNode, adminNode], flow({}, FAST_RECONNECT));
            fake.behaviour.adminFactoryError = new TypeError("'timeout' is not a number, received 'string'");
            const first = await run(message({ action: 'listTopics' }));
            assert.match(first.error.error.message, /Could not create the Kafka admin client: 'timeout' is not a number/);
            // same status as the producer and the consumer for a factory throw
            await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && s.text === 'config error');
            delete fake.behaviour.adminFactoryError;
            await sleep(1100); // a config problem is retried after authRetryMin (1 s here)
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
            await helper.load([brokerNode, adminNode], flow({ timeout: '150' }, FAST_RECONNECT));
            fake.behaviour.connectDelayMs = (args, client) => (client.index === 0 ? 400 : 0);
            const first = await run(message({ action: 'listTopics' }));
            assert.match(first.error.error.message, /timed out after 150ms/);
            await sleep(150); // a connect that timed out counts as a failed attempt (reconnectMin 100 ms here)
            const second = await run(message({ action: 'listTopics' }));
            assert.ok(second.out, 'second message uses a fresh client');
            assert.equal(fake.admins.length, 2);
            // the first connect() resolves later; the node must disconnect that client again
            await waitFor(() => fake.admins[0].connectCount === 1 && fake.admins[0].state === 'disconnected',
                { timeout: 2000, message: 'late connect is disconnected' });
            assert.equal(fake.admins[0].calls.length, 0, 'the abandoned client was never used');
        });

        it('the timeout counts from the arrival: a message that waited only gets the rest of it', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '1000' }));
            fake.behaviour.adminDelayMs = { listTopics: 700 };
            const a1 = helper.getNode('a1');
            const outs = collect('out');
            const caught = collect('caught');
            const started = Date.now();
            a1.receive(message({ n: 1, action: 'listTopics' }));
            a1.receive(message({ n: 2, action: 'listTopics' }));
            await waitFor(() => outs.length + caught.length === 2, { timeout: 3000 });
            const took = Date.now() - started;
            assert.deepEqual(outs.map((m) => m.n), [1]);
            // it would have run until ~1400 ms, 400 ms after its timeout
            assert.equal(caught[0].n, 2);
            assert.match(caught[0].error.message, /: Kafka admin listTopics timed out after 1000ms$/);
            assert.ok(took < 1250, `took ${took} ms`);
        });

        it('behind a hung broker every queued message settles within its own timeout, not one timeout after another', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '300' }));
            fake.behaviour.adminDelayMs = { listTopics: Infinity };
            const a1 = helper.getNode('a1');
            const caught = collect('caught');
            const started = Date.now();
            for (let i = 0; i < 12; i++) a1.receive(message({ action: 'listTopics' }));
            // before: they settled at 0.3, 0.6, ... 3.6 s, each with the same text
            await waitFor(() => caught.length === 12, { timeout: 3000 });
            const took = Date.now() - started;
            assert.ok(took < 900, `took ${took} ms`);
            for (const m of caught) assert.match(m.error.message, /Kafka admin listTopics timed out after 300ms/);
            const calls = fake.admins.reduce((n, admin) => n + admin.calls.length, 0);
            assert.ok(calls <= 2, `expired messages are not sent to Kafka (${calls} calls)`);
        });

        it('a timeout reports how long the message waited behind earlier ones', async function () {
            await helper.load([brokerNode, adminNode], flow({ timeout: '1500' }));
            fake.behaviour.adminDelayMs = { listTopics: (args, admin) => (admin.calls.length === 1 ? 1100 : Infinity) };
            const a1 = helper.getNode('a1');
            const caught = collect('caught');
            a1.receive(message({ action: 'listTopics' }));
            a1.receive(message({ action: 'listTopics' }));
            await waitFor(() => caught.length === 1, { timeout: 3000 });
            assert.match(caught[0].error.message, /: Kafka admin listTopics timed out after 1500ms \(1\.1s of it waiting behind earlier messages\)$/);
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

        it('refuses new messages at once while LIMITS.maxQueued messages are waiting', async function () {
            const saved = adminNode.LIMITS.maxQueued;
            adminNode.LIMITS.maxQueued = 3;
            try {
                await helper.load([brokerNode, adminNode], flow());
                fake.behaviour.adminDelayMs = { listTopics: 200 };
                const a1 = helper.getNode('a1');
                const outs = collect('out');
                const caught = collect('caught');
                a1.receive(message({ n: 1, action: 'listTopics' }));
                await waitFor(() => fake.admins.length === 1 && fake.admins[0].calls.length === 1);
                for (let n = 2; n <= 6; n++) a1.receive(message({ n, action: 'listTopics' }));
                await waitFor(() => caught.length === 2, { timeout: 150 });
                assert.deepEqual(caught.map((m) => m.n), [5, 6]);
                assert.match(caught[0].error.message, /: Kafka admin listTopics not run: 3 messages are already waiting \(is Kafka reachable\?\)$/);
                await waitFor(() => outs.length === 4, { timeout: 2000 });
                assert.deepEqual(outs.map((m) => m.n), [1, 2, 3, 4]);
            } finally {
                adminNode.LIMITS.maxQueued = saved;
            }
            assert.equal(adminNode.LIMITS.maxQueued, 1000);
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

    describe('node construction', function () {
        it('an unexpected error in the constructor shows "internal error", like the producer and the consumer', function () {
            let ctor = null;
            const statuses = [];
            const errors = [];
            const RED = {
                nodes: {
                    registerType(type, fn) {
                        ctor = fn;
                    },
                    createNode(node) {
                        node.on = () => node;
                        node.status = (s) => statuses.push(s);
                        node.error = (e) => errors.push(String(e));
                    },
                    getNode() {
                        throw new Error('registry broken');
                    }
                }
            };
            adminNode(RED);
            const node = {};
            assert.doesNotThrow(() => ctor.call(node, { broker: 'b1', action: '' }));
            assert.deepEqual(statuses.map((s) => [s.fill, s.shape, s.text]), [['red', 'ring', 'internal error']]);
            assert.match(errors[0], /Kafka admin node failed to start: registry broken/);
        });
    });

    describe('editor definition (kafka-admin.html)', function () {
        const html = fs.readFileSync(path.join(__dirname, '../../nodes/kafka-admin.html'), 'utf8');
        const script = /<script type="text\/javascript">([\s\S]*?)<\/script>/.exec(html)[1];

        /** Evaluate the editor script; `$` records what oneditprepare does with the Action select. */
        function editor() {
            const added = [];
            let selected = null;
            const chain = (selector) => {
                let value; // the value given to a new <option> element
                const self = new Proxy({}, {
                    get(_, name) {
                        if (name === 'val') {
                            return (v) => {
                                if (v === undefined) return selector === '#node-input-action' ? selected : '';
                                if (selector === '#node-input-action') selected = v;
                                else value = v;
                                return self;
                            };
                        }
                        if (name === 'text') {
                            return (t) => {
                                added.push({ value, text: t });
                                return self;
                            };
                        }
                        return () => self;
                    }
                });
                return self;
            };
            let def = null;
            vm.runInNewContext(script, { RED: { nodes: { registerType: (type, d) => { def = d; } } }, $: chain },
                { filename: 'kafka-admin.html.js' });
            return { def, added, selected: () => selected };
        }

        it('an action the editor does not list (also an Object.prototype name) is shown as unknown, not as a blank select', function () {
            for (const action of ['dropEverything', 'toString', 'constructor', '__proto__']) {
                const e = editor();
                e.def.oneditprepare.call({ action });
                assert.deepEqual(e.added, [{ value: action, text: `${action} (unknown)` }], action);
                assert.equal(e.selected(), action);
            }
            const known = editor();
            known.def.oneditprepare.call({ action: 'listTopics' });
            assert.deepEqual(known.added, []);
        });

        it('the timeout accepts a whole ${VAR} only (Node-RED replaces nothing else)', function () {
            const { def } = editor();
            const timeout = def.defaults.timeout.validate;
            assert.equal(timeout('${ADMIN_TIMEOUT}', {}), true);
            assert.equal(timeout(' ${ADMIN_TIMEOUT}', undefined), false);
            assert.equal(timeout('5000', {}), true);
            assert.match(timeout('50', {}), /from 100 to 600000/);
        });

        it('help property lines have no extra ":" (Node-RED\'s help renderer drops the text before it)', function () {
            const help = /<script type="text\/markdown"[^>]*>([\s\S]*?)<\/script>/.exec(html)[1];
            assert.deepEqual(help.split('\n').filter((l) => /^: .*\(.*\) : .*:/.test(l)), []);
            for (const re of [/from its arrival/, /at least 15000 ms/, /fail at once/, /At most 1000\s+messages/, /internal error/,
                /stores them as numbers/, /between the low and the high offset/]) {
                assert.match(help, re);
            }
        });
    });
});
