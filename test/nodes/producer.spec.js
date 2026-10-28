'use strict';

/**
 * Tests of the kafkajs-nc-producer node (nodes/kafka-producer.js) with the
 * fake kafkajs (test/helpers/fake-kafkajs.js) and the real broker config node.
 *
 * The broker config uses the shortest backoff the broker allows (reconnect
 * 100 ms, auth/config retry 1000 ms after clamping), so outages and
 * recoveries take a fraction of a second.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir, sleep, waitFor, nextStatus, statusCalls } = require('../helpers/common');
const factory = require('../../lib/kafka-factory');
const queueRegistry = require('../../lib/queue-registry');
const { DiskQueue } = require('../../lib/disk-queue');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');

helper.init(require.resolve('node-red'));

const internals = producerNode._internals;
const ORIGINAL_TIMEOUTS = { ...producerNode.TIMEOUTS };
const ORIGINAL_LIMITS = { ...producerNode.LIMITS };

// ---------------------------------------------------------------- helpers

function brokerConfig(extra = {}) {
    return {
        id: 'b1',
        type: 'kafkajs-nc-broker',
        name: 'test broker',
        brokers: 'localhost:9092',
        clientId: 'test',
        authMechanism: 'none',
        tls: false,
        reconnectMin: 20,     // clamped to 100 by lib/client-config
        reconnectMax: 100,
        authRetryMin: 200,    // clamped to 1000
        logLevel: 'off',
        safetyNet: false,
        ...extra
    };
}

function producerConfig(extra = {}) {
    return {
        id: 'p1',
        type: 'kafkajs-nc-producer',
        name: 'test producer',
        broker: 'b1',
        topic: 'test-topic',
        queueMode: 'disk',
        wires: [['out']],
        ...extra
    };
}

/** Load broker + producer (+ extra nodes); resolves with the producer node. */
async function load(producerExtra = {}, { broker = {}, credentials = {}, extraNodes = [], withBroker = true } = {}) {
    const flow = [producerConfig(producerExtra), { id: 'out', type: 'helper' }, ...extraNodes];
    if (withBroker) flow.unshift(brokerConfig(broker));
    await helper.load([brokerNode, producerNode], flow, credentials);
    return helper.getNode('p1');
}

/**
 * Hand a message to the producer's input handler; resolves with
 * { err, sent: [messages passed to send()], msg } once done() is called.
 */
function produce(node, msg, timeout = 3000) {
    return new Promise((resolve, reject) => {
        const sent = [];
        const timer = setTimeout(() => {
            reject(new Error(`done() not called within ${timeout} ms for payload ${JSON.stringify(msg.payload)}`));
        }, timeout);
        node.producerRuntime.input(msg, (m) => sent.push(m), (err) => {
            clearTimeout(timer);
            resolve({ err: err || null, sent, msg });
        });
    });
}

function rt(node) {
    return node.producerRuntime;
}

async function connected(node, timeout = 3000) {
    await waitFor(() => rt(node).state === 'connected' && rt(node).mode === 'live' && !rt(node).flushing,
        { timeout, message: 'producer did not reach connected/live' });
}

function queueDir(userDir, name = 'p1') {
    return path.join(userDir, 'kafkajs-nc-queue', name);
}

function segFiles(dir) {
    if (!fs.existsSync(dir)) return [];
    return fs.readdirSync(dir).filter((f) => /^seg-\d{16}\.jsonl$/.test(f)).sort();
}

/** Every record line in the segment files (only meaningful while nothing was acked). */
function queuedRecords(dir) {
    const out = [];
    for (const f of segFiles(dir)) {
        const lines = fs.readFileSync(path.join(dir, f), 'utf8').split('\n').filter(Boolean);
        for (const line of lines) out.push(JSON.parse(line));
    }
    return out;
}

function deadLetters(dir) {
    const file = path.join(dir, 'dead-letter.jsonl');
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
}

/** Calls of node[method] (a spy shared by all nodes) made by `node`, as strings. */
function logged(node, method, re) {
    const spy = node[method];
    const out = [];
    for (let i = 0; i < spy.args.length; i++) {
        if (spy.thisValues[i] !== node) continue;
        const text = String(spy.args[i][0]);
        if (!re || re.test(text)) out.push(text);
    }
    return out;
}

/** Write a queue segment file by hand (as left by an earlier run or damaged). */
function writeSegment(dir, lines) {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'seg-0000000000000001.jsonl'), lines.map((l) => l + '\n').join(''));
}

function record(value, extra = {}) {
    return JSON.stringify({ v: 1, t: 'test-topic', tf: 0, ts: Date.now(), m: { v: { s: value }, ts: String(Date.now()) }, ...extra });
}

// ---------------------------------------------------------------- tests

describe('kafka producer node', function () {
    let fake;
    let userDir;

    beforeEach(function (done) {
        fake = createFakeKafkajs();
        factory.setKafkajs(fake);
        userDir = tmpDir('kafkajs-nc-producer-');
        helper.settings({ userDir });
        helper.startServer(done);
    });

    afterEach(async function () {
        await helper.unload();
        await helper.stopServer();
        Object.assign(producerNode.TIMEOUTS, ORIGINAL_TIMEOUTS);
        Object.assign(producerNode.LIMITS, ORIGINAL_LIMITS);
        factory.setKafkajs(null);
        queueRegistry._reset();
    });

    describe('sending while connected', function () {
        it('sends a message and reports partition and offset', async function () {
            const node = await load();
            await connected(node);
            const { err, sent, msg } = await produce(node, { payload: 'hello', topic: 'ignored' });
            assert.equal(err, null);
            assert.equal(sent.length, 1);
            assert.equal(sent[0], msg, 'the input msg is passed on');
            assert.equal(msg.kafka.status, 'sent');
            assert.equal(msg.kafka.topic, 'test-topic');
            assert.equal(msg.kafka.partition, 0);
            assert.equal(msg.kafka.offset, '0');
            assert.equal(msg.kafka.results.length, 1);
            assert.equal(msg.kafka.results[0].topicName, 'test-topic');

            assert.equal(fake.sent.length, 1);
            assert.equal(fake.sent[0].value, 'hello');
            assert.equal(fake.sent[0].topic, 'test-topic');
            assert.match(fake.sent[0].timestamp, /^\d+$/, 'the arrival time is the Kafka timestamp');

            const p = fake.producers[0];
            assert.equal(typeof p.options.createPartitioner, 'function', 'partitioner passed explicitly');
            assert.equal(p.options.allowAutoTopicCreation, true);
            assert.deepEqual(p.options.retry, { retries: 0 }, 'no kafkajs produce retry (LOSS-1)');
            assert.equal(p.options.idempotent, undefined);
            const args = p.sendCalls[0].args;
            assert.equal(args.acks, -1);
            assert.equal(args.timeout, 30000);
            assert.equal(args.compression, 0);
            await nextStatus(node, (s) => s.text === 'connected' && s.fill === 'green');
        });

        it('passes the delivery report to the next node and calls done() (Node-RED wiring)', async function () {
            const node = await load({}, {
                extraNodes: [
                    { id: 'cmp', type: 'complete', scope: ['p1'], uncaught: false, wires: [['completed']] },
                    { id: 'completed', type: 'helper' }
                ]
            });
            await connected(node);
            const out = new Promise((resolve) => helper.getNode('out').once('input', resolve));
            const completed = new Promise((resolve) => helper.getNode('completed').once('input', resolve));
            node.receive({ payload: { a: 1 }, topic: 'x' });
            const msg = await out;
            assert.equal(msg.kafka.status, 'sent');
            assert.deepEqual(msg.payload, { a: 1 });
            const done = await completed;
            assert.deepEqual(done.payload, { a: 1 });
            assert.equal(fake.sent[0].value, '{"a":1}');
        });

        it('a failing message reaches a catch node with the original msg', async function () {
            const node = await load({ topic: '' }, {
                extraNodes: [
                    { id: 'c1', type: 'catch', scope: null, uncaught: false, wires: [['caught']] },
                    { id: 'caught', type: 'helper' }
                ]
            });
            await connected(node);
            const caught = new Promise((resolve) => helper.getNode('caught').once('input', resolve));
            node.receive({ payload: 'no topic', marker: 42 });
            const msg = await caught;
            assert.equal(msg.marker, 42);
            assert.equal(msg.payload, 'no topic');
            assert.match(msg.error.message, /No topic: set the node Topic or msg\.topic/);
            assert.equal(msg.error.source.id, 'p1');
            assert.equal(fake.sent.length, 0);
        });

        it('converts every payload type; undefined is an error', async function () {
            const node = await load();
            await connected(node);
            const buf = Buffer.from([0, 1, 2, 255]);
            const cases = [
                ['text', 'text'],
                [buf, buf],
                [0, '0'],
                [false, 'false'],
                ['', ''],
                [{ a: [1, 2] }, '{"a":[1,2]}'],
                [[1, 'x'], '[1,"x"]'],
                [12.5, '12.5'],
                [null, null]
            ];
            for (const [payload] of cases) {
                const { err, msg } = await produce(node, { payload });
                assert.equal(err, null, `payload ${String(payload)}`);
                assert.equal(msg.kafka.status, 'sent');
            }
            assert.deepEqual(fake.sent.map((s) => s.value), cases.map((c) => c[1]));
            assert.ok(Buffer.isBuffer(fake.sent[1].value));
            assert.equal(fake.sent[8].value, null, 'null is a tombstone');

            const bad = await produce(node, { payload: undefined });
            assert.ok(bad.err);
            assert.equal(bad.err.name, 'KafkaNcMessageError');
            assert.match(bad.err.message, /msg\.payload is undefined/);
            const circular = {};
            circular.self = circular;
            const bad2 = await produce(node, { payload: circular });
            assert.match(bad2.err.message, /cannot be converted to JSON/);
            assert.equal(fake.sent.length, cases.length, 'invalid messages are not sent');
            assert.equal(rt(node).queuedCount(), 0, 'invalid messages are not queued');
        });

        it('sends msg.messages as one batch with per-element topic, key, headers and partition', async function () {
            const node = await load({ topic: '' });
            await connected(node);
            const { err, msg } = await produce(node, {
                topic: 'main',
                payload: 'ignored',
                messages: [
                    { value: 'a', key: 'k1' },
                    { payload: { n: 2 }, headers: { h: 'v', n: 7 } },
                    { value: 'c', topic: 'other', partition: 3 },
                    { value: 'd', timestamp: 1700000000000 }
                ]
            });
            assert.equal(err, null);
            assert.equal(msg.kafka.status, 'sent');
            assert.equal(msg.kafka.count, 4);
            assert.equal(msg.kafka.topic, 'main');
            assert.deepEqual(fake.sent.map((s) => [s.topic, s.value]), [
                ['main', 'a'], ['main', '{"n":2}'], ['other', 'c'], ['main', 'd']
            ]);
            assert.equal(fake.sent[0].key, 'k1');
            assert.deepEqual(fake.sent[1].headers, { h: 'v', n: '7' });
            assert.equal(fake.sent[2].partition, 3);
            assert.equal(fake.sent[3].timestamp, '1700000000000');
            // one request per run of the same topic
            assert.deepEqual(fake.producers[0].sendCalls.map((c) => c.args.topic), ['main', 'other', 'main']);
        });

        it('rejects invalid msg.messages', async function () {
            const node = await load();
            await connected(node);
            const empty = await produce(node, { messages: [] });
            assert.match(empty.err.message, /msg\.messages is an empty array/);
            const notObject = await produce(node, { messages: [{ value: 'ok' }, 'x'] });
            assert.match(notObject.err.message, /msg\.messages\[1\] must be an object/);
            const noValue = await produce(node, { messages: [{ value: 'ok' }, { key: 'k' }] });
            assert.match(noValue.err.message, /msg\.messages\[1\]\.value is undefined/);
            const badPartition = await produce(node, { messages: [{ value: 'ok', partition: -1 }] });
            assert.match(badPartition.err.message, /msg\.messages\[0\]\.partition must be an integer/);
            assert.equal(fake.sent.length, 0, 'nothing of an invalid batch is sent');
        });

        it('node settings win over msg properties; msg properties are used when settings are empty', async function () {
            const node = await load({
                topic: 'cfg-topic', key: 'cfg-key', partition: '1',
                headers: { fixed: 'f', shared: 'from-config' }
            });
            await connected(node);
            await produce(node, {
                payload: 'x', topic: 'msg-topic', key: 'msg-key', partition: 2,
                headers: { shared: 'from-msg', extra: 'e' }
            });
            const s = fake.sent[0];
            assert.equal(s.topic, 'cfg-topic');
            assert.equal(s.key, 'cfg-key');
            assert.equal(s.partition, 1);
            assert.deepEqual(s.headers, { shared: 'from-config', extra: 'e', fixed: 'f' });
        });

        it('uses msg.topic/key/partition/headers when the settings are empty', async function () {
            const node = await load({ topic: '', key: '', partition: '' });
            await connected(node);
            const { msg } = await produce(node, {
                payload: 'x', topic: 'msg-topic', key: 42, partition: '2', headers: { a: 1, skip: null }
            });
            const s = fake.sent[0];
            assert.equal(s.topic, 'msg-topic');
            assert.equal(s.key, '42');
            assert.equal(s.partition, 2);
            assert.deepEqual(s.headers, { a: '1' });
            assert.equal(msg.kafka.partition, 2);
        });

        it('ignores msg.headers when useMsgHeaders is off', async function () {
            const node = await load({ useMsgHeaders: false, headers: { only: 'static' } });
            await connected(node);
            await produce(node, { payload: 'x', headers: { 'content-length': 12 } });
            assert.deepEqual(fake.sent[0].headers, { only: 'static' });
        });

        it('rejects missing and invalid topics without sending', async function () {
            const node = await load({ topic: '' });
            await connected(node);
            const none = await produce(node, { payload: 'x' });
            assert.match(none.err.message, /No topic/);
            const mqttStyle = await produce(node, { payload: 'x', topic: 'sensors/temp' });
            assert.match(mqttStyle.err.message, /invalid Kafka topic name "sensors\/temp"/);
            const notString = await produce(node, { payload: 'x', topic: 5 });
            assert.match(notString.err.message, /msg\.topic must be a string/);
            assert.equal(fake.sent.length, 0);
        });

        it('reports invalid settings (unset env variable, bad partition) on every message', async function () {
            const node = await load({ topic: '${KAFKA_TOPIC_NOT_SET}', partition: 'abc' });
            await waitFor(() => logged(node, 'error', /environment variable KAFKA_TOPIC_NOT_SET is not set/).length === 1);
            assert.equal(logged(node, 'error', /Partition setting/).length, 1);
            await connected(node);
            const r = await produce(node, { payload: 'x' });
            assert.match(r.err.message, /environment variable KAFKA_TOPIC_NOT_SET is not set/);
            assert.equal(fake.sent.length, 0);
        });

        for (const [setting, expected] of [['all', -1], ['leader', 1], ['none', 0], ['bogus', -1]]) {
            it(`maps acks "${setting}" to ${expected} (never null)`, async function () {
                const node = await load({ acks: setting });
                await connected(node);
                const { err, msg } = await produce(node, { payload: 'x' });
                assert.equal(err, null);
                const args = fake.producers[0].sendCalls[0].args;
                assert.equal(args.acks, expected);
                assert.equal(msg.kafka.status, 'sent');
                if (expected === 0) assert.deepEqual(msg.kafka.results, [], 'acks 0: kafkajs returns no metadata');
                if (setting === 'bogus') assert.equal(logged(node, 'warn', /Unknown acks/).length, 1);
            });
        }

        it('passes ack timeout, compression and the partitioner to kafkajs', async function () {
            const node = await load({ ackTimeout: '1234', compression: 'gzip', acks: 'leader', partitioner: 'legacy' });
            await connected(node);
            await produce(node, { payload: 'x' });
            const p = fake.producers[0];
            assert.equal(p.options.createPartitioner, fake.Partitioners.LegacyPartitioner);
            const args = p.sendCalls[0].args;
            assert.equal(args.timeout, 1234);
            assert.equal(args.compression, fake.CompressionTypes.GZIP);
            assert.equal(args.acks, 1);
        });

        it('an old flow with "idempotent" on still works: the option is ignored with one warning', async function () {
            // kafkajs' idempotent mode needs its own produce retry, which can lose messages (LOSS-1)
            const node = await load({ idempotent: true, acks: 'leader' });
            await connected(node);
            const r = await produce(node, { payload: 'x' });
            assert.equal(r.msg.kafka.status, 'sent');
            const p = fake.producers[0];
            assert.equal(p.options.idempotent, undefined);
            assert.equal(p.options.maxInFlightRequests, undefined);
            assert.deepEqual(p.options.retry, { retries: 0 });
            assert.equal(p.sendCalls[0].args.acks, 1, 'acks is no longer forced to "all"');
            assert.equal(logged(node, 'warn', /"Idempotent producer" option is no longer supported and is ignored/).length, 1);
        });

        it('fails a message whose explicit partition does not exist (kafkajs silently writes nothing)', async function () {
            fake.behaviour.partitionCount = 2;
            const node = await load({ topic: 'test-topic' });
            await connected(node);
            const missing = await produce(node, { payload: 'x', partition: 5 });
            assert.ok(missing.err);
            assert.equal(missing.err.name, 'KafkaNcMessageError');
            assert.match(missing.err.message, /Partition 5 does not exist for topic test-topic/);
            const ok = await produce(node, { payload: 'y', partition: 1 });
            assert.equal(ok.err, null);
            assert.equal(ok.msg.kafka.partition, 1);
            assert.equal(ok.msg.kafka.offset, '0');
            assert.deepEqual(fake.sent.map((s) => s.value), ['y']);
            assert.equal(rt(node).queuedCount(), 0, 'not queued');
        });

        it('batches messages that arrive while a request is in flight, in order, one request at a time', async function () {
            fake.behaviour.sendDelayMs = 30;
            const node = await load({ batchSize: 3 });
            await connected(node);
            const results = await Promise.all(Array.from({ length: 10 }, (_, i) => produce(node, { payload: `m${i}` })));
            assert.ok(results.every((r) => r.err === null && r.msg.kafka.status === 'sent'));
            assert.deepEqual(fake.sent.map((s) => s.value), Array.from({ length: 10 }, (_, i) => `m${i}`));
            const p = fake.producers[0];
            assert.equal(p.maxInFlight, 1, 'one request in flight at a time');
            assert.ok(p.sendCalls.every((c) => c.args.messages.length <= 3), 'batch size respected');
            assert.ok(p.sendCalls.length < 10, 'messages were batched');
            assert.deepEqual(results.map((r) => r.msg.kafka.offset), Array.from({ length: 10 }, (_, i) => String(i)));
        });

        it('a MESSAGE error fails only the bad message (batch isolated), nothing is queued', async function () {
            fake.behaviour.sendDelayMs = 20;
            fake.behaviour.sendError = (args) => (args.messages.some((m) => m.value === 'big') ? fake.errors.messageTooLarge() : null);
            const node = await load();
            await connected(node);
            const first = produce(node, { payload: 'm0' });  // occupies the in-flight slot
            const rest = ['ok1', 'big', 'ok2'].map((payload) => produce(node, { payload }));
            const [r0, ok1, big, ok2] = await Promise.all([first, ...rest]);
            assert.equal(r0.err, null);
            assert.equal(ok1.err, null);
            assert.equal(ok2.err, null);
            assert.ok(big.err);
            assert.equal(big.err.type, 'MESSAGE_TOO_LARGE');
            assert.deepEqual(fake.sent.map((s) => s.value), ['m0', 'ok1', 'ok2']);
            assert.equal(rt(node).queuedCount(), 0);
            assert.equal(rt(node).state, 'connected', 'a bad message does not drop the connection');
            assert.equal(segFiles(queueDir(userDir)).length, 0);
        });

        it('a topic error is a message error when the topic came from msg.topic', async function () {
            fake.behaviour.sendError = (args) => (args.topic === 'forbidden' ? fake.errors.topicAuthorizationFailed() : null);
            const node = await load({ topic: '' });
            await connected(node);
            const r = await produce(node, { payload: 'x', topic: 'forbidden' });
            assert.ok(r.err);
            assert.match(r.err.message, /TOPIC_AUTHORIZATION_FAILED|authoriz/i);
            const ok = await produce(node, { payload: 'y', topic: 'allowed' });
            assert.equal(ok.err, null);
            assert.equal(rt(node).state, 'connected');
        });

        it('listens to request timeouts through a crash-safe listener', async function () {
            const node = await load();
            await connected(node);
            const p = fake.producers[0];
            assert.equal(p.listenerCount(p.events.REQUEST_TIMEOUT), 1);
            p.emit(p.events.REQUEST_TIMEOUT, { broker: 'localhost:9092', apiName: 'Produce' });
            p.emit(p.events.REQUEST_TIMEOUT, undefined);
            assert.equal(p.listenerErrors.length, 0);
        });
    });

    describe('offline queue (store-and-forward)', function () {
        it('queues to disk while the broker is unreachable, then flushes in exact order', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            const reports = [];
            for (let i = 0; i < 5; i++) {
                const { err, sent, msg } = await produce(node, { payload: `offline-${i}`, key: `k${i}` });
                assert.equal(err, null, 'done() without error: the message is safe');
                assert.equal(sent.length, 1);
                reports.push(msg.kafka);
            }
            assert.deepEqual(reports.map((k) => k.status), Array(5).fill('queued'));
            assert.deepEqual(reports.map((k) => k.queueSize), [1, 2, 3, 4, 5]);
            assert.equal(reports[0].topic, 'test-topic');

            const dir = queueDir(userDir);
            assert.ok(segFiles(dir).length >= 1, `segment files in ${dir}`);
            const stored = queuedRecords(dir);
            assert.deepEqual(stored.map((r) => r.m.v.s), ['offline-0', 'offline-1', 'offline-2', 'offline-3', 'offline-4']);
            assert.deepEqual(stored.map((r) => r.m.k.s), ['k0', 'k1', 'k2', 'k3', 'k4']);
            assert.ok(stored.every((r) => r.t === 'test-topic' && r.v === 1 && Number.isFinite(r.ts)));

            await nextStatus(node, (s) => /^offline · 5 queued · retry/.test(s.text) && s.fill === 'yellow');
            assert.equal(fake.sent.length, 0);
            assert.ok(logged(node, 'error', /Kafka connection failed: .*ECONNREFUSED/).length >= 1);

            // the broker comes back
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 5, { message: 'backlog flushed' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['offline-0', 'offline-1', 'offline-2', 'offline-3', 'offline-4']);
            assert.deepEqual(fake.sent.map((s) => s.key), ['k0', 'k1', 'k2', 'k3', 'k4']);
            await connected(node);
            await waitFor(() => segFiles(dir).length === 0, { message: 'segments removed once drained' });
            await nextStatus(node, (s) => s.text === 'connected');
            assert.ok(statusCalls(node).some((s) => /^flushing · \d+ queued$/.test(s.text)), 'flushing status shown');
            assert.ok(logged(node, 'log', /Connected to Kafka after \d+ attempts/).length >= 1);
            assert.ok(logged(node, 'log', /Sent 5 queued message\(s\)/).length === 1);

            // and live again
            const live = await produce(node, { payload: 'live' });
            assert.equal(live.msg.kafka.status, 'sent');
            assert.equal(fake.sent[5].value, 'live');
        });

        it('keeps the original time of queued messages as the Kafka timestamp', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            const before = Date.now();
            await produce(node, { payload: 'old' });
            await sleep(30);
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1);
            const ts = Number(fake.sent[0].timestamp);
            assert.ok(ts >= before && ts <= before + 25, 'timestamp = arrival time, not flush time');
        });

        it('queues messages arriving during the flush after the backlog (global order kept)', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ batchSize: 5 });
            await waitFor(() => rt(node).state === 'waiting');
            for (let i = 0; i < 30; i++) await produce(node, { payload: `old-${i}` });
            fake.behaviour.sendDelayMs = 15;
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length >= 5 && rt(node).flushing, { message: 'flush started' });
            const during = [];
            for (let i = 0; i < 10; i++) {
                assert.equal(rt(node).mode, 'backlog');
                during.push(await produce(node, { payload: `new-${i}` }));
            }
            assert.ok(during.every((r) => r.err === null && r.msg.kafka.status === 'queued'), 'queued behind the backlog');
            await waitFor(() => fake.sent.length === 40, { message: 'all sent' });
            const expected = [...Array.from({ length: 30 }, (_, i) => `old-${i}`), ...Array.from({ length: 10 }, (_, i) => `new-${i}`)];
            assert.deepEqual(fake.sent.map((s) => s.value), expected);
            await connected(node);
            const after = await produce(node, { payload: 'after' });
            assert.equal(after.msg.kafka.status, 'sent');
            assert.equal(fake.producers[fake.producers.length - 1].maxInFlight, 1);
        });

        it('a transient send failure while live moves the failed batch and all pending messages to the queue, in order', async function () {
            fake.behaviour.sendDelayMs = 40;
            let failNext = true;
            fake.behaviour.sendError = () => {
                if (!failNext) return null;
                failNext = false;
                return fake.errors.sendDuringOutage();
            };
            const node = await load();
            await connected(node);
            const first = produce(node, { payload: 'm1' }); // in flight, will fail
            const second = produce(node, { payload: 'm2' }); // pending
            const third = produce(node, { payload: 'm3' }); // pending
            const results = await Promise.all([first, second, third]);
            assert.deepEqual(results.map((r) => r.err), [null, null, null]);
            assert.deepEqual(results.map((r) => r.msg.kafka.status), ['queued', 'queued', 'queued']);
            assert.ok(logged(node, 'error', /Kafka send failed: .*kept in the offline queue/).length === 1);

            let fourth;
            if (rt(node).state === 'waiting') {
                fourth = await produce(node, { payload: 'm4' });
                assert.equal(fourth.msg.kafka.status, 'queued');
            } else {
                fourth = await produce(node, { payload: 'm4' });
            }
            assert.equal(fourth.err, null);
            await waitFor(() => fake.sent.length === 4, { message: 'everything delivered after reconnecting' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2', 'm3', 'm4']);
            assert.ok(fake.producers.length >= 2, 'a new producer was created to reconnect');
            assert.equal(fake.producers[0].state, 'disconnected', 'the broken producer was disconnected');
            await connected(node);
        });

        it('moves a poison message to dead-letter.jsonl and keeps flushing', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            fake.behaviour.sendError = (args) => (args.messages.some((m) => m.value === 'poison') ? fake.errors.messageTooLarge() : null);
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            for (const payload of ['a', 'poison', 'b']) {
                const r = await produce(node, { payload });
                assert.equal(r.msg.kafka.status, 'queued');
            }
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live', { message: 'flushed around the poison' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b']);
            const dead = deadLetters(queueDir(userDir));
            assert.equal(dead.length, 1);
            assert.equal(dead[0].record.m.v.s, 'poison');
            assert.equal(dead[0].error.kind, 'message');
            assert.match(dead[0].error.message, /MESSAGE_TOO_LARGE|too large/i);
            assert.ok(logged(node, 'warn', /dead-letter\.jsonl/).length === 1);
            assert.equal(rt(node).state, 'connected', 'a poison message does not drop the connection');
            const stats = await produce(node, { action: 'queue-status' });
            assert.equal(stats.msg.payload.deadLettered, 1);
            assert.equal(stats.msg.payload.size, 0);
        });

        it('dead-letters corrupt queue lines left on disk and sends the rest', async function () {
            writeSegment(queueDir(userDir), [record('first'), '{"this is": not json', record('second'), JSON.stringify({ v: 99, t: 'x' })]);
            const node = await load();
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live', { message: 'flushed' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['first', 'second']);
            const dead = deadLetters(queueDir(userDir));
            assert.equal(dead.length, 2);
            assert.equal(dead[0].raw, '{"this is": not json');
            assert.match(dead[0].error.message, /Corrupt queue record/);
            assert.deepEqual(dead[1].record, { v: 99, t: 'x' });
            assert.ok(logged(node, 'log', /holds 4 message\(s\) from earlier/).length === 1);
        });

        it('dead-letters a record that keeps failing with an UNKNOWN error after 5 attempts', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            let weirdAttempts = 0;
            fake.behaviour.sendError = (args) => {
                if (!args.messages.some((m) => m.value === 'weird')) return null;
                weirdAttempts++;
                return fake.makeError('WeirdError', 'something odd happened');
            };
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'weird' });
            await produce(node, { payload: 'after' });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1 && rt(node).mode === 'live', { timeout: 4000, message: 'unblocked' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['after']);
            assert.ok(weirdAttempts >= 5, `attempts: ${weirdAttempts}`);
            const dead = deadLetters(queueDir(userDir));
            assert.equal(dead.length, 1);
            assert.equal(dead[0].record.m.v.s, 'weird');
            assert.equal(dead[0].error.kind, 'unknown');
            // repeated identical failures are not logged as errors every time
            assert.ok(logged(node, 'error', /Sending queued messages to Kafka failed/).length <= 2);
        });

        it('moves expired queued messages to the dead-letter file', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMaxAgeHours: 0.0001 }); // 360 ms
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'stale' });
            await sleep(450);
            await produce(node, { payload: 'fresh' });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => s.value), ['fresh']);
            const dead = deadLetters(queueDir(userDir));
            assert.equal(dead.length, 1);
            assert.equal(dead[0].record.m.v.s, 'stale');
            assert.match(dead[0].error, /^expired/);
        });

        it('dead-letters queued messages for a partition that does not exist', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            fake.behaviour.partitionCount = 1;
            const node = await load({ topic: '' });
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a', topic: 't1' });
            await produce(node, { payload: 'nope', topic: 't1', partition: 7 });
            await produce(node, { payload: 'b', topic: 't1' });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b']);
            const dead = deadLetters(queueDir(userDir));
            assert.equal(dead.length, 1);
            assert.match(dead[0].error.message, /Partition 7 does not exist/);
        });

        it('sends queued messages for several topics in order, one topic per request', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ topic: '' });
            await waitFor(() => rt(node).state === 'waiting');
            const plan = [['a', 't1'], ['b', 't1'], ['c', 't2'], ['d', 't1']];
            for (const [payload, topic] of plan) await produce(node, { payload, topic });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 4);
            assert.deepEqual(fake.sent.map((s) => [s.value, s.topic]), plan);
            const p = fake.producers[fake.producers.length - 1];
            assert.deepEqual(p.sendCalls.map((c) => c.args.topic), ['t1', 't2', 't1']);
        });

        it('the queue survives a restart: unload, load again with the same node id and user directory', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            let node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            for (let i = 0; i < 3; i++) await produce(node, { payload: `persisted-${i}` });
            await helper.unload();
            assert.equal(queuedRecords(queueDir(userDir)).length, 3, 'still on disk after close');
            assert.equal(queueRegistry.isOwned(queueDir(userDir)), false, 'directory released');

            delete fake.behaviour.connectError;
            node = await load();
            await waitFor(() => fake.sent.length === 3, { message: 'backlog of the previous run flushed' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['persisted-0', 'persisted-1', 'persisted-2']);
            await connected(node);
        });

        it('memory mode queues in memory (no files) and flushes in order', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMode: 'memory' });
            await waitFor(() => rt(node).state === 'waiting');
            for (const payload of ['x', 'y', 'z']) {
                const r = await produce(node, { payload });
                assert.equal(r.msg.kafka.status, 'queued');
            }
            assert.equal(fs.existsSync(path.join(userDir, 'kafkajs-nc-queue')), false, 'nothing written to disk');
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 3);
            assert.deepEqual(fake.sent.map((s) => s.value), ['x', 'y', 'z']);
        });

        it('off mode fails messages while not connected and sends them when connected', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMode: 'off' });
            await waitFor(() => rt(node).state === 'waiting');
            const r = await produce(node, { payload: 'lost' });
            assert.ok(r.err);
            assert.match(r.err.message, /Kafka is not connected and the offline queue is off/);
            assert.equal(fs.existsSync(path.join(userDir, 'kafkajs-nc-queue')), false);
            await nextStatus(node, (s) => /ECONNREFUSED .* · retry/.test(s.text));
            delete fake.behaviour.connectError;
            await connected(node);
            const ok = await produce(node, { payload: 'sent' });
            assert.equal(ok.msg.kafka.status, 'sent');
        });

        it('off mode: a transient live failure fails the batch and pending messages', async function () {
            fake.behaviour.sendDelayMs = 30;
            fake.behaviour.sendError = fake.errors.sendDuringOutage();
            const node = await load({ queueMode: 'off' });
            await connected(node);
            const results = await Promise.all(['a', 'b'].map((payload) => produce(node, { payload })));
            assert.ok(results.every((r) => r.err && /ECONNREFUSED|Connection error/.test(r.err.message)));
            delete fake.behaviour.sendError;
            await connected(node);
        });

        it('a full queue with drop-newest rejects new messages with done(err)', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMaxMessages: 2, queueOverflow: 'drop-newest' });
            await waitFor(() => rt(node).state === 'waiting');
            assert.equal((await produce(node, { payload: 'a' })).err, null);
            assert.equal((await produce(node, { payload: 'b' })).err, null);
            const full = await produce(node, { payload: 'c' });
            assert.ok(full.err);
            assert.equal(full.err.name, 'KafkaNcQueueFullError');
            assert.equal(full.sent.length, 0);
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2);
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b']);
        });

        it('a full queue with drop-oldest drops the oldest message and warns', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMaxMessages: 2, queueOverflow: 'drop-oldest' });
            await waitFor(() => rt(node).state === 'waiting');
            for (const payload of ['a', 'b', 'c']) assert.equal((await produce(node, { payload })).err, null);
            assert.ok(logged(node, 'warn', /dropped 1 oldest/i).length >= 1);
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2);
            assert.deepEqual(fake.sent.map((s) => s.value), ['b', 'c']);
        });

        it('a slow Kafka request sends new messages to the queue; order is kept', async function () {
            producerNode.TIMEOUTS.liveStall = 40;
            fake.behaviour.sendDelayMs = 150;
            const node = await load();
            await connected(node);
            const first = produce(node, { payload: 'm1' });
            await sleep(80);
            const second = await produce(node, { payload: 'm2' });
            assert.equal(second.msg.kafka.status, 'queued', 'stored durably while Kafka is slow');
            assert.equal((await first).msg.kafka.status, 'sent');
            fake.behaviour.sendDelayMs = 0;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2']);
        });

        it('a slow request that then fails: it is sent before the messages queued meanwhile', async function () {
            producerNode.TIMEOUTS.liveStall = 40;
            fake.behaviour.sendDelayMs = 150;
            let failNext = true;
            fake.behaviour.sendError = () => {
                if (!failNext) return null;
                failNext = false;
                return fake.errors.sendDuringOutage();
            };
            const node = await load();
            await connected(node);
            const first = produce(node, { payload: 'm1' });
            await sleep(80);
            const second = await produce(node, { payload: 'm2' });
            assert.equal(second.msg.kafka.status, 'queued');
            const r1 = await first;
            assert.equal(r1.err, null);
            assert.equal(r1.msg.kafka.status, 'queued', 'the failed in-flight batch was already queued, in front');
            fake.behaviour.sendDelayMs = 0;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2']);
        });

        it('too many messages waiting in memory: the rest go to the queue, order kept, no duplicates', async function () {
            producerNode.LIMITS.maxPending = 2;
            fake.behaviour.sendDelayMs = 40;
            const node = await load({ batchSize: 1 });
            await connected(node);
            const all = ['m1', 'm2', 'm3', 'm4', 'm5'].map((payload) => produce(node, { payload }));
            const results = await Promise.all(all);
            assert.deepEqual(results.map((r) => r.msg.kafka.status), ['sent', 'queued', 'queued', 'queued', 'queued']);
            await waitFor(() => fake.sent.length >= 5 && rt(node).mode === 'live');
            await sleep(30);
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2', 'm3', 'm4', 'm5']);
        });

        it('close during a slow request that was already queued does not queue it twice', async function () {
            producerNode.TIMEOUTS.liveStall = 30;
            producerNode.TIMEOUTS.closeSendWait = 80;
            producerNode.TIMEOUTS.disconnect = 50;
            fake.behaviour.sendDelayMs = Infinity;
            const node = await load();
            await connected(node);
            const first = produce(node, { payload: 'm1' });
            await sleep(60);
            const second = await produce(node, { payload: 'm2' });
            assert.equal(second.msg.kafka.status, 'queued');
            await helper.unload();
            const r1 = await first;
            assert.equal(r1.msg.kafka.status, 'queued');
            assert.deepEqual(queuedRecords(queueDir(userDir)).map((r) => r.m.v.s), ['m1', 'm2']);
            fake.behaviour.sendDelayMs = 0;
            await load();
            await waitFor(() => fake.sent.length === 2);
            await sleep(30);
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2']);
        });

        it('drop-oldest removing the queued copy of a slow request never acks someone else\'s record', async function () {
            producerNode.TIMEOUTS.liveStall = 30;
            fake.behaviour.sendDelayMs = (args) => (args.messages[0].value === 'm1' ? 150 : 0);
            const node = await load({ queueMaxMessages: 2, queueOverflow: 'drop-oldest' });
            await connected(node);
            const first = produce(node, { payload: 'm1' });
            await sleep(60);
            await produce(node, { payload: 'm2' }); // switch: queue = [m1 (copy), m2]
            await produce(node, { payload: 'm3' }); // drop-oldest drops the m1 copy: [m2, m3]
            assert.equal((await first).msg.kafka.status, 'sent');
            await waitFor(() => fake.sent.length >= 3 && rt(node).mode === 'live');
            await sleep(30);
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2', 'm3'], 'm2 was not acked by mistake');
        });

        it('queue off: too many messages waiting in memory fail with done(err)', async function () {
            producerNode.LIMITS.maxPending = 2;
            fake.behaviour.sendDelayMs = 40;
            const node = await load({ queueMode: 'off', batchSize: 1 });
            await connected(node);
            const results = await Promise.all(['m1', 'm2', 'm3', 'm4'].map((payload) => produce(node, { payload })));
            assert.deepEqual(results.map((r) => (r.err ? 'err' : r.msg.kafka.status)), ['sent', 'sent', 'sent', 'err']);
            assert.match(results[3].err.message, /Too many messages \(2\) are waiting/);
        });

        it('uses settings.kafkajsNcProducerQueueDir and the queue name', async function () {
            helper.settings({ userDir, kafkajsNcProducerQueueDir: 'custom-queues' });
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueName: 'my queue/../x' });
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'q' });
            const dir = path.join(userDir, 'custom-queues', 'my_queue_.._x');
            assert.equal(queuedRecords(dir).length, 1, `stored in ${dir}`);
            const stats = await produce(node, { action: 'queue-status' });
            assert.equal(stats.msg.payload.queueDir, dir);
        });

        it('a second producer with the same queue directory runs without a queue and says so', async function () {
            const flowExtra = [producerConfig({ id: 'p2', queueName: 'shared', wires: [[]] })];
            const node = await load({ queueName: 'shared' }, { extraNodes: flowExtra });
            const other = helper.getNode('p2');
            await connected(node);
            await waitFor(() => rt(other).state === 'connected');
            const failed = [node, other].filter((n) => rt(n).queueState === 'failed');
            assert.equal(failed.length, 1, 'exactly one of them got the directory');
            assert.equal(logged(failed[0], 'error', /already used by node/).length, 1);
            await nextStatus(failed[0], (s) => s.text === 'connected · queue unavailable');
            const r = await produce(failed[0], { payload: 'still works' });
            assert.equal(r.msg.kafka.status, 'sent');
        });

        it('queues messages that arrive before the queue is open and keeps their order', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            const early = rt(node).queueState === 'pending';
            const results = await Promise.all(['e1', 'e2', 'e3'].map((payload) => produce(node, { payload })));
            assert.ok(results.every((r) => r.err === null && r.msg.kafka.status === 'queued'));
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 3);
            assert.deepEqual(fake.sent.map((s) => s.value), ['e1', 'e2', 'e3']);
            if (!early) this.test.title += ' (queue was already open)';
        });
    });

    describe('robustness', function () {
        it('a msg.messages batch larger than the batch size is split; one report for the input', async function () {
            const node = await load({ batchSize: 2 });
            await connected(node);
            const { err, msg } = await produce(node, { messages: [1, 2, 3, 4, 5].map((n) => ({ value: n })) });
            assert.equal(err, null);
            assert.equal(msg.kafka.status, 'sent');
            assert.equal(msg.kafka.count, 5);
            assert.deepEqual(fake.sent.map((s) => s.value), ['1', '2', '3', '4', '5']);
            assert.deepEqual(fake.producers[0].sendCalls.map((c) => c.args.messages.length), [2, 2, 1]);
            assert.equal(msg.kafka.results.length, 3, 'results of every request that carried it');
        });

        it('a multi-topic msg.messages partly sent before an outage: the rest is queued, nothing twice', async function () {
            let fail = false;
            fake.behaviour.sendError = (args) => (fail && args.topic === 't2' ? fake.errors.sendDuringOutage() : null);
            const node = await load({ topic: '' });
            await connected(node);
            fail = true;
            const { err, msg } = await produce(node, {
                messages: [{ topic: 't1', value: 'a' }, { topic: 't2', value: 'b' }, { topic: 't1', value: 'c' }]
            });
            assert.equal(err, null);
            assert.equal(msg.kafka.status, 'queued');
            assert.deepEqual(queuedRecords(queueDir(userDir)).map((r) => [r.t, r.m.v.s]), [['t2', 'b'], ['t1', 'c']]);
            fail = false;
            await waitFor(() => fake.sent.length === 3 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => [s.topic, s.value]), [['t1', 'a'], ['t2', 'b'], ['t1', 'c']]);
        });

        it('a queue read error while flushing is logged and retried; nothing is lost', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            await produce(node, { payload: 'b' });
            const queue = rt(node).queue;
            const peek = queue.peek.bind(queue);
            let failures = 1;
            queue.peek = (n) => {
                if (failures-- > 0) return Promise.reject(Object.assign(new Error('EIO: i/o error, read (simulated)'), { code: 'EIO' }));
                return peek(n);
            };
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live', { message: 'flushed after the retry' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b']);
            assert.equal(logged(node, 'error', /Offline queue error while sending queued messages: .*EIO/).length, 1);
        });

        it('an acknowledgement that does not advance the queue backs off instead of spinning', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            const queue = rt(node).queue;
            const ack = queue.ack.bind(queue);
            let broken = 1;
            queue.ack = (entries) => (broken-- > 0 ? Promise.resolve(0) : ack(entries));
            delete fake.behaviour.connectError;
            await waitFor(() => rt(node).mode === 'live' && queue.size === 0, { message: 'recovered' });
            assert.equal(logged(node, 'error', /did not advance/).length, 1);
            assert.ok(fake.sent.length >= 1 && fake.sent.length <= 2, 'at most one duplicate');
            assert.ok(fake.sent.every((m) => m.value === 'a'));
        });

        it('a dead-letter write failure is logged with the record and does not block the queue', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            fake.behaviour.sendError = (args) => (args.messages.some((m) => m.value === 'poison') ? fake.errors.messageTooLarge() : null);
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'poison' });
            await produce(node, { payload: 'ok' });
            rt(node).queue.deadLetter = () => Promise.reject(new Error('ENOSPC: no space left on device (simulated)'));
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1 && rt(node).mode === 'live');
            assert.deepEqual(fake.sent.map((s) => s.value), ['ok']);
            assert.equal(logged(node, 'error', /Dropped an undeliverable queued message .*ENOSPC.*poison/).length, 1);
        });

        it('a msg whose properties throw when read fails cleanly', async function () {
            const node = await load();
            await connected(node);
            const msg = {};
            Object.defineProperty(msg, 'payload', { enumerable: true, get() { throw new Error('getter exploded'); } });
            const r = await produce(node, msg);
            assert.match(r.err.message, /getter exploded/);
            const ok = await produce(node, { payload: 'next' });
            assert.equal(ok.msg.kafka.status, 'sent');
        });

        it('a done(err) that throws (Catch cannot clone the msg) is logged, never thrown', async function () {
            const node = await load({ topic: '' }, {
                extraNodes: [{ id: 'c1', type: 'catch', scope: ['p1'], uncaught: false, wires: [['caught']] },
                    { id: 'caught', type: 'helper' }]
            });
            await connected(node);
            const msg = { payload: 'no topic anywhere' };
            // Node-RED deep-clones msg for Catch nodes; a throwing getter makes that clone throw
            Object.defineProperty(msg, 'evil', { enumerable: true, get() { throw new Error('getter boom'); } });
            assert.doesNotThrow(() => node.receive(msg));
            await waitFor(() => logged(node, 'error', /Could not complete the message: No topic.*getter boom/).length === 1);
            const ok = await produce(node, { payload: 'next', topic: 'test-topic' });
            assert.equal(ok.msg.kafka.status, 'sent');
        });

        it('send() or done() throwing in a delivery report does not stop the pipeline', async function () {
            const node = await load();
            await connected(node);
            const outcome = await new Promise((resolve) => {
                rt(node).input({ payload: 'x' }, () => {
                    throw new Error('send exploded');
                }, () => {
                    throw new Error('done exploded');
                });
                setTimeout(resolve, 50);
            });
            assert.equal(outcome, undefined);
            const ok = await produce(node, { payload: 'y' });
            assert.equal(ok.msg.kafka.status, 'sent');
            assert.deepEqual(fake.sent.map((s) => s.value), ['x', 'y']);
        });

        it('not authorised for the configured topic: messages are queued, slow retry, red status', async function () {
            let denied = true;
            fake.behaviour.sendError = () => (denied ? fake.errors.topicAuthorizationFailed() : null);
            const node = await load();
            await connected(node);
            const r = await produce(node, { payload: 'x' });
            assert.equal(r.err, null);
            assert.equal(r.msg.kafka.status, 'queued');
            await nextStatus(node, (s) => s.fill === 'red' && /^not authorized \(topic\) · 1 queued · retry 1s$/.test(s.text));
            denied = false;
            await waitFor(() => fake.sent.length === 1, { timeout: 2500 });
            await connected(node);
        });
    });

    describe('control messages', function () {
        it('queue-status reports the queue and connection state', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            await produce(node, { payload: 'b' });
            const { err, sent, msg } = await produce(node, { action: 'queue-status', topic: 'kept' });
            assert.equal(err, null);
            assert.equal(sent[0], msg);
            assert.equal(msg.topic, 'kept', 'the control msg itself is passed on');
            assert.equal(msg.payload.size, 2);
            assert.ok(msg.payload.bytes > 0);
            assert.equal(msg.payload.state, 'waiting');
            assert.equal(msg.payload.mode, 'backlog');
            assert.equal(msg.payload.queueMode, 'disk');
            assert.equal(msg.payload.queueDir, queueDir(userDir));
            // oldestTs is only known once the queue has read its head record
            assert.ok(msg.payload.oldestTs === undefined || Number.isFinite(msg.payload.oldestTs));
            assert.match(msg.payload.lastError, /ECONNREFUSED/);
            assert.equal(fake.sent.length, 0, 'control messages are not produced');
        });

        it('queue-clear drops queued messages and reports the statistics', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            for (const payload of ['a', 'b', 'c']) await produce(node, { payload });
            const { err, msg } = await produce(node, { action: 'queue-clear' });
            assert.equal(err, null);
            assert.equal(msg.payload.size, 0);
            assert.equal(logged(node, 'warn', /Cleared 3 message\(s\)/).length, 1);
            delete fake.behaviour.connectError;
            await connected(node);
            await sleep(30);
            assert.equal(fake.sent.length, 0, 'cleared messages are not sent');
        });

        it('queue-clear fails when there is no queue', async function () {
            const node = await load({ queueMode: 'off' });
            await connected(node);
            const r = await produce(node, { action: 'queue-clear' });
            assert.match(r.err.message, /offline queue is off/);
            const status = await produce(node, { action: 'queue-status' });
            assert.equal(status.msg.payload.queueMode, 'off');
            assert.equal(status.msg.payload.size, 0);
        });

        it('reconnect/flush retry immediately instead of waiting for the backoff', async function () {
            fake.behaviour.connectError = fake.errors.saslAuth();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            assert.ok(rt(node).retryInMs >= 1000, 'auth errors wait at least authRetryMin');
            await produce(node, { payload: 'queued' });
            delete fake.behaviour.connectError;
            const t0 = Date.now();
            const r = await produce(node, { action: 'reconnect' });
            assert.equal(r.err, null);
            assert.equal(r.sent.length, 0);
            await waitFor(() => fake.sent.length === 1, { timeout: 800 });
            assert.ok(Date.now() - t0 < 800, 'did not wait for the 1 s backoff');
            const flush = await produce(node, { action: 'flush' });
            assert.equal(flush.err, null);
        });

        it('messages with another msg.action are produced normally', async function () {
            const node = await load();
            await connected(node);
            const r = await produce(node, { action: 'something-else', payload: 'p' });
            assert.equal(r.msg.kafka.status, 'sent');
            assert.equal(fake.sent[0].value, 'p');
        });
    });

    describe('broker configuration problems', function () {
        it('without a broker config: red status, messages still queued, no crash', async function () {
            const node = await load({ broker: '' }, { withBroker: false });
            await nextStatus(node, (s) => s.fill === 'red' && s.text === 'no broker config');
            assert.equal(logged(node, 'error', /No Kafka broker configuration selected/).length, 1);
            const r = await produce(node, { payload: 'kept' });
            assert.equal(r.err, null);
            assert.equal(r.msg.kafka.status, 'queued');
            assert.equal(queuedRecords(queueDir(userDir)).length, 1);
            await nextStatus(node, (s) => s.text === 'no broker config · 1 queued');
            assert.equal(fake.instances.length, 0);
            assert.equal(rt(node).state, 'stopped');
        });

        it('with an invalid broker config (SASL credentials missing): config error, messages queued', async function () {
            const node = await load({}, { broker: { authMechanism: 'plain' } });
            await nextStatus(node, (s) => s.fill === 'red' && s.text === 'config error');
            assert.equal(logged(node, 'error', /Invalid Kafka broker configuration: .*SASL (username|password) is required/).length, 1);
            const r = await produce(node, { payload: 'kept' });
            assert.equal(r.msg.kafka.status, 'queued');
            assert.equal(queuedRecords(queueDir(userDir)).length, 1);
            await sleep(150);
            assert.equal(fake.instances.length, 0, 'no connection attempt with an invalid config');
        });

        it('broker config error with the queue off: messages fail with a clear reason', async function () {
            const node = await load({ queueMode: 'off' }, { broker: { authMechanism: 'plain' } });
            await nextStatus(node, (s) => s.text === 'config error');
            const r = await produce(node, { payload: 'x' });
            assert.match(r.err.message, /The Kafka broker configuration is invalid and the offline queue is off/);
        });

        it('a SASL authentication failure shows "auth failed" and retries slowly', async function () {
            fake.behaviour.connectError = fake.errors.saslAuth();
            const node = await load();
            await nextStatus(node, (s) => s.fill === 'red' && /^auth failed · retry 1s$/.test(s.text));
            assert.equal(logged(node, 'error', /Kafka connection failed: .*SASL/).length, 1);
            await sleep(400);
            assert.equal(fake.producers.length, 1, 'no fast retry after an auth error');
            const r = await produce(node, { payload: 'kept' });
            assert.equal(r.msg.kafka.status, 'queued');
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1, { timeout: 2500 });
            await connected(node);
            assert.equal(fake.producers.length, 2, 'the second attempt, about 1 s later, succeeded');
            assert.ok(statusCalls(node).some((s) => s.text === 'auth failed · 1 queued · retry 1s' ||
                s.text === 'connecting… · 1 queued'), 'queued count shown');
            assert.equal(logged(node, 'log', /Connected to Kafka after 2 attempts/).length, 1);
        });

        it('a TLS trust problem is shown with its reason (red) and retried slowly', async function () {
            fake.behaviour.connectError = fake.errors.tlsSelfSigned();
            const node = await load();
            await nextStatus(node, (s) => s.fill === 'red' && s.text === 'TLS: self-signed cert · retry 1s');
            assert.equal(rt(node).retryInMs >= 1000, true);
        });

        it('a message-content protocol error at connect time is retried with the normal backoff', async function () {
            fake.behaviour.connectError = () => fake.errors.protocol('CORRUPT_MESSAGE');
            const node = await load();
            const s = await nextStatus(node, (st) => / · retry /.test(st.text));
            assert.equal(s.fill, 'yellow');
            assert.equal(rt(node).lastKind, 'unknown');
            assert.ok(rt(node).retryInMs < 1000, 'normal backoff, not the auth/config minimum');
        });

        it('a producer factory that throws (invalid options) is a config error, retried slowly, no crash', async function () {
            fake.behaviour.producerFactoryError = new TypeError('\'timeout\' is not a number, received \'string\'');
            const node = await load();
            await nextStatus(node, (s) => /^config error · retry 1s$/.test(s.text));
            assert.equal(logged(node, 'error', /Could not create the Kafka producer: 'timeout' is not a number/).length, 1);
            delete fake.behaviour.producerFactoryError;
            await waitFor(() => rt(node).state === 'connected', { timeout: 2500 });
        });

        it('logs repeated identical connection failures once (then debug)', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => fake.producers.length >= 4, { message: 'several attempts' });
            assert.equal(logged(node, 'error', /Kafka connection failed/).length, 1);
            assert.ok(logged(node, 'debug', /Kafka connection failed/).length >= 2);
            // every created producer was disconnected after its failed connect
            await waitFor(() => fake.producers.slice(0, -1).every((p) => p.disconnectCount >= 1));
        });
    });

    describe('close', function () {
        it('a connect that completes after close is disconnected (no zombie producer)', async function () {
            fake.behaviour.connectDelayMs = 150;
            await load();
            await waitFor(() => fake.producers.length === 1);
            const p = fake.producers[0];
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 1000, 'close did not wait for the connect');
            await waitFor(() => p.connectCount === 1 && p.state === 'disconnected', { message: 'zombie disconnected' });
            await sleep(250);
            assert.equal(fake.producers.length, 1, 'no reconnect after close');
            assert.equal(p.state, 'disconnected');
        });

        it('persists the in-flight and pending messages when a send hangs during close', async function () {
            producerNode.TIMEOUTS.closeSendWait = 100;
            producerNode.TIMEOUTS.disconnect = 100;
            fake.behaviour.sendDelayMs = Infinity; // a hung broker
            const node = await load();
            await connected(node);
            const inFlight = produce(node, { payload: 'm1' });
            const pending = ['m2', 'm3'].map((payload) => produce(node, { payload }));
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 1500, `close took ${Date.now() - t0} ms`);
            const results = await Promise.all([inFlight, ...pending]);
            assert.deepEqual(results.map((r) => r.err), [null, null, null]);
            assert.deepEqual(results.map((r) => r.msg.kafka.status), ['queued', 'queued', 'queued']);
            assert.deepEqual(results.map((r) => r.sent.length), [0, 0, 0], 'no output after close');
            assert.deepEqual(queuedRecords(queueDir(userDir)).map((r) => r.m.v.s), ['m1', 'm2', 'm3']);

            // next start: everything is delivered, in order
            fake.behaviour.sendDelayMs = 0;
            await load();
            await waitFor(() => fake.sent.length === 3);
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2', 'm3']);
        });

        it('waits for an in-flight send that finishes during close and reports it as sent', async function () {
            fake.behaviour.sendDelayMs = 80;
            const node = await load();
            await connected(node);
            const inFlight = produce(node, { payload: 'm1' });
            const pending = produce(node, { payload: 'm2' });
            await helper.unload();
            const [r1, r2] = await Promise.all([inFlight, pending]);
            assert.equal(r1.err, null);
            assert.equal(r1.msg.kafka.status, 'sent');
            assert.equal(r2.err, null);
            assert.ok(['sent', 'queued'].includes(r2.msg.kafka.status));
            const onDisk = queuedRecords(queueDir(userDir)).map((r) => r.m.v.s);
            const delivered = fake.sent.map((s) => s.value);
            assert.deepEqual([...delivered, ...onDisk], ['m1', 'm2'], 'each message is either sent or queued');
        });

        it('memory mode: unsent messages fail with a warning on close', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueMode: 'memory' });
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            // the test helper restores its spies before the close handlers run
            const warnings = [];
            node.warn = (m) => warnings.push(String(m));
            await helper.unload();
            assert.equal(warnings.filter((w) => /Memory queue closed with 1 unsent/.test(w)).length, 1);
        });

        it('close calls done quickly, also when disconnect hangs', async function () {
            producerNode.TIMEOUTS.disconnect = 100;
            fake.behaviour.disconnectDelayMs = Infinity;
            const node = await load();
            await connected(node);
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 1000, `close took ${Date.now() - t0} ms`);
        });

        it('close in every state is fast and leaves no timers running', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 500);
            const attempts = fake.producers.length;
            await sleep(250);
            assert.equal(fake.producers.length, attempts, 'no reconnect after close');
            assert.equal(rt(node).retryTimer, null);
        });

        it('messages received while closing fail instead of being lost silently', async function () {
            const node = await load();
            await connected(node);
            const unloading = helper.unload();
            const r = await produce(node, { payload: 'late' });
            await unloading;
            assert.match(r.err.message, /closing/);
        });

        it('warns when a node with queued messages is removed', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'left behind' });
            await helper.setFlows([brokerConfig(), { id: 'out', type: 'helper' }], 'full');
            await waitFor(() => logged(node, 'warn', /Node removed with 1 unsent message/).length === 1);
            assert.equal(queuedRecords(queueDir(userDir)).length, 1, 'the data stays on disk');
        });

        it('a redeploy waits for the previous instance to release the queue directory', async function () {
            const dir = queueDir(userDir);
            const release = queueRegistry.acquire(dir, 'p1'); // the old instance, still closing
            release.closing();
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await sleep(60);
            assert.equal(rt(node).queueState, 'pending', 'waits for the old instance');
            const queued = produce(node, { payload: 'during redeploy' });
            release();
            const r = await queued;
            assert.equal(r.msg.kafka.status, 'queued');
            assert.equal(queuedRecords(dir).length, 1);
        });
    });

    describe('queue directory problems', function () {
        it('an unusable queue directory: error logged, node works without a queue', async function () {
            const blocker = path.join(userDir, 'not-a-dir');
            fs.writeFileSync(blocker, 'x');
            const node = await load({ queueDir: blocker });
            await waitFor(() => rt(node).queueState === 'failed');
            assert.equal(logged(node, 'error', /Offline queue unavailable/).length, 1);
            await connected(node);
            const r = await produce(node, { payload: 'x' });
            assert.equal(r.msg.kafka.status, 'sent');
        });

        it('an unset environment variable as queue directory is reported', async function () {
            const node = await load({ queueDir: '${QUEUE_DIR_NOT_SET}' });
            await waitFor(() => rt(node).queueState === 'failed');
            assert.equal(logged(node, 'error', /environment variable QUEUE_DIR_NOT_SET is not set/).length, 1);
        });

        it('a queue left with records is reopened by DiskQueue exactly as the producer wrote it', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: Buffer.from('bin'), key: Buffer.from([1, 2]), headers: { h: Buffer.from('x') } });
            await helper.unload();
            const q = new DiskQueue({ dir: queueDir(userDir) });
            await q.open();
            const [entry] = await q.peek(10);
            await q.close();
            assert.equal(entry.record.t, 'test-topic');
            assert.deepEqual(entry.record.m.v, { b: Buffer.from('bin').toString('base64') });
            assert.deepEqual(entry.record.m.k, { b: Buffer.from([1, 2]).toString('base64') });
            assert.deepEqual(entry.record.m.h, { h: { b: Buffer.from('x').toString('base64') } });
        });
    });

    // Regression tests of the review findings (ids in the test names). Each one
    // fails on the code before the fix.

    describe('node-level send retry instead of kafkajs\' (LOSS-1)', function () {
        /** What kafkajs throws with retries: 0 for a retriable Kafka error. */
        function retriable(type) {
            return fake.errors.retriesExceeded(fake.errors.protocol(type), { retryCount: 0, retryTime: 300 });
        }

        beforeEach(function () {
            producerNode.TIMEOUTS.sendRetryMin = 10;
            producerNode.TIMEOUTS.sendRetryMax = 40;
        });

        it('a leader change during a send is retried by sending the whole batch again, on the same connection', async function () {
            let failures = 2;
            fake.behaviour.sendError = () => (failures-- > 0 ? retriable('NOT_LEADER_FOR_PARTITION') : null);
            const node = await load();
            await connected(node);
            const { err, msg } = await produce(node, { payload: 'm1' });
            assert.equal(err, null);
            assert.equal(msg.kafka.status, 'sent', 'sent, not queued for a reconnect');
            const p = fake.producers[0];
            assert.equal(fake.producers.length, 1, 'no reconnect');
            assert.deepEqual(p.sendCalls.map((c) => c.args.messages.map((m) => m.value)), [['m1'], ['m1'], ['m1']]);
            assert.equal(logged(node, 'debug', /sending its 1 message\(s\) again .* \(retry 2 of 3\)/).length, 1);
            assert.equal(logged(node, 'error').length, 0, 'retries are not errors');
        });

        it('request timeouts are retried too; after 3 retries the batch goes to the queue as before', async function () {
            fake.behaviour.sendError = (args, client) => (client === fake.producers[0] ? fake.errors.requestTimeout() : null);
            const node = await load();
            await connected(node);
            const { msg } = await produce(node, { payload: 'm1' });
            assert.equal(msg.kafka.status, 'queued');
            assert.equal(fake.producers[0].sendCalls.length, 4, 'one attempt and 3 retries');
            await waitFor(() => fake.sent.length === 1 && rt(node).mode === 'live', { message: 'sent after reconnecting' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1']);
        });

        it('a connection failure is not retried: queue and reconnect at once', async function () {
            fake.behaviour.sendError = (args, client) => (client === fake.producers[0] ? fake.errors.sendDuringOutage() : null);
            const node = await load();
            await connected(node);
            const { msg } = await produce(node, { payload: 'm1' });
            assert.equal(msg.kafka.status, 'queued');
            assert.equal(fake.producers[0].sendCalls.length, 1);
        });

        it('the flusher retries a retriable error without dropping the connection', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'q1' });
            await produce(node, { payload: 'q2' });
            let failures = 1;
            fake.behaviour.sendError = () => (failures-- > 0 ? retriable('LEADER_NOT_AVAILABLE') : null);
            const created = fake.producers.length;
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live', { message: 'flushed' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['q1', 'q2']);
            assert.equal(fake.producers.length, created + 1, 'one connection: the flush was retried, not reconnected');
        });

        it('sendRetriable: Kafka errors that clear by themselves and request timeouts only', function () {
            const { sendRetriable } = internals;
            assert.equal(sendRetriable(retriable('NOT_LEADER_FOR_PARTITION')), true);
            assert.equal(sendRetriable(retriable('NOT_ENOUGH_REPLICAS')), true);
            assert.equal(sendRetriable(fake.errors.retriesExceeded(fake.errors.requestTimeout())), true);
            assert.equal(sendRetriable(fake.errors.sendDuringOutage()), false);
            assert.equal(sendRetriable(fake.errors.unknownTopic()), false);
            assert.equal(sendRetriable(fake.errors.messageTooLarge()), false);
            assert.equal(sendRetriable(new Error('odd')), false);
            assert.equal(sendRetriable(undefined), false);
        });
    });

    describe('partitions added to a topic (PART-1)', function () {
        /** The first connected producer has stale metadata (1 partition), later ones see 2. */
        function partitionsAddedAfterFirstConnect() {
            fake.behaviour.partitionCount = () => (fake.producers.filter((p) => p.connectCount > 0).length >= 2 ? 2 : 1);
        }

        it('live: a skipped partition with possibly stale metadata is sent again after a reconnect', async function () {
            producerNode.TIMEOUTS.freshMetadata = 0;
            partitionsAddedAfterFirstConnect();
            const node = await load();
            await connected(node);
            const { err, msg } = await produce(node, { payload: 'to-p1', partition: 1 });
            assert.equal(err, null, 'not "Partition 1 does not exist"');
            assert.equal(msg.kafka.status, 'sent');
            assert.equal(msg.kafka.partition, 1);
            assert.deepEqual(fake.sent.map((s) => [s.value, s.partition]), [['to-p1', 1]]);
            assert.equal(logged(node, 'error', /Partition 1 of topic test-topic is not in the producer's metadata/).length, 1);
        });

        it('live: only the skipped messages of a batch are sent again', async function () {
            producerNode.TIMEOUTS.freshMetadata = 0;
            partitionsAddedAfterFirstConnect();
            const node = await load();
            await connected(node);
            const { msg } = await produce(node, { messages: [{ value: 'a', partition: 0 }, { value: 'b', partition: 1 }] });
            assert.equal(msg.kafka.status, 'sent');
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b'], 'a is not sent twice');
        });

        it('flush: a queued message for a skipped partition stays queued and is sent after a reconnect', async function () {
            producerNode.TIMEOUTS.freshMetadata = 0;
            partitionsAddedAfterFirstConnect();
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            await produce(node, { payload: 'to-p1', partition: 1 });
            await produce(node, { payload: 'c' });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.some((s) => s.value === 'to-p1') && rt(node).mode === 'live', { message: 'sent' });
            assert.deepEqual(deadLetters(queueDir(userDir)), [], 'not dead-lettered');
            const values = fake.sent.map((s) => s.value);
            assert.deepEqual(values.slice(0, 2), ['a', 'c'], 'first round: a and c written, to-p1 skipped');
            assert.deepEqual(values.slice(2), ['to-p1', 'c'], 'after the reconnect: from to-p1 on (c again: at-least-once)');
        });
    });

    describe('queue directory hand-over on redeploy (LOSS-2, MAINT-9)', function () {
        it('a slow disk during a redeploy: the new instance waits for the real close and nothing is overwritten', async function () {
            producerNode.TIMEOUTS.closeQueue = 100;
            fake.behaviour.connectError = fake.errors.connectionRefused();
            await load();
            const old = rt(helper.getNode('p1'));
            await waitFor(() => old.queueState === 'open' && old.state === 'waiting');
            // the next fdatasync of the old queue takes 600 ms (SD card under load)
            const queue = old.queue;
            const syncData = queue._syncData;
            let stalled = false;
            queue._syncData = async function (...args) {
                if (!stalled) {
                    stalled = true;
                    await sleep(600);
                }
                return syncData.apply(this, args);
            };
            old.input({ payload: 'm0' }, () => {}, () => {});
            await sleep(30); // m0 written, its fsync hangs
            old.input({ payload: 'm1' }, () => {}, () => {}); // behind m0 on the old queue
            await sleep(30);
            await helper.setFlows([brokerConfig(), producerConfig(), { id: 'out', type: 'helper' }], 'full');
            const node = helper.getNode('p1');
            assert.notEqual(rt(node), old);
            const r = await produce(node, { payload: 'm2' });
            assert.equal(r.msg.kafka.status, 'queued');
            assert.equal(rt(node).queue.size, 3, 'the new instance opened the queue after the old one wrote m1');
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length >= 3 && rt(node).mode === 'live', { message: 'flushed' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['m0', 'm1', 'm2']);
        });

        it('the directory stays locked until the queue has really closed, although done() came earlier', async function () {
            producerNode.TIMEOUTS.closeQueue = 50;
            const node = await load();
            await connected(node);
            const dir = queueDir(userDir);
            const queue = rt(node).queue;
            const close = queue.close.bind(queue);
            queue.close = () => sleep(300).then(close);
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 250, 'Node-RED is not held up by the slow close');
            assert.equal(queueRegistry.isOwned(dir), true, 'still locked while the close runs');
            await waitFor(() => !queueRegistry.isOwned(dir), { timeout: 2000, message: 'released after the close' });
        });

        it('if the old instance is still closing after previousClose, the new one runs without the queue, then opens it', async function () {
            producerNode.TIMEOUTS.previousClose = 50;
            const dir = queueDir(userDir);
            const old = queueRegistry.acquire(dir, 'p1');
            old.closing();
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).queueState === 'failed', { message: 'gave up waiting' });
            assert.equal(logged(node, 'error', /still being closed by node p1 .*opened as soon as that node has closed it/).length, 1);
            const refused = await produce(node, { payload: 'not stored' });
            assert.match(refused.err.message, /offline queue is unavailable/);
            old(); // the old instance has finished closing
            await waitFor(() => rt(node).queueState === 'open', { message: 'queue opened later' });
            assert.equal(logged(node, 'log', /Offline queue .* is open now/).length, 1);
            const r = await produce(node, { payload: 'stored' });
            assert.equal(r.msg.kafka.status, 'queued');
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1);
            assert.deepEqual(fake.sent.map((s) => s.value), ['stored']);
        });

        it('close gives up waiting for the queued copy of a slow request: no duplicate at the tail', async function () {
            producerNode.TIMEOUTS.liveStall = 30;
            producerNode.TIMEOUTS.closeSendWait = 50;
            producerNode.TIMEOUTS.closeQueue = 50;
            producerNode.TIMEOUTS.disconnect = 50;
            fake.behaviour.sendDelayMs = Infinity;
            const node = await load();
            await connected(node);
            const queue = rt(node).queue;
            const syncData = queue._syncData;
            queue._syncData = async function (...args) {
                await sleep(300); // every write waits for a slow disk
                return syncData.apply(this, args);
            };
            const first = produce(node, { payload: 'm1' }); // in flight, never answered
            await sleep(60);
            const second = produce(node, { payload: 'm2' }); // stalled request: switch to the queue
            await sleep(10);
            await helper.unload();
            const [r1, r2] = await Promise.all([first, second]);
            assert.equal(r1.msg.kafka.status, 'queued', 'reported once its copy was written');
            assert.equal(r2.msg.kafka.status, 'queued');
            await waitFor(() => !queueRegistry.isOwned(queueDir(userDir)));
            assert.deepEqual(queuedRecords(queueDir(userDir)).map((r) => r.m.v.s), ['m1', 'm2'], 'no second m1 at the tail');
        });
    });

    describe('queued messages left behind (LOSS-4)', function () {
        it('changing the Queue name warns where the queued messages are', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load({ queueName: 'old-name' });
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'a' });
            await produce(node, { payload: 'b' });
            await helper.setFlows([brokerConfig(), producerConfig({ queueName: 'new-name' }), { id: 'out', type: 'helper' }], 'full');
            const renamed = helper.getNode('p1');
            const oldDir = path.join(userDir, 'kafkajs-nc-queue', 'old-name');
            await waitFor(() => logged(renamed, 'warn', /2 unsent message\(s\) are left in the offline queue/).length === 1);
            assert.ok(logged(renamed, 'warn').some((w) => w.includes(oldDir) && /set the old values again/.test(w)));
            assert.equal(queuedRecords(oldDir).length, 2, 'nothing is deleted');
        });

        it('warns once about queue directories with messages that no running producer uses', async function () {
            producerNode.TIMEOUTS.orphanScan = 30;
            const base = path.join(userDir, 'kafkajs-nc-queue');
            writeSegment(path.join(base, 'renamed-queue'), [record('stranded')]);
            fs.mkdirSync(path.join(base, 'drained-queue'), { recursive: true }); // empty: not reported
            const flowExtra = [producerConfig({ id: 'p2', queueName: 'other', wires: [[]] })];
            writeSegment(path.join(base, 'other'), [record('owned by p2')]);
            const node = await load({}, { extraNodes: flowExtra });
            await waitFor(() => logged(node, 'warn', /Found 1 offline queue/).length + logged(helper.getNode('p2'), 'warn', /Found 1 offline queue/).length === 1,
                { message: 'orphan warning' });
            const warning = [...logged(node, 'warn', /Found/), ...logged(helper.getNode('p2'), 'warn', /Found/)][0];
            assert.match(warning, /Found 1 offline queue\(s\) under .*kafkajs-nc-queue with unsent messages that no running producer uses: renamed-queue \(\d+ B\)/);
            // the next start does not report the same directory again
            await helper.setFlows([brokerConfig(), producerConfig(), ...flowExtra, { id: 'out', type: 'helper' }], 'full');
            await sleep(150);
            const again = [...logged(helper.getNode('p1'), 'warn', /Found/), ...logged(helper.getNode('p2'), 'warn', /Found/)];
            assert.equal(again.length, 0);
        });
    });

    describe('more review fixes', function () {
        it('a queued record for a topic the node no longer uses is dead-lettered instead of blocking the queue (STUCK-1)', async function () {
            fake.behaviour.sendError = (args) => (args.topic === 'ordrs' ? fake.errors.unknownTopic() : null);
            const node = await load({ topic: 'ordrs' }); // typo, topic auto-creation off on the cluster
            await connected(node);
            for (const payload of ['x1', 'x2']) assert.equal((await produce(node, { payload })).msg.kafka.status, 'queued');
            assert.equal(logged(node, 'error', /Kafka send failed \(topic ordrs\): .*UNKNOWN_TOPIC_OR_PARTITION/).length, 1,
                'the log names the topic');
            await helper.setFlows([brokerConfig(), producerConfig({ topic: 'orders' }), { id: 'out', type: 'helper' }], 'full');
            const fixed = helper.getNode('p1');
            const r = await produce(fixed, { payload: 'new' });
            assert.equal(r.err, null);
            await waitFor(() => fake.sent.length === 1 && rt(fixed).mode === 'live', { timeout: 4000, message: 'unblocked' });
            assert.deepEqual(fake.sent.map((s) => [s.topic, s.value]), [['orders', 'new']]);
            const dead = deadLetters(queueDir(userDir));
            assert.deepEqual(dead.map((d) => [d.record.t, d.record.m.v.s]), [['ordrs', 'x1'], ['ordrs', 'x2']]);
        });

        it('msg.action is matched trimmed and case-insensitively; other values are produced (MAINT-3)', async function () {
            const node = await load();
            await connected(node);
            const status = await produce(node, { action: ' Queue-Status ', payload: 'not data' });
            assert.equal(status.msg.payload.state, 'connected');
            const flush = await produce(node, { action: 'FLUSH', payload: 'not data either' });
            assert.equal(flush.err, null);
            assert.equal(flush.sent.length, 0);
            assert.equal(fake.sent.length, 0, 'control messages are never produced');
            const data = await produce(node, { action: 'created', payload: 'p' });
            assert.equal(data.msg.kafka.status, 'sent');
            assert.deepEqual(fake.sent.map((s) => s.value), ['p']);
        });

        it('logs "Connected to Kafka after N attempts" when a failed send is followed by a quick reconnect (MAINT-1)', async function () {
            fake.behaviour.sendError = (args, client) => (client === fake.producers[0] ? fake.errors.sendDuringOutage() : null);
            const node = await load({ queueMode: 'off' });
            await connected(node);
            const failed = await produce(node, { payload: 'a' });
            assert.ok(failed.err);
            await connected(node);
            const ok = await produce(node, { payload: 'b' });
            assert.equal(ok.msg.kafka.status, 'sent');
            assert.equal(logged(node, 'log', /Connected to Kafka after 2 attempts/).length, 1);
        });

        it('internal errors are not reported while closing, like the consumer (MAINT-1)', async function () {
            const node = await load();
            await connected(node);
            rt(node).closing = true;
            rt(node).bug('test', new Error('late'));
            assert.equal(logged(node, 'error', /Internal error/).length, 0);
            rt(node).closing = false;
        });

        it('memory mode: queue-status returns the dead letters, which exist nowhere else (MAINT-7)', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            fake.behaviour.sendError = (args) => (args.messages.some((m) => m.value === 'poison') ? fake.errors.messageTooLarge() : null);
            const node = await load({ queueMode: 'memory' });
            await waitFor(() => rt(node).state === 'waiting');
            await produce(node, { payload: 'poison' });
            await produce(node, { payload: 'ok' });
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 1 && rt(node).mode === 'live');
            const { msg } = await produce(node, { action: 'queue-status' });
            assert.equal(msg.payload.deadLettered, 1);
            assert.equal(msg.payload.deadLetters.length, 1);
            assert.equal(msg.payload.deadLetters[0].record.m.v.s, 'poison');
            assert.equal(logged(node, 'warn', /cannot be delivered to the memory queue's dead letters \(see msg\.payload\.deadLetters/).length, 1);
        });

        it('a red status shows the whole reason when it fits (not cut at 24 characters)', async function () {
            fake.behaviour.connectError = fake.errors.retriesExceeded(fake.errors.connectionError({
                code: 'UNABLE_TO_VERIFY_LEAF_SIGNATURE', message: 'Connection error: unable to verify the first certificate'
            }));
            const node = await load();
            await nextStatus(node, (s) => s.fill === 'red' && s.text === 'TLS: untrusted cert (CA?) · retry 1s');
        });
    });

    describe('paths the suite did not run before (MAINT-10)', function () {
        it('10 identical connection failures: one error, then a "still failing" warning', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => fake.producers.length >= 11, { timeout: 5000, message: '11 attempts' });
            assert.equal(logged(node, 'error', /Kafka connection failed/).length, 1);
            assert.equal(logged(node, 'warn', /Kafka still failing after 10 attempts: .*ECONNREFUSED/).length, 1);
        });

        it('a live send that fails while the node closes is stored in the queue', async function () {
            fake.behaviour.sendDelayMs = 150;
            fake.behaviour.sendError = fake.errors.sendDuringOutage();
            const node = await load();
            await connected(node);
            const r = produce(node, { payload: 'during-close' });
            await sleep(20);
            const logs = [];
            node.log = (m) => logs.push(String(m));
            await helper.unload();
            const { err, msg } = await r;
            assert.equal(err, null);
            assert.equal(msg.kafka.status, 'queued');
            assert.deepEqual(queuedRecords(queueDir(userDir)).map((x) => x.m.v.s), ['during-close']);
            assert.ok(logs.includes('Stored 1 unsent message(s) in the offline queue while closing'));
        });

        it('memory mode: messages still waiting for Kafka at close fail with done(err) and a warning', async function () {
            producerNode.TIMEOUTS.closeSendWait = 50;
            producerNode.TIMEOUTS.disconnect = 50;
            fake.behaviour.sendDelayMs = Infinity;
            const node = await load({ queueMode: 'memory' });
            await connected(node);
            const results = ['m0', 'm1', 'm2'].map((payload) => produce(node, { payload }));
            await sleep(20);
            const warnings = [];
            node.warn = (m) => warnings.push(String(m));
            await helper.unload();
            const errors = (await Promise.all(results)).map((r) => r.err && r.err.message);
            assert.ok(errors.every((m) => /closed before the message was sent/.test(m)), String(errors));
            assert.ok(warnings.includes('3 message(s) were not sent to Kafka before the node closed (the memory queue does not survive a restart)'));
        });

        it('persisting at close that fails is logged as such, not as "Stored" (LOG-1)', async function () {
            producerNode.TIMEOUTS.closeSendWait = 50;
            producerNode.TIMEOUTS.disconnect = 50;
            fake.behaviour.sendDelayMs = Infinity;
            const node = await load();
            await connected(node);
            const results = ['m0', 'm1', 'm2'].map((payload) => produce(node, { payload }));
            await sleep(20);
            rt(node).queue.enqueueMany = () => Promise.reject(Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' }));
            const lines = [];
            node.log = (m) => lines.push(String(m));
            node.warn = (m) => lines.push(String(m));
            await helper.unload();
            const errors = (await Promise.all(results)).map((r) => r.err && r.err.message);
            assert.ok(errors.every((m) => /Could not store the message in the offline queue: ENOSPC/.test(m)), String(errors));
            assert.equal(lines.filter((l) => /^Stored \d+ unsent/.test(l)).length, 0, String(lines));
            assert.ok(lines.includes('Closing: 0 unsent message(s) stored in the offline queue, 3 could not be stored ' +
                '(they failed with an error), 0 still being written (slow disk)'), String(lines));
        });

        it('closing while the queue is still opening is quick and fails the waiting messages cleanly', async function () {
            producerNode.TIMEOUTS.closeQueue = 50;
            const dir = queueDir(userDir);
            const old = queueRegistry.acquire(dir, 'p1');
            old.closing(); // the queue cannot open yet
            const node = await load();
            assert.equal(rt(node).queueState, 'pending');
            const waiting = produce(node, { payload: 'early' });
            const t0 = Date.now();
            await helper.unload();
            assert.ok(Date.now() - t0 < 500);
            const r = await waiting;
            assert.match(r.err.message, /closed before the message was sent/);
            old();
        });

        it('too many messages while the queue opens: the rest fail instead of piling up', async function () {
            producerNode.LIMITS.maxPending = 2;
            const dir = queueDir(userDir);
            const old = queueRegistry.acquire(dir, 'p1');
            old.closing();
            const node = await load();
            const early = ['e1', 'e2'].map((payload) => produce(node, { payload }));
            const overflow = await produce(node, { payload: 'e3' });
            assert.match(overflow.err.message, /still opening and too many messages are waiting/);
            old();
            const results = await Promise.all(early);
            assert.ok(results.every((r) => r.err === null));
        });

        it('a connection failure while isolating a poison batch stops, keeps the order and sends nothing twice', async function () {
            fake.behaviour.connectError = fake.errors.connectionRefused();
            const node = await load();
            await waitFor(() => rt(node).state === 'waiting');
            for (const payload of ['a', 'b', 'c', 'd']) await produce(node, { payload });
            let calls = 0;
            fake.behaviour.sendError = (args) => {
                calls++;
                if (calls === 1) return fake.errors.messageTooLarge(); // the batch: isolate
                if (calls === 3 && args.messages[0].value === 'b') return fake.errors.sendDuringOutage();
                return null;
            };
            delete fake.behaviour.connectError;
            await waitFor(() => fake.sent.length === 4 && rt(node).mode === 'live', { timeout: 4000 });
            assert.deepEqual(fake.sent.map((s) => s.value), ['a', 'b', 'c', 'd']);
            assert.equal(logged(node, 'error', /Sending queued messages to Kafka failed: .*ECONNREFUSED/).length, 1);
            assert.deepEqual(deadLetters(queueDir(userDir)), []);
        });

        it('a failed request while newer messages are already queued stays in memory and is sent first', async function () {
            producerNode.TIMEOUTS.liveStall = 30;
            fake.behaviour.sendDelayMs = (args) => (args.messages[0].value === 'm1' ? 120 : 0);
            fake.behaviour.sendError = (args, client) => (client === fake.producers[0] && args.messages[0].value === 'm1'
                ? fake.errors.sendDuringOutage() : null);
            const node = await load();
            await connected(node);
            const queue = rt(node).queue;
            const enqueueMany = queue.enqueueMany.bind(queue);
            let failCopy = true;
            queue.enqueueMany = (records) => {
                if (failCopy) { // storing the copy of the in-flight batch fails (switchToBacklog)
                    failCopy = false;
                    return Promise.reject(new Error('EIO (simulated)'));
                }
                return enqueueMany(records);
            };
            const first = produce(node, { payload: 'm1' });
            await sleep(60);
            const second = await produce(node, { payload: 'm2' });
            assert.equal(second.msg.kafka.status, 'queued');
            const r1 = await first;
            assert.equal(r1.err, null);
            await waitFor(() => fake.sent.length === 2 && rt(node).mode === 'live', { message: 'both sent' });
            assert.deepEqual(fake.sent.map((s) => s.value), ['m1', 'm2'], 'm1 before the newer queued m2');
            assert.equal(r1.msg.kafka.status, 'sent');
        });
    });
});

describe('kafka producer editor (kafka-producer.html)', function () {
    const html = fs.readFileSync(path.join(__dirname, '../../nodes/kafka-producer.html'), 'utf8');

    function loadDefinition(context = {}) {
        const script = /<script type="text\/javascript">([\s\S]*?)<\/script>/.exec(html)[1];
        let definition = null;
        const RED = {
            nodes: {
                registerType(type, def) {
                    assert.equal(type, 'kafkajs-nc-producer');
                    definition = def;
                }
            },
            validators: {}
        };
        const sandbox = Object.assign({ RED, $: () => ({}) }, context);
        vm.runInNewContext(script, sandbox, { filename: 'kafka-producer.html' });
        definition.sandbox = sandbox;
        return definition;
    }

    it('oneditsave stores the header rows in the format the runtime reads', function () {
        const rows = [
            { name: 'a', value: '1' }, { name: ' a ', value: '2' }, { name: '', value: 'ignored' }, { name: 'b', value: '' }
        ];
        // just enough jQuery for oneditsave
        const $ = (target) => {
            if (target === '#node-input-headers-container') {
                return { editableList: () => ({ each: (fn) => rows.forEach((row) => fn.call(row)) }) };
            }
            return { find: (cls) => ({ val: () => (cls.includes('name') ? target.name : target.value) }) };
        };
        const def = loadDefinition({ $ });
        const node = {};
        def.oneditsave.call(node);
        assert.equal(JSON.stringify(node.headers), JSON.stringify({ a: ['1', '2'], b: '' }));
        assert.deepEqual(internals.parseStaticHeaders(JSON.parse(JSON.stringify(node.headers))).headers, { a: ['1', '2'], b: '' });
    });

    /**
     * Run oneditprepare with a recording stand-in for jQuery: every call is
     * logged with the chain it was made on, e.g. '#node-input-headers-container
     * .css().editableList().parent()'. Returns the calls and the editableList
     * options of the static headers list.
     */
    function runEditPrepare(node) {
        const calls = [];
        let listOptions = null;
        const make = (chain) => new Proxy(function () {}, {
            get(_, method) {
                if (method === 'then') return undefined; // not a promise
                return (...args) => {
                    calls.push({ chain, method, args });
                    if (method === 'editableList' && args[0] && typeof args[0] === 'object') listOptions = args[0];
                    return make(`${chain}.${String(method)}()`);
                };
            }
        });
        const $ = (selector, options) => make(String(selector) + (options && options.class ? `.${options.class}` : ''));
        const def = loadDefinition({ $, setTimeout: () => {} });
        def.sandbox.RED.tabs = { create: () => ({ addTab() {}, resize() {} }) };
        def.sandbox.RED.popover = { tooltip() {} };
        def.oneditprepare.call(node);
        return { calls, listOptions };
    }

    it('the static headers list grows with its rows instead of overflowing onto the controls below (EDITOR-1)', function () {
        const { calls } = runEditPrepare({ headers: { a: '1', b: '2', c: '3', d: '4' } });
        // editableList fixes the list height to the measured min-height (120 px); it must be cleared again
        const cleared = calls.find((c) => c.method === 'css' && c.args[0] === 'height' && c.args[1] === '' &&
            /^#node-input-headers-container\..*editableList\(\)\.parent\(\)$/.test(c.chain));
        assert.ok(cleared, 'height of the list container cleared after editableList()');
    });

    it('opening and saving the dialog keeps hand-written static headers the runtime accepts (EDITOR-2)', function () {
        for (const headers of ['{"source":"line-3","n":[1,2]}', { meta: { a: 1 }, gone: null, list: ['x', null] },
            [{ key: 'k', value: 'v' }, { name: 'k', value: { deep: true } }]]) {
            const { calls, listOptions } = runEditPrepare({ headers });
            const rows = calls.filter((c) => c.method === 'editableList' && c.args[0] === 'addItem').map((c) => c.args[1]);
            // what the dialog shows in each row
            const shown = rows.map((row) => {
                const before = calls.length;
                listOptions.addItem({}, 0, row);
                const vals = calls.slice(before).filter((c) => c.method === 'val' && c.args.length === 1);
                return { name: vals[0].args[0], value: vals[1].args[0] };
            });
            assert.ok(shown.every((r) => r.value !== '[object Object]' && r.value !== ''), JSON.stringify(shown));
            // Done: oneditsave reads the rows back
            const $ = (target) => (target === '#node-input-headers-container'
                ? { editableList: () => ({ each: (fn) => shown.forEach((row) => fn.call(row)) }) }
                : { find: (cls) => ({ val: () => (cls.includes('name') ? target.name : target.value) }) });
            const node = {};
            loadDefinition({ $ }).oneditsave.call(node);
            // the Kafka headers sent: (name, value) pairs; ['x'] and 'x' are the same single header
            const sent = (h) => Object.entries(internals.parseStaticHeaders(h).headers || {})
                .flatMap(([name, value]) => [].concat(value).map((v) => [name, String(v)]));
            const saved = JSON.parse(JSON.stringify(node.headers));
            assert.deepEqual(sent(saved), sent(headers), `same headers sent after open + Done for ${JSON.stringify(headers)}`);
        }
    });

    it('every message property of the help renders as one complete entry in the Node-RED help sidebar (DOC-1)', function () {
        // Node-RED's markdown extension (editor-client red.js, descriptionList/description): a line with a
        // third ':' ends the property list and only the text after its last ':' is kept.
        const block = /^(?::[^:\n]+:[^:\n]*(?:\n|$))+/;
        const entry = /^:([^:\n]+)\(([^:\n]+)\).*?:([^:\n]*)(?:\n|$)/;
        const help = /<script type="text\/markdown" data-help-name="kafkajs-nc-producer">\n([\s\S]*?)<\/script>/.exec(html)[1];
        const lines = help.split('\n').filter((l) => l.startsWith(': '));
        assert.ok(lines.length >= 15);
        for (const line of lines) {
            assert.equal((block.exec(line + '\n') || [''])[0], line + '\n', `stays in the list: ${line}`);
            const m = entry.exec(line);
            assert.equal(m && m[3].trim(), line.slice(line.indexOf(') : ') + 4).trim(), `whole description kept: ${line}`);
        }
    });

    it('the help matches the runtime (DOC-2, DOC-4, idempotent removed, timestamp types)', function () {
        const help = /<script type="text\/markdown" data-help-name="kafkajs-nc-producer">\n([\s\S]*?)<\/script>/.exec(html)[1];
        assert.ok(!/idempotent/i.test(help), 'the removed option is not documented');
        assert.ok(!/Any setting can be/.test(help), 'only some settings fail every message when a variable is not set');
        for (const state of ['`idle`', '`connecting`', '`connected`', '`waiting`', '`stopped`', '`closing`']) {
            assert.ok(help.includes(state), state);
        }
        assert.ok(help.includes('`starting` / `12 queued` (grey)'));
        assert.match(help, /a string of digits; other strings such as ISO dates are an error/);
        assert.match(help, /A copy of a node keeps its \*Queue name\*/);
        assert.match(help, /msg\.payload\.deadLetters/);
    });

    it('registers the node with every runtime property as a default', function () {
        const def = loadDefinition();
        assert.equal(def.category, 'kafka');
        assert.equal(def.color, '#c7d8e6');
        assert.equal(def.icon, 'kafka.svg');
        assert.equal(def.paletteLabel, 'kafka producer');
        assert.equal(def.inputs, 1);
        assert.equal(def.outputs, 1);
        assert.equal(def.align, 'right');
        assert.deepEqual(Array.from(def.outputLabels), ['delivery report']); // vm realm array
        assert.deepEqual(Object.keys(def.defaults).sort(), [
            'ackTimeout', 'acks', 'allowAutoTopicCreation', 'batchSize', 'broker', 'compression', 'headers',
            'key', 'name', 'partition', 'partitioner', 'queueDir', 'queueFsync', 'queueMaxAgeHours',
            'queueMaxMB', 'queueMaxMessages', 'queueMode', 'queueName', 'queueOverflow', 'topic', 'useMsgHeaders'
        ]);
        assert.equal(def.defaults.broker.type, 'kafkajs-nc-broker');
        assert.equal(def.defaults.broker.required, true);
        // the editor defaults are the runtime defaults
        const runtime = internals.parseConfig(Object.fromEntries(Object.entries(def.defaults).map(([k, v]) => [k, v.value])));
        const fallback = internals.parseConfig({});
        delete runtime.warnings;
        delete fallback.warnings;
        assert.deepEqual(runtime, fallback);
        for (const hook of ['oneditprepare', 'oneditsave', 'label']) {
            assert.equal(typeof def[hook], 'function', hook);
        }
        assert.equal(def.label.call({ name: '', topic: 'orders' }), 'kafka → orders');
        assert.equal(def.label.call({ name: 'Mine', topic: 'orders' }), 'Mine');
        assert.equal(def.label.call({}), 'kafka producer');
    });

    it('validators accept what the runtime accepts', function () {
        const d = loadDefinition().defaults;
        const ok = (field, v) => d[field].validate.call({}, v, {}) === true;
        assert.ok(ok('topic', ''));
        assert.ok(ok('topic', 'orders.v1'));
        assert.ok(ok('topic', '${TOPIC}'));
        assert.ok(!ok('topic', 'a/b'));
        assert.equal(typeof d.topic.validate.call({}, 'a/b', {}), 'string', 'reason text on Node-RED >= 3.1');
        assert.equal(d.topic.validate.call({}, 'a/b'), false, 'boolean on older Node-RED');
        assert.ok(ok('partition', ''));
        assert.ok(ok('partition', '0'));
        assert.ok(!ok('partition', '-1'));
        assert.ok(!ok('partition', '1.5'));
        assert.ok(ok('batchSize', '10000'));
        assert.ok(!ok('batchSize', '0'));
        assert.ok(ok('ackTimeout', ''));
        assert.ok(!ok('ackTimeout', 'abc'));
        assert.ok(ok('queueMaxMB', '0.5'));
        assert.ok(!ok('queueMaxMessages', '1.5'));
        assert.ok(ok('queueMaxAgeHours', '${MAX_AGE}'));
    });

    it('validators accept a ${VAR} only as the whole value, as Node-RED replaces nothing else (VALID-1)', function () {
        const d = loadDefinition().defaults;
        const reason = (field, v) => d[field].validate.call({}, v, {});
        assert.equal(reason('topic', '$(TOPIC)'), true);
        assert.equal(reason('partition', '${PARTITION}'), true);
        for (const [field, v] of [['topic', ' ${TOPIC}'], ['topic', 'orders-${REGION}'], ['partition', '${PARTITION} '],
            ['batchSize', ' ${BATCH}'], ['queueMaxMB', '${MB}0']]) {
            assert.match(String(reason(field, v)), /only replaced when it is the whole value/, `${field} ${JSON.stringify(v)}`);
        }
    });

    it('has a markdown help text with the standard sections', function () {
        const m = /<script type="text\/markdown" data-help-name="kafkajs-nc-producer">\n([\s\S]*?)<\/script>/.exec(html);
        assert.ok(m, 'markdown help present');
        const help = m[1];
        assert.ok(!/^\s/.test(help), 'no leading whitespace (Node-RED renders indented markdown as code)');
        for (const heading of ['### Inputs', '### Outputs', '### Details', '### References', '#### Offline queue',
            '#### Order', '#### Delivery guarantee', '#### Dead-letter file', '#### Status', '#### Control messages',
            '#### Examples']) {
            assert.ok(help.includes(heading), heading);
        }
        for (const text of ['kafkajsNcProducerQueueDir', 'dead-letter.jsonl', 'kafkajs-nc-queue', 'queue-status',
            'queue-clear', 'duplicate', 'msg.messages', 'tombstone']) {
            assert.ok(help.includes(text), text);
        }
    });
});

describe('kafka producer internals', function () {
    const { parseConfig, parseStaticHeaders, buildEntries, locateMessages, missingPartitions, fmtDelay, topicProblem, queueDirFor } = internals;

    it('parseConfig: defaults for an empty config', function () {
        const c = parseConfig({});
        assert.equal(c.topic, '');
        assert.equal(c.key, '');
        assert.equal(c.partition, undefined);
        assert.equal(c.headers, undefined);
        assert.equal(c.useMsgHeaders, true);
        assert.equal(c.acks, -1);
        assert.equal(c.ackTimeout, 30000);
        assert.equal(c.compression, 'none');
        assert.equal(c.partitioner, 'default');
        assert.equal('idempotent' in c, false);
        assert.equal(c.allowAutoTopicCreation, true);
        assert.equal(c.batchSize, 100);
        assert.deepEqual(c.queue, {
            mode: 'disk', dir: '', name: '', maxBytes: 100 * 1024 * 1024, maxMessages: 0, overflow: 'drop-oldest',
            maxAgeMs: 0, maxAgeHours: 0, fsync: true
        });
        assert.deepEqual(c.warnings, []);
        assert.deepEqual(parseConfig(undefined).queue.mode, 'disk');
    });

    it('parseConfig: editor strings are parsed and clamped', function () {
        const c = parseConfig({
            ackTimeout: '50', batchSize: '99999', queueMaxMB: '0.5', queueMaxMessages: '-3', queueMaxAgeHours: '2',
            queueFsync: 'false', useMsgHeaders: 'false', acks: 'none', compression: 'LZ4', queueMode: 'memory',
            allowAutoTopicCreation: false, partition: ' 3 ', topic: ' t1 '
        });
        assert.equal(c.ackTimeout, 100);
        assert.equal(c.batchSize, 10000);
        assert.equal(c.queue.maxBytes, 524288);
        assert.equal(c.queue.maxMessages, 0);
        assert.equal(c.queue.maxAgeMs, 7200000);
        assert.equal(c.queue.fsync, false);
        assert.equal(c.useMsgHeaders, false);
        assert.equal(c.acks, 0);
        assert.equal(c.compression, 'lz4');
        assert.equal(c.queue.mode, 'memory');
        assert.equal(c.allowAutoTopicCreation, false);
        assert.equal(c.partition, 3);
        assert.equal(c.topic, 't1');
    });

    it('parseConfig: unknown values fall back with warnings; invalid settings become errors', function () {
        const c = parseConfig({ acks: 'maybe', compression: 'zstd', queueMode: 'cloud', partition: '-1', topic: 'a b', key: '${K}' });
        assert.equal(c.acks, -1);
        assert.equal(c.compression, 'none');
        assert.equal(c.queue.mode, 'disk');
        assert.equal(c.warnings.length, 3);
        assert.match(c.partitionError, /Partition setting/);
        assert.match(c.topicError, /invalid Kafka topic name/);
        assert.match(c.keyError, /environment variable K is not set/);
        assert.equal(parseConfig({ acks: 0 }).acks, 0, 'numeric acks 0 is kept');
        assert.equal(parseConfig({ acks: '1' }).acks, 1);
    });

    it('parseStaticHeaders accepts objects, pair arrays and JSON', function () {
        assert.deepEqual(parseStaticHeaders({ a: 'x', b: ['1', '2'], c: null }).headers, { a: 'x', b: ['1', '2'] });
        assert.deepEqual(parseStaticHeaders([{ key: 'a', value: '1' }, { name: 'a', value: '2' }, { key: '', value: 'skip' }]).headers,
            { a: ['1', '2'] });
        assert.deepEqual(parseStaticHeaders('{"j":"son"}').headers, { j: 'son' });
        assert.equal(parseStaticHeaders({}).headers, undefined);
        assert.equal(parseStaticHeaders('').headers, undefined);
        assert.match(parseStaticHeaders('{oops').error, /Invalid static headers/);
        assert.match(parseStaticHeaders([1, 2]).error || '', /^$|Invalid/);
    });

    it('topicProblem validates Kafka topic names', function () {
        assert.equal(topicProblem('orders.v1_x-y', 'T'), null);
        assert.match(topicProblem('a/b', 'T'), /invalid Kafka topic name/);
        assert.match(topicProblem('.', 'T'), /invalid/);
        assert.match(topicProblem('x'.repeat(250), 'T'), /invalid/);
        assert.match(topicProblem('${TOPIC}', 'T'), /environment variable TOPIC is not set/);
    });

    it('buildEntries sets the arrival time as timestamp unless msg.timestamp is given', function () {
        const cfg = parseConfig({ topic: 't' });
        const [e] = buildEntries({ payload: 'x' }, cfg, 1234);
        assert.deepEqual(e, { topic: 't', tf: 0, message: { value: 'x', timestamp: '1234' } });
        const [d] = buildEntries({ payload: 'x', timestamp: new Date(5000) }, cfg, 1234);
        assert.equal(d.message.timestamp, '5000');
        const [m] = buildEntries({ payload: 'x', topic: 'ignored', key: '' }, parseConfig({}), 1);
        assert.equal(m.tf, 1);
        assert.equal(m.topic, 'ignored');
        assert.equal('key' in m.message, false, 'empty key is no key');
    });

    it('locateMessages only reports exact positions', function () {
        const r = (partition, baseOffset) => ({ topicName: 't', partition, baseOffset, errorCode: 0 });
        // all to one partition
        assert.deepEqual(locateMessages([{}, {}], [r(2, '10')], 't'), [{ partition: 2, offset: '10' }, { partition: 2, offset: '11' }]);
        // explicit partitions, interleaved
        assert.deepEqual(locateMessages([{ partition: 0 }, { partition: 1 }, { partition: 0 }], [r(0, '5'), r(1, '7')], 't'),
            [{ partition: 0, offset: '5' }, { partition: 1, offset: '7' }, { partition: 0, offset: '6' }]);
        // spread by the partitioner: unknown
        assert.deepEqual(locateMessages([{}, {}], [r(0, '1'), r(1, '1')], 't'), [null, null]);
        // explicit partition that was not written
        assert.deepEqual(locateMessages([{ partition: 9 }, { partition: 0 }], [r(0, '3')], 't'), [null, { partition: 0, offset: '3' }]);
        assert.deepEqual(locateMessages([{}], [], 't'), [null]);
        assert.deepEqual(locateMessages([{}], undefined, 't'), [null]);
        assert.deepEqual(locateMessages([{}], [r(0, 'garbage')], 't'), [{ partition: 0, offset: undefined }]);
        assert.deepEqual(missingPartitions([{ partition: 9 }, { partition: 0 }, { partition: 9 }, {}], [r(0, '3')], 't'), [9]);
    });

    it('fmtDelay', function () {
        assert.equal(fmtDelay(100), '0.1s');
        assert.equal(fmtDelay(1000), '1s');
        assert.equal(fmtDelay(8040), '8s');
        assert.equal(fmtDelay(60000), '60s');
        assert.equal(fmtDelay(300000), '5m');
        assert.equal(fmtDelay(NaN), '0s');
    });

    it('queueDirFor resolves against the user directory', function () {
        const cfg = parseConfig({});
        const RED = { settings: { userDir: '/data/nr' } };
        assert.equal(queueDirFor(cfg, RED, 'abc.123').dir, path.join('/data/nr', 'kafkajs-nc-queue', 'abc.123'));
        assert.equal(queueDirFor(parseConfig({ queueDir: '/var/q', queueName: 'line 1' }), RED, 'x').dir, path.join('/var/q', 'line_1'));
        assert.equal(queueDirFor(parseConfig({ queueDir: 'rel' }), RED, 'x').dir, path.join('/data/nr', 'rel', 'x'));
        assert.equal(queueDirFor(cfg, { settings: { userDir: '/u', kafkajsNcProducerQueueDir: '/srv/q' } }, 'n').dir, path.join('/srv/q', 'n'));
        assert.match(queueDirFor(parseConfig({ queueName: '${N}' }), RED, 'x').error, /environment variable N is not set/);
        const hostile = { get settings() { throw new Error('boom'); } };
        assert.ok(queueDirFor(cfg, hostile, 'x').dir, 'never throws');
    });

    it('queueDirFor rejects ${VAR} inside a path and ~user instead of creating literal directories (PATH-2)', function () {
        const RED = { settings: { userDir: '/data/nr' } };
        const embedded = /\$\{HOME\} is only replaced by Node-RED when it is the whole/;
        for (const [cfg, settings, re] of [
            [{ queueDir: '${HOME}/queues' }, {}, embedded],
            [{ queueDir: '/mnt/${DATA}/q' }, {}, /\$\{DATA\} is only replaced/],
            [{}, { kafkajsNcProducerQueueDir: '${HOME}/q' }, embedded],
            [{ queueName: 'line-${LINE}' }, {}, /Queue name setting: \$\{LINE\} is only replaced/],
            [{ queueDir: '~user/q' }, {}, /"~user\/q" is not expanded; use an absolute path/]
        ]) {
            const where = queueDirFor(parseConfig(cfg), { settings: { ...RED.settings, ...settings } }, 'x');
            assert.equal(where.dir, null, JSON.stringify(cfg));
            assert.match(where.error, re);
        }
        assert.equal(queueDirFor(parseConfig({ queueDir: '~/q' }), RED, 'x').error, null, '~/ is expanded');
    });
});
