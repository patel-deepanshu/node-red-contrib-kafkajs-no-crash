'use strict';

/**
 * Producer node → real Kafka → consumer node, for every compression codec.
 *
 * Snappy and LZ4 are registered by this palette (lib/codecs.js). Kafka's own
 * Java client is used as an independent decoder when KAFKA_HOME is set: the
 * LZ4 frame format in particular must be readable by the Java consumer (an
 * lz4js frame with dependent blocks would be rejected by every broker).
 */

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const { waitFor, nextStatus } = require('../helpers/common');
const {
    E2E, helper, uniqueName, requireKafka, createTopic, readAll, storedCodecs, useNodeRed, brokerConfig, outcomeNodes,
    collect, outcome, expectOk, calls, withAdmin
} = require('./e2e-helpers');

/** Input messages with every payload type, and what Kafka must contain for each. */
function inputs() {
    const text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. '.repeat(200); // compressible
    const list = [
        { msg: { payload: 'hello', key: 'k1', headers: { h: 'v', multi: ['1', '2'] } }, value: 'hello', key: 'k1' },
        { msg: { payload: { a: 1, b: [true, null] } }, value: '{"a":1,"b":[true,null]}' },
        { msg: { payload: 42 }, value: '42' },
        { msg: { payload: text }, value: text },
        { msg: { payload: Buffer.from('binary\u0000\u0001') }, value: 'binary\u0000\u0001' }
    ];
    for (let i = 0; i < 30; i++) list.push({ msg: { payload: `seq-${i}`, key: `key-${i % 3}` }, value: `seq-${i}`, key: `key-${i % 3}` });
    return list;
}

/** Read a topic with Kafka's Java console consumer (an independent decoder). */
function javaConsume(topic, count) {
    const bin = path.join(E2E.kafkaHome, 'bin', 'kafka-console-consumer.sh');
    const out = execFileSync(bin, [
        '--bootstrap-server', E2E.brokers.join(','), '--topic', topic, '--from-beginning',
        '--max-messages', String(count), '--timeout-ms', '60000'
    ], {
        encoding: 'utf8',
        timeout: 90000,
        env: { ...process.env, KAFKA_HEAP_OPTS: '-Xmx256m' },
        stdio: ['ignore', 'pipe', 'pipe']
    });
    return out.split('\n').filter((l) => l !== '');
}

describe('e2e: producer node → Kafka → consumer node', function () {
    this.timeout(180000);

    before(function () {
        requireKafka(this);
    });

    useNodeRed();

    function roundTripFlow(topic, producerExtra = {}, consumerExtra = {}) {
        return [
            brokerConfig(),
            { id: 'p1', type: 'kafkajs-nc-producer', name: 'producer', broker: 'b1', topic, wires: [['reports']], ...producerExtra },
            { id: 'reports', type: 'helper' },
            {
                id: 'k1', type: 'kafkajs-nc-consumer', name: 'consumer', broker: 'b1', topics: topic, fromBeginning: true,
                payloadFormat: 'buffer', sessionTimeout: '10000', heartbeatInterval: '1000', maxWaitTimeInMs: '200',
                wires: [['out']], ...consumerExtra
            },
            { id: 'out', type: 'helper' },
            ...outcomeNodes(['p1', 'k1'])
        ];
    }

    for (const compression of ['none', 'gzip', 'snappy', 'lz4']) {
        it(`compression ${compression}: every payload type arrives intact and in order`, async function () {
            const topic = uniqueName(`rt-${compression}`);
            await createTopic(topic, 1);
            await helper.load([brokerNode, producerNode, consumerNode], roundTripFlow(topic, { compression, batchSize: '50' }));
            const p1 = helper.getNode('p1');
            const k1 = helper.getNode('k1');
            const out = collect('out');
            const reports = collect('reports');
            const caught = collect('caught');
            await nextStatus(p1, (s) => s.fill === 'green', 30000);
            await nextStatus(k1, (s) => s.fill === 'green', 30000);

            const list = inputs();
            for (const { msg } of list) p1.receive(msg); // a burst: batched by the live pipeline
            await waitFor(() => reports.length === list.length, { timeout: 30000, message: 'delivery reports' });
            assert.deepEqual(caught.map((m) => m.error.message), []);
            assert.ok(reports.every((m) => m.kafka.status === 'sent'), JSON.stringify(reports.map((m) => m.kafka.status)));
            // offsets of a single partition are exact and consecutive
            assert.deepEqual(reports.map((m) => m.kafka.offset), list.map((_, i) => String(i)));

            await waitFor(() => out.length === list.length, { timeout: 60000, message: `consumed ${out.length}/${list.length}` });
            out.forEach((m, i) => {
                assert.ok(Buffer.isBuffer(m.payload));
                assert.equal(m.payload.toString('latin1'), Buffer.from(list[i].value).toString('latin1'), `message ${i}`);
                assert.equal(m.offset, String(i));
                if (list[i].key) assert.equal(m.key, list[i].key);
            });
            assert.deepEqual(out[0].headers, { h: 'v', multi: ['1', '2'] });

            // the bytes in Kafka, read by a plain kafkajs consumer
            const stored = await readAll(topic);
            assert.deepEqual(stored.map((r) => r.value.toString('latin1')), list.map((x) => Buffer.from(x.value).toString('latin1')));

            if (E2E.control) {
                // the batches really are stored compressed (the broker validated them on produce)
                const codecs = storedCodecs(topic);
                assert.ok(codecs.length > 0 && codecs.every((c) => c === compression), `stored codecs: ${codecs}`);
            }
            if (E2E.kafkaHome && (compression === 'lz4' || compression === 'snappy' || compression === 'gzip')) {
                // Kafka's Java client must decode what we compressed
                const lines = javaConsume(topic, list.length);
                assert.equal(lines.length, list.length, `java consumer read ${lines.length} records`);
                assert.equal(lines[0], 'hello');
                assert.equal(lines[3], list[3].value);
                assert.deepEqual(lines.slice(5), list.slice(5).map((x) => x.value));
            }
        });
    }

    it('msg.messages: several Kafka messages in one input, one delivery report', async function () {
        const topic = uniqueName('rt-batch');
        await createTopic(topic, 1);
        await helper.load([brokerNode, producerNode, consumerNode], roundTripFlow(topic, {}, { payloadFormat: 'string' }));
        const p1 = helper.getNode('p1');
        await nextStatus(p1, (s) => s.fill === 'green', 30000);
        const out = collect('out');
        const done = await expectOk('p1', {
            messages: [{ value: 'a', key: 'x', headers: { n: '1' } }, { value: 'b' }, { payload: 'c', timestamp: 1700000000000 }]
        });
        assert.equal(done.kafka.status, 'sent');
        assert.equal(done.kafka.count, 3);
        await waitFor(() => out.length === 3, { timeout: 60000 });
        assert.deepEqual(out.map((m) => m.payload), ['a', 'b', 'c']);
        assert.equal(out[0].key, 'x');
        assert.deepEqual(out[0].headers, { n: '1' });
        assert.equal(out[2].timestamp, 1700000000000);
    });

    it('msg.partition of a partition that does not exist: done(err), nothing is written', async function () {
        const topic = uniqueName('rt-nopart');
        await createTopic(topic, 2);
        await helper.load([brokerNode, producerNode], roundTripFlow(topic).filter((n) => n.id !== 'k1'));
        const p1 = helper.getNode('p1');
        await nextStatus(p1, (s) => s.fill === 'green', 30000);
        const bad = await outcome('p1', { payload: 'lost?', partition: 7 });
        assert.ok(bad.error, 'a message for partition 7 of a 2-partition topic must fail');
        assert.match(bad.error.error.message, /Partition 7 does not exist/);
        const good = await expectOk('p1', { payload: 'kept', partition: 1 });
        assert.equal(good.kafka.partition, 1);
        const stored = await readAll(topic);
        assert.deepEqual(stored.map((r) => `${r.partition}:${r.value}`), ['1:kept']);
    });

    it('a topic that does not exist yet is created on first use: the first messages are sent, no error or warning (R1)', async function () {
        // the broker auto-creates topics (Kafka's default, auto.create.topics.enable=true); while it does,
        // it answers UNKNOWN_TOPIC_OR_PARTITION for a moment, which the producer waits out
        const configured = uniqueName('rt-auto-configured');
        const fromMsg = uniqueName('rt-auto-msg');
        const existing = await withAdmin((admin) => admin.listTopics());
        assert.ok(!existing.includes(configured) && !existing.includes(fromMsg), 'fresh topic names');
        await helper.load([brokerNode, producerNode], [
            brokerConfig(),
            { id: 'p1', type: 'kafkajs-nc-producer', name: 'configured', broker: 'b1', topic: configured, wires: [[]] },
            { id: 'p2', type: 'kafkajs-nc-producer', name: 'msg.topic', broker: 'b1', topic: '', queueMode: 'off', wires: [[]] },
            ...outcomeNodes(['p1', 'p2'])
        ]);
        const p1 = helper.getNode('p1');
        const p2 = helper.getNode('p2');
        await nextStatus(p1, (s) => s.fill === 'green', 30000);
        await nextStatus(p2, (s) => s.fill === 'green', 30000);
        // several messages at once: the first request waits for the topic, the rest follow in order
        const results = await Promise.all([
            outcome('p1', { payload: 'c1' }), outcome('p1', { payload: 'c2' }),
            outcome('p2', { payload: 'm1', topic: fromMsg }), outcome('p2', { payload: 'm2', topic: fromMsg })
        ]);
        for (const r of results) {
            assert.ok(r.ok, `failed: ${r.error && r.error.error && r.error.error.message}`);
            assert.equal(r.ok.kafka.status, 'sent', 'sent, not queued');
        }
        assert.deepEqual(results.map((r) => r.ok.kafka.offset), ['0', '1', '0', '1']);
        for (const node of [p1, p2]) {
            assert.deepEqual(calls(node, 'error'), [], `${node.name}: no error`);
            assert.deepEqual(calls(node, 'warn'), [], `${node.name}: no warning`);
        }
        assert.deepEqual((await readAll(configured)).map((r) => r.value.toString()), ['c1', 'c2']);
        assert.deepEqual((await readAll(fromMsg)).map((r) => r.value.toString()), ['m1', 'm2']);
    });

    it('JSON consumer output and tombstones', async function () {
        const topic = uniqueName('rt-json');
        await createTopic(topic, 1);
        await helper.load([brokerNode, producerNode, consumerNode], roundTripFlow(topic, {}, { payloadFormat: 'json' }));
        const p1 = helper.getNode('p1');
        await nextStatus(p1, (s) => s.fill === 'green', 30000);
        const out = collect('out');
        const caught = collect('caught');
        await expectOk('p1', { payload: { temperature: 21.5, unit: 'C' }, key: 'sensor-1' });
        await expectOk('p1', { payload: null, key: 'sensor-1' }); // tombstone
        await expectOk('p1', { payload: 'not json' });
        await waitFor(() => out.length === 2 && caught.length === 1, { timeout: 60000 });
        assert.deepEqual(out[0].payload, { temperature: 21.5, unit: 'C' });
        assert.equal(out[1].payload, null);
        assert.equal(out[1].key, 'sensor-1');
        assert.equal(caught[0].payload, 'not json');
        assert.match(caught[0].error.message, /Invalid JSON/);
    });
});
