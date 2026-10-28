'use strict';

/**
 * Consumer node → real Kafka: output shape, control actions, regex
 * subscriptions, missing topics, and no zombie group members after close
 * (the upstream bug: a consumer closed while it was still joining kept
 * consuming after a redeploy).
 */

const assert = require('node:assert/strict');
const brokerNode = require('../../nodes/kafka-broker.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const producerNode = require('../../nodes/kafka-producer.js');
const { sleep, waitFor, nextStatus, statusMark } = require('../helpers/common');
const {
    helper, uniqueName, requireKafka, createTopic, produceRaw, groupMembers, useNodeRed, brokerConfig, outcomeNodes,
    collect, expectOk, outcome, calls, withAdmin
} = require('./e2e-helpers');

/** The committed offset of partition 0 of `topic` for `groupId` ('-1' when none). */
async function committed(groupId, topic) {
    const offsets = await withAdmin((admin) => admin.fetchOffsets({ groupId, topics: [topic] }));
    return offsets[0].partitions.find((p) => p.partition === 0).offset;
}

const values = (list) => list.map((m) => m.payload);

/** The minimum session timeout a default broker accepts (group.min.session.timeout.ms). */
const SESSION_MS = 6000;

describe('e2e: consumer node', function () {
    this.timeout(180000);

    before(function () {
        requireKafka(this);
    });

    useNodeRed();

    async function load(consumerExtra = {}, brokerExtra = {}) {
        await helper.load([brokerNode, consumerNode], [
            brokerConfig(brokerExtra),
            {
                id: 'k1', type: 'kafkajs-nc-consumer', name: 'consumer', broker: 'b1', sessionTimeout: String(SESSION_MS),
                heartbeatInterval: '1000', maxWaitTimeInMs: '200', wires: [['out']], ...consumerExtra
            },
            { id: 'out', type: 'helper' },
            ...outcomeNodes(['k1'])
        ]);
        return helper.getNode('k1');
    }

    it('output shape: key, headers, offsets, timestamp, highWatermark, tombstone, invalid JSON to Catch', async function () {
        const topic = uniqueName('c-shape');
        await createTopic(topic, 1);
        await produceRaw(topic, [
            { key: 'k1', value: '{"a":1}', headers: { h: 'v', multi: ['1', '2'] }, timestamp: '1700000000000' },
            { key: null, value: 'not json' },
            { key: 'gone', value: null },
            { value: Buffer.from('[1,2]') }
        ]);
        const k1 = await load({ topics: topic, fromBeginning: true, payloadFormat: 'json' });
        const out = collect('out');
        const caught = collect('caught');
        await waitFor(() => out.length === 3 && caught.length === 1, { timeout: 60000 });
        await nextStatus(k1, (s) => s.fill === 'green' && s.text === `connected · ${topic}`, 10000);
        assert.deepEqual(out[0].payload, { a: 1 });
        assert.equal(out[0].key, 'k1');
        assert.deepEqual(out[0].headers, { h: 'v', multi: ['1', '2'] });
        assert.equal(out[0].offset, '0');
        assert.equal(out[0].timestamp, 1700000000000);
        assert.equal(out[0].kafka.groupId, 'node-red-k1');
        assert.equal(out[0].kafka.highWatermark, '4');
        assert.equal(out[1].payload, null); // tombstone
        assert.equal(out[1].key, 'gone');
        assert.deepEqual(out[2].payload, [1, 2]);
        assert.equal(caught[0].payload, 'not json');
        assert.match(caught[0].error.message, /Invalid JSON/);
    });

    it('control: pause/resume, manual commit, seek earliest, stop/start, restart (one group member)', async function () {
        const topic = uniqueName('c-ctl');
        const groupId = uniqueName('c-ctl-group');
        await createTopic(topic, 1);
        const k1 = await load({ topics: topic, groupId, commitMode: 'manual' });
        const out = collect('out');
        await nextStatus(k1, (s) => s.fill === 'green', 60000);

        await expectOk('k1', { action: 'pause' });
        await nextStatus(k1, (s) => s.text === `paused · ${topic}`);
        await produceRaw(topic, [{ value: 'm0' }, { value: 'm1' }]);
        await sleep(2500);
        assert.equal(out.length, 0, 'paused: nothing delivered');
        await expectOk('k1', { action: 'resume' });
        await waitFor(() => out.length === 2, { timeout: 30000 });
        assert.deepEqual(out.map((m) => m.payload), ['m0', 'm1']);

        // commit the first message only (msg.kafka of an output message)
        await expectOk('k1', { ...out[0], action: 'commit' });
        // seek back to the beginning: both again
        await expectOk('k1', { action: 'seek', payload: { partition: 0, offset: 'earliest' } });
        await waitFor(() => out.length === 4, { timeout: 30000 });
        assert.deepEqual(out.slice(2).map((m) => m.payload), ['m0', 'm1']);

        // stop: the member leaves the group
        await expectOk('k1', { action: 'stop' });
        await nextStatus(k1, (s) => s.text === 'stopped');
        assert.equal((await groupMembers(groupId)).length, 0);

        // start: resumes from the committed offset (1), so m1 is delivered again (at least once)
        await expectOk('k1', { action: 'start' });
        await waitFor(() => out.length === 5, { timeout: 60000 });
        assert.equal(out[4].payload, 'm1');
        assert.equal(out[4].offset, '1');

        // restart: exactly one member afterwards
        const mark = statusMark(k1);
        await expectOk('k1', { action: 'restart' });
        await nextStatus(k1, (s) => s.fill === 'green', { timeout: 60000, since: mark });
        await sleep(1000);
        assert.equal((await groupMembers(groupId)).length, 1);
    });

    it('seek "earliest" in auto commit mode re-reads the partition and stores that position (fromBeginning off)', async function () {
        // kafkajs' -2 marker would apply fromBeginning instead: nothing re-read, and "latest" committed
        const topic = uniqueName('c-seek-auto');
        const groupId = uniqueName('c-seek-auto-group');
        await createTopic(topic, 1);
        await produceRaw(topic, [0, 1, 2, 3].map((i) => ({ value: `m${i}` })));
        const k1 = await load({ topics: topic, groupId, fromBeginning: false, autoCommitInterval: '0' });
        const out = collect('out');
        await nextStatus(k1, (s) => s.fill === 'green', 60000);
        await sleep(1500);
        // a backlog that arrived while paused must not be skipped by the seek either
        await expectOk('k1', { action: 'pause' });
        await produceRaw(topic, [{ value: 'm4' }, { value: 'm5' }]);
        await expectOk('k1', { action: 'seek', payload: { partition: 0, offset: 'earliest' } });
        await expectOk('k1', { action: 'resume' });
        await waitFor(() => out.length === 6, { timeout: 30000 });
        assert.deepEqual(values(out), ['m0', 'm1', 'm2', 'm3', 'm4', 'm5']);
        await waitFor(async () => (await committed(groupId, topic)) === '6', { timeout: 15000, interval: 300 });
    });

    it('seek "latest" skips to the end, also with fromBeginning on (manual commit mode)', async function () {
        const topic = uniqueName('c-seek-latest');
        await createTopic(topic, 1);
        await produceRaw(topic, [0, 1, 2].map((i) => ({ value: `m${i}` })));
        await load({ topics: topic, groupId: uniqueName('c-seek-latest-group'), fromBeginning: true, commitMode: 'manual' });
        const out = collect('out');
        await waitFor(() => out.length === 3, { timeout: 60000 });
        await expectOk('k1', { action: 'seek', payload: { partition: 0, offset: 'latest' } });
        await sleep(3000);
        assert.equal(out.length, 3, `re-delivered: ${values(out.slice(3))}`);
        await produceRaw(topic, [{ value: 'new' }]);
        await waitFor(() => out.length === 4, { timeout: 30000 });
        assert.equal(out[3].payload, 'new');
    });

    it('seek "earliest" goes to the log start offset after retention deleted records (manual mode)', async function () {
        const topic = uniqueName('c-seek-logstart');
        await createTopic(topic, 1);
        await produceRaw(topic, [0, 1, 2, 3, 4].map((i) => ({ value: `m${i}` })));
        await withAdmin(async (admin) => {
            await admin.fetchTopicMetadata({ topics: [topic] }); // deleteTopicRecords needs the metadata loaded
            await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '3' }] });
        });
        await load({ topics: topic, groupId: uniqueName('c-seek-logstart-group'), commitMode: 'manual' });
        const out = collect('out');
        await nextStatus(helper.getNode('k1'), (s) => s.fill === 'green', 60000);
        await sleep(1500);
        await expectOk('k1', { action: 'seek', payload: { partition: 0, offset: 'earliest' } });
        await waitFor(() => out.length === 2, { timeout: 30000 });
        assert.deepEqual(values(out), ['m3', 'm4']);
    });

    it('a seek outside the partition\'s offsets is refused and keeps the manually committed position', async function () {
        const topic = uniqueName('c-seek-range');
        const groupId = uniqueName('c-seek-range-group');
        await createTopic(topic, 1);
        await produceRaw(topic, [0, 1, 2, 3, 4].map((i) => ({ value: `m${i}` })));
        await load({ topics: topic, groupId, fromBeginning: true, commitMode: 'manual' });
        const out = collect('out');
        await waitFor(() => out.length === 5, { timeout: 60000 });
        await expectOk('k1', { ...out[1], action: 'commit' });
        assert.equal(await committed(groupId, topic), '2');
        const r = await outcome('k1', { action: 'seek', payload: { partition: 0, offset: 100 } });
        assert.match(r.error.error.message, /seek: offset 100 is outside the offsets of .*\[0\] \(0 to 5\)/);
        await sleep(2000);
        // kafkajs would have answered OffsetOutOfRange by committing its default position (-1)
        assert.equal(await committed(groupId, topic), '2');
        // the position is kept: a restart delivers m2..m4 again
        const before = out.length;
        await expectOk('k1', { action: 'restart' });
        await waitFor(() => out.length === before + 3, { timeout: 60000 });
        assert.deepEqual(values(out.slice(before)), ['m2', 'm3', 'm4']);
    });

    it('consume → producer → commit: the producer\'s msg.kafka is refused; the kept copy commits the input topic', async function () {
        const IN = uniqueName('c-ctp-in');
        const OUT = uniqueName('c-ctp-out');
        const groupId = uniqueName('c-ctp-group');
        await createTopic(IN, 1);
        await createTopic(OUT, 3);
        await produceRaw(IN, [0, 1, 2].map((i) => ({ value: `r${i}` })));
        await helper.load([brokerNode, consumerNode, producerNode], [
            brokerConfig(),
            {
                id: 'k1', type: 'kafkajs-nc-consumer', broker: 'b1', topics: IN, groupId, commitMode: 'manual', fromBeginning: true,
                sessionTimeout: String(SESSION_MS), heartbeatInterval: '1000', maxWaitTimeInMs: '200', wires: [['keep']]
            },
            { id: 'keep', type: 'helper' },
            { id: 'p1', type: 'kafkajs-nc-producer', broker: 'b1', topic: OUT, wires: [['reports']] },
            { id: 'reports', type: 'helper' },
            ...outcomeNodes(['k1', 'p1'])
        ]);
        // the documented pattern (a change node in a real flow): keep a copy of the consumer's msg.kafka
        helper.getNode('keep').on('input', (msg) => helper.getNode('p1').receive({ ...msg, consumed: msg.kafka }));
        const reports = collect('reports');
        await waitFor(() => reports.length === 3, { timeout: 60000 });
        assert.equal(reports[0].kafka.topic, OUT, 'the producer replaced msg.kafka with its report');
        for (const r of reports) {
            const res = await outcome('k1', { ...r, action: 'commit' });
            assert.match(res.error.error.message, /msg\.kafka is not a message of this consumer \(it has no groupId\)/);
        }
        const offsets = await withAdmin((admin) => admin.fetchOffsets({ groupId }));
        assert.deepEqual(offsets.filter((t) => t.topic === OUT).flatMap((t) => t.partitions).filter((p) => p.offset !== '-1'), [],
            'nothing committed on the output topic');
        await expectOk('k1', { ...reports[2], kafka: reports[2].consumed, action: 'commit' });
        assert.equal(await committed(groupId, IN), '3');
    });

    it('a topic regex that matches no topic: yellow "no partitions" status and a warning', async function () {
        const k1 = await load({ topics: `^${uniqueName('c-rxnone')}-`, topicIsRegex: true });
        const s = await nextStatus(k1, (st) => st.shape === 'dot', 60000);
        assert.deepEqual([s.fill, s.text], ['yellow', 'connected · no partitions']);
        await waitFor(() => calls(k1, 'warn').some((w) => /No partition is assigned to this consumer: the topic pattern .* matches no topic/.test(w)));
    });

    it('regex subscription', async function () {
        const prefix = uniqueName('c-rx');
        await createTopic(`${prefix}-a`, 1);
        await createTopic(`${prefix}-b`, 1);
        await produceRaw(`${prefix}-a`, [{ value: 'A' }]);
        await produceRaw(`${prefix}-b`, [{ value: 'B' }]);
        await load({ topics: `^${prefix}-`, topicIsRegex: true, fromBeginning: true });
        const out = collect('out');
        await waitFor(() => out.length === 2, { timeout: 60000 });
        assert.deepEqual(out.map((m) => m.payload).sort(), ['A', 'B']);
    });

    it('a topic that does not exist (no auto-creation): clear status, retries, no crash', async function () {
        const k1 = await load({ topics: uniqueName('c-missing'), allowAutoTopicCreation: false });
        const s = await nextStatus(k1, (st) => st.fill === 'yellow' && / · retry /.test(st.text), 60000);
        assert.match(s.text, /^unknown topic\/partition · retry /);
        // usually temporary: logged as a warning, not an error
        await waitFor(() => calls(k1, 'warn').some((e) => /Kafka connection failed: .*UNKNOWN_TOPIC_OR_PARTITION.*check that the topic exists/.test(e)),
            { message: calls(k1, 'warn').join(' / ') });
        assert.deepEqual(calls(k1, 'error').filter((e) => /UNKNOWN_TOPIC_OR_PARTITION/.test(e)), []);
    });

    it('unreachable broker: retry status, close is fast', async function () {
        const k1 = await load({ topics: 'nothing' }, { brokers: '127.0.0.1:9', connectionTimeout: '500', retries: '1' });
        const s = await nextStatus(k1, (st) => / · retry /.test(st.text), 30000);
        assert.match(s.text, /ECONNREFUSED 127\.0\.0\.1:9 · retry/);
        const started = Date.now();
        await helper.unload();
        assert.ok(Date.now() - started < 3000, `close took ${Date.now() - started} ms`);
    });

    it('a redeploy of a running consumer is quick with the default settings', async function () {
        const topic = uniqueName('c-redeploy');
        await createTopic(topic, 1);
        // default fetch settings: stopping waits for the fetch in flight (Max wait, 1000 ms by default)
        await helper.load([brokerNode, consumerNode], [
            brokerConfig(),
            { id: 'k1', type: 'kafkajs-nc-consumer', broker: 'b1', topics: topic, wires: [['out']] },
            { id: 'out', type: 'helper' }
        ]);
        const k1 = helper.getNode('k1');
        await nextStatus(k1, (s) => s.fill === 'green', 60000);
        await sleep(2000); // idle: a fetch is waiting on the broker
        const started = Date.now();
        await helper.unload();
        const took = Date.now() - started;
        assert.ok(took < 2000, `close took ${took} ms`);
    });

    for (const delay of [5, 300, 1500]) {
        it(`closing ${delay} ms after deploy leaves no zombie group member`, async function () {
            const topic = uniqueName('c-zombie');
            const groupId = uniqueName(`c-zombie-${delay}`);
            await createTopic(topic, 1);
            await load({ topics: topic, groupId });
            await sleep(delay);
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started < 6000, `close took ${Date.now() - started} ms`);
            // longer than the session timeout: a member that left uncleanly is gone by now,
            // a zombie (still heartbeating) would still be listed
            await sleep(SESSION_MS + 3000);
            const members = await groupMembers(groupId);
            assert.equal(members.length, 0, `members: ${JSON.stringify(members.map((m) => m.clientId))}`);
            // nothing new can join later either
            await produceRaw(topic, [{ value: 'late' }]);
            await sleep(1500);
            assert.equal((await groupMembers(groupId)).length, 0);
        });
    }
});
