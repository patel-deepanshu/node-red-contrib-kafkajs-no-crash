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
const { sleep, waitFor, nextStatus, statusMark } = require('../helpers/common');
const {
    helper, uniqueName, requireKafka, createTopic, produceRaw, groupMembers, useNodeRed, brokerConfig, outcomeNodes,
    collect, expectOk, calls
} = require('./e2e-helpers');

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

    it('a topic that does not exist (no auto-creation): clear red status, slow retry, no crash', async function () {
        const k1 = await load({ topics: uniqueName('c-missing'), allowAutoTopicCreation: false });
        const s = await nextStatus(k1, (st) => st.fill === 'red' && / · retry /.test(st.text), 60000);
        // authRetryMin of the e2e broker config is 1 s
        assert.equal(s.text, 'unknown topic/partition · retry 1s');
        assert.ok(calls(k1, 'error').some((e) => /Kafka connection failed: .*UNKNOWN_TOPIC_OR_PARTITION.*check that the topic exists/.test(e)),
            calls(k1, 'error').join(' / '));
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
