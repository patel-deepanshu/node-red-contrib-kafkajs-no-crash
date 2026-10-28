'use strict';

/** Admin node → real Kafka: every action, plus error handling that must not drop the connection. */

const assert = require('node:assert/strict');
const brokerNode = require('../../nodes/kafka-broker.js');
const adminNode = require('../../nodes/kafka-admin.js');
const { nextStatus, waitFor } = require('../helpers/common');
const {
    helper, uniqueName, requireKafka, produceRaw, testClient, withAdmin, useNodeRed, brokerConfig, outcomeNodes, outcome,
    expectOk
} = require('./e2e-helpers');

/** Run one admin call with the checker client (not the node under test). */
function helperAdmin(method, args) {
    return withAdmin((admin) => admin[method](args));
}

describe('e2e: admin node', function () {
    this.timeout(120000);

    before(function () {
        requireKafka(this);
    });

    useNodeRed();

    async function load(adminExtra = {}) {
        await helper.load([brokerNode, adminNode], [
            brokerConfig(),
            { id: 'a1', type: 'kafkajs-nc-admin', name: 'admin', broker: 'b1', timeout: '30000', wires: [['out']], ...adminExtra },
            { id: 'out', type: 'helper' },
            ...outcomeNodes(['a1'])
        ]);
        return helper.getNode('a1');
    }

    it('topics: create, list, metadata, partitions, offsets, configs, delete', async function () {
        const a1 = await load();
        const topic = uniqueName('admin-topic');

        const created = await expectOk('a1', {
            action: 'createTopics',
            payload: { topics: [{ topic, numPartitions: '2', replicationFactor: 1, configEntries: { 'retention.ms': 3600000 } }], waitForLeaders: true }
        });
        assert.equal(created.payload, true);
        assert.equal(created.kafka.action, 'createTopics');
        assert.equal(typeof created.kafka.durationMs, 'number');
        await nextStatus(a1, (s) => s.fill === 'green' && s.text === 'ok · createTopics');

        const topics = await expectOk('a1', { action: 'listTopics' });
        assert.ok(topics.payload.includes(topic));

        const meta = await expectOk('a1', { action: 'fetchTopicMetadata', payload: topic });
        assert.equal(meta.payload.topics[0].name, topic);
        assert.equal(meta.payload.topics[0].partitions.length, 2);

        await expectOk('a1', { action: 'createPartitions', payload: { topicPartitions: [{ topic, count: 3 }] } });
        await waitFor(async () => {
            const m = await expectOk('a1', { action: 'fetchTopicMetadata', payload: { topics: [topic] } });
            return m.payload.topics[0].partitions.length === 3;
        }, { timeout: 30000, interval: 300 });

        const before = Date.now();
        await produceRaw(topic, [{ value: 'a', partition: 0 }, { value: 'b', partition: 0 }, { value: 'c', partition: 2 }]);
        const offsets = await expectOk('a1', { action: 'fetchTopicOffsets', payload: { topic } });
        const high = Object.fromEntries(offsets.payload.map((o) => [o.partition, o.high]));
        assert.deepEqual(high, { 0: '2', 1: '0', 2: '1' });
        const byTime = await expectOk('a1', { action: 'fetchTopicOffsetsByTimestamp', payload: { topic, timestamp: new Date(before - 60000).toISOString() } });
        assert.equal(byTime.payload.find((o) => o.partition === 0).offset, '0');

        const configs = await expectOk('a1', {
            action: 'describeConfigs', payload: { resources: [{ type: 'topic', name: topic, configNames: ['retention.ms'] }] }
        });
        const entry = configs.payload.resources[0].configEntries.find((e) => e.configName === 'retention.ms');
        assert.equal(entry.configValue, '3600000');

        const deleted = await expectOk('a1', { action: 'deleteTopics', payload: topic });
        assert.equal(deleted.payload, true);
        await waitFor(async () => !(await expectOk('a1', { action: 'listTopics' })).payload.includes(topic),
            { timeout: 30000, interval: 300 });
    });

    it('a request error goes to Catch and the node keeps working', async function () {
        const a1 = await load({ action: 'createPartitions' });
        const topic = uniqueName('admin-err');
        assert.equal(await helperAdmin('createTopics', { topics: [{ topic }], waitForLeaders: true }), true);
        // (kafkajs resolves false, not an error, when every topic exists already)
        assert.equal(await helperAdmin('createTopics', { topics: [{ topic }] }), false);
        // shrinking a topic is refused by Kafka (INVALID_PARTITIONS)
        const bad = await outcome('a1', { payload: { topicPartitions: [{ topic, count: 1 }] } });
        assert.ok(bad.error, 'reducing the partition count must fail');
        assert.match(bad.error.error.message, /partition/i);
        await nextStatus(a1, (s) => s.fill === 'red' && /^createPartitions: /.test(s.text));
        // validation errors never reach Kafka
        const invalid = await outcome('a1', { payload: { topicPartitions: [{ topic }] } });
        assert.match(invalid.error.error.message, /count \(the new total number of partitions\) is required/);
        // and the node still works
        const ok = await expectOk('a1', { payload: { topicPartitions: [{ topic, count: 2 }] } });
        assert.equal(ok.payload, true);
        await nextStatus(a1, (s) => s.fill === 'green' && s.text === 'ok · createPartitions');
    });

    it('consumer groups: list, describe, fetch/set/reset offsets, delete', async function () {
        await load();
        const topic = uniqueName('admin-group-topic');
        const groupId = uniqueName('admin-group');
        await expectOk('a1', { action: 'createTopics', payload: { topics: [{ topic }], waitForLeaders: true } });
        await produceRaw(topic, [{ value: '1' }, { value: '2' }, { value: '3' }]);
        // a group with committed offsets, then left empty
        const consumer = testClient().consumer({ groupId, sessionTimeout: 10000, heartbeatInterval: 1000, maxWaitTimeInMs: 200 });
        await consumer.connect();
        let read = 0;
        try {
            await consumer.subscribe({ topics: [topic], fromBeginning: true });
            await consumer.run({ eachMessage: async () => { read++; } });
            await waitFor(() => read === 3, { timeout: 30000 });
        } finally {
            await consumer.disconnect();
        }

        const groups = await expectOk('a1', { action: 'listGroups' });
        assert.ok(groups.payload.groups.some((g) => g.groupId === groupId));
        const described = await expectOk('a1', { action: 'describeGroups', payload: groupId });
        assert.equal(described.payload.groups[0].groupId, groupId);
        await waitFor(async () => {
            const offsets = await expectOk('a1', { action: 'fetchOffsets', payload: { groupId, topics: [topic] } });
            return offsets.payload[0].partitions[0].offset === '3';
        }, { timeout: 15000, interval: 300 });

        await expectOk('a1', { action: 'setOffsets', payload: { groupId, topic, partitions: [{ partition: 0, offset: 1 }] } });
        let fetched = await expectOk('a1', { action: 'fetchOffsets', payload: { groupId, topics: [topic] } });
        assert.equal(fetched.payload[0].partitions[0].offset, '1');
        // kafkajs commits the "earliest" marker (-2); resolveOffsets turns it into the real offset
        await expectOk('a1', { action: 'resetOffsets', payload: { groupId, topic, earliest: true } });
        fetched = await expectOk('a1', { action: 'fetchOffsets', payload: { groupId, topics: [topic] } });
        assert.equal(fetched.payload[0].partitions[0].offset, '-2');
        fetched = await expectOk('a1', { action: 'fetchOffsets', payload: { groupId, topics: [topic], resolveOffsets: true } });
        assert.equal(fetched.payload[0].partitions[0].offset, '0');

        const deleted = await expectOk('a1', { action: 'deleteGroups', payload: [groupId] });
        assert.equal(deleted.payload[0].groupId, groupId);
    });

    it('describeCluster, and an unknown action fails with the list of valid actions', async function () {
        await load();
        const cluster = await expectOk('a1', { action: 'describeCluster' });
        assert.ok(cluster.payload.brokers.length >= 1);
        assert.equal(typeof cluster.payload.clusterId, 'string');
        const bad = await outcome('a1', { action: 'dropEverything' });
        assert.match(bad.error.error.message, /Unknown admin action "dropEverything"\. Valid actions: listTopics/);
    });

    it('unreachable broker: the message fails after the timeout, the status says why, close is fast', async function () {
        await helper.load([brokerNode, adminNode], [
            brokerConfig({ brokers: '127.0.0.1:9', connectionTimeout: '500', retries: '1' }),
            { id: 'a1', type: 'kafkajs-nc-admin', broker: 'b1', action: 'listTopics', timeout: '3000', wires: [['out']] },
            { id: 'out', type: 'helper' },
            ...outcomeNodes(['a1'])
        ]);
        const a1 = helper.getNode('a1');
        const r = await outcome('a1', {});
        assert.ok(r.error);
        assert.match(r.error.error.message, /ECONNREFUSED|timed out/);
        await nextStatus(a1, (s) => s.fill === 'red' && /listTopics/.test(s.text));
        const started = Date.now();
        await helper.unload();
        assert.ok(Date.now() - started < 3500, `close took ${Date.now() - started} ms`);
    });
});
