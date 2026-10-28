'use strict';

/**
 * Broker outages with a REAL broker that the test stops, kills, freezes and
 * restarts (KAFKA_E2E_CONTROL=1, scripts/kafka/*.sh). These are the
 * situations the palette exists for:
 *  - nothing produced is lost, the order is kept (duplicates are allowed:
 *    delivery is at-least-once), and nothing crashes;
 *  - the consumer reconnects by itself and stays a single group member;
 *  - a redeploy during an outage keeps the backlog on disk;
 *  - closing never hangs on a frozen broker.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const adminNode = require('../../nodes/kafka-admin.js');
const { sleep, waitFor, nextStatus, statusMark } = require('../helpers/common');
const {
    helper, uniqueName, requireKafka, createTopic, readAll, groupMembers, broker, useNodeRed, brokerConfig,
    outcomeNodes, collect, expectOk, calls
} = require('./e2e-helpers');

const NODES = [brokerNode, producerNode, consumerNode, adminNode];

/**
 * The values read back must contain every sequence number 0..total-1, and
 * their first occurrences must be in order. Returns the number of duplicates.
 */
function assertCompleteAndOrdered(values, total) {
    const seen = new Set();
    const firsts = [];
    for (const v of values) {
        if (!seen.has(v)) {
            seen.add(v);
            firsts.push(v);
        }
    }
    const missing = [];
    for (let i = 0; i < total; i++) if (!seen.has(i)) missing.push(i);
    assert.deepEqual(missing, [], `lost messages: ${missing.slice(0, 20).join(', ')}`);
    const outOfOrder = firsts.findIndex((v, i) => v !== i);
    assert.equal(outOfOrder, -1, `order broken at position ${outOfOrder}: ${firsts.slice(Math.max(0, outOfOrder - 3), outOfOrder + 3)}`);
    return values.length - seen.size;
}

describe('e2e: broker outages (broker stopped, killed, frozen)', function () {
    this.timeout(300000);

    before(function () {
        requireKafka(this, 'control');
    });

    useNodeRed();

    afterEach(async function () {
        this.timeout(200000);
        await broker.ensureUp(); // a failed test may have left it stopped or frozen
    });

    function flow(topic, groupId, brokerExtra = {}) {
        return [
            brokerConfig({ requestTimeout: '20000', ...brokerExtra }),
            { id: 'p1', type: 'kafkajs-nc-producer', broker: 'b1', topic, batchSize: '50', wires: [['reports']] },
            { id: 'reports', type: 'helper' },
            {
                id: 'k1', type: 'kafkajs-nc-consumer', broker: 'b1', topics: topic, groupId, fromBeginning: true,
                sessionTimeout: '10000', heartbeatInterval: '1000', maxWaitTimeInMs: '200', wires: [['out']]
            },
            { id: 'out', type: 'helper' },
            { id: 'a1', type: 'kafkajs-nc-admin', broker: 'b1', action: 'listTopics', timeout: '5000', wires: [[]] },
            ...outcomeNodes(['p1', 'k1', 'a1'])
        ];
    }

    /** Feed the producer a sequence number every `ms` until stopped. */
    function feeder(node, ms) {
        let seq = 0;
        const timer = setInterval(() => node.receive({ payload: String(seq++) }), ms);
        return { stop() { clearInterval(timer); return seq; }, get count() { return seq; } };
    }

    async function loadConnected(topic, groupId) {
        await helper.load(NODES, flow(topic, groupId));
        const p1 = helper.getNode('p1');
        const k1 = helper.getNode('k1');
        await nextStatus(p1, (s) => s.fill === 'green', 60000);
        await nextStatus(k1, (s) => s.fill === 'green', 60000);
        return { p1, k1 };
    }

    async function drained(p1, timeout = 120000) {
        await waitFor(() => {
            const s = p1.producerRuntime.snapshot();
            return s.state === 'connected' && s.mode === 'live' && s.queued === 0 && !s.flushing && s.pending === 0;
        }, { timeout, interval: 100, message: 'producer back to live with an empty queue' });
    }

    it('broker crash (SIGKILL) and restart during continuous produce: nothing lost, order kept, consumer recovers', async function () {
        const topic = uniqueName('out-kill');
        const groupId = uniqueName('out-kill-group');
        await createTopic(topic, 1);
        const { p1, k1 } = await loadConnected(topic, groupId);
        const reports = collect('reports');
        const caught = collect('caught');
        const out = collect('out');

        const feed = feeder(p1, 20);
        let total;
        try {
            await sleep(1500);
            await broker.stop(true);
            await nextStatus(p1, (s) => / queued/.test(s.text), 60000);
            await sleep(5000); // outage: messages go to the offline queue on disk
            assert.ok(p1.producerRuntime.snapshot().queued > 0, 'messages are queued during the outage');
            await broker.start();
            await sleep(3000);
        } finally {
            total = feed.stop();
        }

        await waitFor(() => reports.length + caught.length >= total, { timeout: 120000, message: `reports ${reports.length}/${total}` });
        assert.deepEqual(caught.map((m) => m.error.message), []);
        assert.ok(reports.some((m) => m.kafka.status === 'queued'), 'some messages were queued');
        await drained(p1);
        await nextStatus(p1, (s) => s.fill === 'green' && s.text === 'connected', 30000);

        const values = (await readAll(topic)).map((r) => Number(r.value.toString()));
        const duplicates = assertCompleteAndOrdered(values, total);
        console.log(`      ${total} produced, ${values.length} stored, ${duplicates} duplicate(s)`);

        // the consumer reconnected by itself and consumed up to the end, as a single member
        await waitFor(() => out.some((m) => m.payload === String(total - 1)), { timeout: 120000, message: 'consumer caught up' });
        await nextStatus(k1, (s) => s.fill === 'green', 60000);
        assert.equal((await groupMembers(groupId)).length, 1);
        assert.ok(calls(p1, 'log').some((l) => /Sent \d+ queued message\(s\) to Kafka; the offline queue is empty/.test(l)));
    });

    it('frozen broker (SIGSTOP) during continuous produce: nothing lost, order kept, no duplicates', async function () {
        const topic = uniqueName('out-freeze');
        await createTopic(topic, 1);
        const { p1 } = await loadConnected(topic, uniqueName('out-freeze-group'));
        const reports = collect('reports');
        const caught = collect('caught');

        const feed = feeder(p1, 20);
        let total;
        try {
            await sleep(1000);
            await broker.pause();
            // a request stays in flight; after liveStall (3 s) new messages go to the durable queue
            await waitFor(() => p1.producerRuntime.snapshot().queued > 0, { timeout: 15000, message: 'switched to the queue' });
            await sleep(3000);
            await broker.resume();
            await sleep(2000);
        } finally {
            total = feed.stop();
            await broker.resume().catch(() => {});
        }

        await waitFor(() => reports.length + caught.length >= total, { timeout: 120000, message: `reports ${reports.length}/${total}` });
        assert.deepEqual(caught.map((m) => m.error.message), []);
        await drained(p1);
        const values = (await readAll(topic)).map((r) => Number(r.value.toString()));
        const duplicates = assertCompleteAndOrdered(values, total);
        // the in-flight request succeeded late; its copy at the head of the queue was removed (ackSentHead)
        assert.equal(duplicates, 0, `${duplicates} duplicate(s) in ${values.length} stored messages`);
    });

    it('redeploy during an outage: the backlog survives on disk and is delivered after the broker is back', async function () {
        const topic = uniqueName('out-redeploy');
        await createTopic(topic, 1);
        const { p1 } = await loadConnected(topic, uniqueName('out-redeploy-group'));
        await expectOk('p1', { payload: 'before' });
        await broker.stop(true);
        for (let i = 0; i < 20; i++) {
            const r = await expectOk('p1', { payload: `during-${i}` });
            assert.equal(r.kafka.status, 'queued');
        }
        const dir = p1.producerRuntime.queueDir;
        const started = Date.now();
        await helper.unload(); // redeploy, part 1
        assert.ok(Date.now() - started < 11000, `close took ${Date.now() - started} ms`);
        assert.ok(fs.readdirSync(dir).some((f) => /^seg-\d+\.jsonl$/.test(f)), 'queue segment kept on disk');

        await helper.load(NODES, flow(topic, uniqueName('out-redeploy-group2'))); // redeploy, part 2 (same node id)
        const p1b = helper.getNode('p1');
        await nextStatus(p1b, (s) => / 20 queued/.test(s.text), 30000);
        assert.ok(calls(p1b, 'log').some((l) => /The offline queue holds 20 message\(s\) from earlier/.test(l)));
        await broker.start();
        await drained(p1b);
        const values = (await readAll(topic)).map((r) => r.value.toString());
        assert.deepEqual(values, ['before', ...Array.from({ length: 20 }, (_, i) => `during-${i}`)]);
        assert.ok(!fs.readdirSync(dir).some((f) => /^seg-\d+\.jsonl$/.test(f)), 'queue files removed once empty');
        assert.equal(path.basename(dir), 'p1');
    });

    it('closing with a frozen broker never hangs (producer mid-send, consumer joined, admin connected)', async function () {
        const topic = uniqueName('out-close');
        await createTopic(topic, 1);
        const { p1 } = await loadConnected(topic, uniqueName('out-close-group'));
        await expectOk('a1', {});
        await broker.pause();
        try {
            for (let i = 0; i < 5; i++) p1.receive({ payload: `stuck-${i}` });
            await sleep(500);
            const started = Date.now();
            await helper.unload();
            const took = Date.now() - started;
            assert.ok(took < 11000, `close took ${took} ms (Node-RED gives up after 15 s)`);
        } finally {
            await broker.resume();
        }
        await sleep(5000); // late kafkajs activity after the resume happens under the crash trap
    });

    it('a consumer whose broker restarts reconnects without a redeploy and does not duplicate itself', async function () {
        const topic = uniqueName('out-consumer');
        const groupId = uniqueName('out-consumer-group');
        await createTopic(topic, 1);
        const { k1 } = await loadConnected(topic, groupId);
        const out = collect('out');
        await expectOk('p1', { payload: 'one' });
        await waitFor(() => out.length === 1, { timeout: 30000 });
        const mark = statusMark(k1);
        await broker.stop(false); // controlled shutdown
        await nextStatus(k1, (s) => s.fill !== 'green', { timeout: 120000, since: mark });
        await sleep(3000);
        await broker.start();
        await nextStatus(k1, (s) => s.fill === 'green', { timeout: 120000, since: mark });
        await expectOk('p1', { payload: 'two' }, 120000);
        await waitFor(() => out.some((m) => m.payload === 'two'), { timeout: 120000 });
        await sleep(2000);
        assert.equal((await groupMembers(groupId)).length, 1);
    });
});
