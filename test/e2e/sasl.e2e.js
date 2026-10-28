'use strict';

/**
 * SASL against a real broker: every mechanism works; wrong credentials (the
 * situation that crashed Node-RED upstream) give a clear red status, a slow
 * retry and one log line, keep the messages, and deliver them once the
 * credentials are fixed.
 */

const assert = require('node:assert/strict');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const adminNode = require('../../nodes/kafka-admin.js');
const { sleep, waitFor, nextStatus } = require('../helpers/common');
const {
    E2E, helper, uniqueName, requireKafka, createTopic, readAll, useNodeRed, brokerConfig, outcomeNodes, collect,
    outcome, expectOk, calls
} = require('./e2e-helpers');

const NODES = [brokerNode, producerNode, consumerNode, adminNode];

describe('e2e: SASL', function () {
    this.timeout(180000);

    before(function () {
        requireKafka(this, 'sasl');
    });

    useNodeRed();

    function saslBroker(authMechanism, extra = {}) {
        return brokerConfig({ brokers: E2E.saslBrokers.join(','), authMechanism, ...extra });
    }

    function flow(broker, topic, { producer = {}, consumer = null, admin = false } = {}) {
        const nodes = [
            broker,
            { id: 'p1', type: 'kafkajs-nc-producer', broker: 'b1', topic, wires: [['reports']], ...producer },
            { id: 'reports', type: 'helper' }
        ];
        const ids = ['p1'];
        if (consumer) {
            nodes.push({
                id: 'k1', type: 'kafkajs-nc-consumer', broker: 'b1', topics: topic, fromBeginning: true,
                sessionTimeout: '6000', heartbeatInterval: '1000', maxWaitTimeInMs: '200', wires: [['out']], ...consumer
            }, { id: 'out', type: 'helper' });
            ids.push('k1');
        }
        if (admin) {
            nodes.push({ id: 'a1', type: 'kafkajs-nc-admin', broker: 'b1', action: 'listTopics', timeout: '10000', wires: [[]] });
            ids.push('a1');
        }
        return nodes.concat(outcomeNodes(ids));
    }

    for (const mechanism of ['plain', 'scram-sha-256', 'scram-sha-512']) {
        it(`${mechanism}: produce and consume`, async function () {
            const topic = uniqueName(`sasl-${mechanism}`);
            await createTopic(topic, 1);
            await helper.load(NODES, flow(saslBroker(mechanism), topic, { consumer: {} }),
                { b1: { username: E2E.saslUser, password: E2E.saslPassword } });
            const out = collect('out');
            await nextStatus(helper.getNode('p1'), (s) => s.fill === 'green', 30000);
            const r = await expectOk('p1', { payload: `via ${mechanism}` });
            assert.equal(r.kafka.status, 'sent');
            await waitFor(() => out.length === 1, { timeout: 60000 });
            assert.equal(out[0].payload, `via ${mechanism}`);
            // the broker warns about PLAIN without TLS
            const warnings = calls(helper.getNode('b1'), 'warn');
            assert.equal(warnings.some((w) => /SASL PLAIN without TLS/.test(w)), mechanism === 'plain');
        });
    }

    it('wrong password: red "auth failed", slow retry, one error line, messages kept and sent after the fix', async function () {
        const topic = uniqueName('sasl-wrong');
        await createTopic(topic, 1);
        const credentials = { b1: { username: E2E.saslUser, password: 'definitely-wrong' } };
        await helper.load(NODES, flow(saslBroker('scram-sha-256'), topic, { consumer: {}, admin: true }), credentials);
        const p1 = helper.getNode('p1');
        const k1 = helper.getNode('k1');

        await nextStatus(p1, (s) => s.fill === 'red' && /^auth failed · retry 1s$/.test(s.text), 30000);
        await nextStatus(k1, (s) => s.fill === 'red' && /^auth failed · retry 1s$/.test(s.text), 30000);
        const queued = await expectOk('p1', { payload: 'kept while the password is wrong' });
        assert.equal(queued.kafka.status, 'queued');
        await nextStatus(p1, (s) => s.text === 'auth failed · 1 queued · retry 1s', 10000);

        const admin = await outcome('a1', {});
        assert.ok(admin.error);
        assert.match(admin.error.error.message, /SASL|authentication/i);
        await nextStatus(helper.getNode('a1'), (s) => s.fill === 'red' && s.text === 'auth failed');

        await sleep(2500); // a few slow retries
        assert.equal(calls(p1, 'error').filter((e) => /Kafka connection failed: .*SASL/.test(e)).length, 1, calls(p1, 'error').join(' / '));
        assert.equal(calls(k1, 'error').filter((e) => /Kafka connection failed: .*SASL/.test(e)).length, 1, calls(k1, 'error').join(' / '));

        // fix the password and redeploy: the queued message (on disk) is delivered
        await helper.unload();
        await helper.load(NODES, flow(saslBroker('scram-sha-256'), topic),
            { b1: { username: E2E.saslUser, password: E2E.saslPassword } });
        await waitFor(async () => (await readAll(topic)).length === 1, { timeout: 60000, interval: 500 });
        const stored = await readAll(topic);
        assert.equal(stored[0].value.toString(), 'kept while the password is wrong');
        await nextStatus(helper.getNode('p1'), (s) => s.fill === 'green' && s.text === 'connected', 30000);
    });

    it('a client without SASL on the SASL listener: messages are kept, retries without crashing', async function () {
        await helper.load(NODES, flow(saslBroker('none'), uniqueName('sasl-none'), { consumer: {} }));
        const p1 = helper.getNode('p1');
        // kafkajs' connect() only sends ApiVersions, which Kafka answers before authentication:
        // the producer looks connected until its first real request is refused
        await nextStatus(p1, (st) => st.text === 'connected', 30000);
        const r = await expectOk('p1', { payload: 'not lost' }, 60000);
        assert.equal(r.kafka.status, 'queued');
        const s = await nextStatus(p1, (st) => / · retry /.test(st.text), 60000);
        assert.ok(s.fill === 'yellow' || s.fill === 'red', JSON.stringify(s));
        await nextStatus(helper.getNode('k1'), (st) => / · retry /.test(st.text), 60000);
        const started = Date.now();
        await helper.unload();
        assert.ok(Date.now() - started < 11000);
    });

    it('a SASL client on the PLAINTEXT listener gets a clear error, not a crash', async function () {
        await helper.load(NODES, flow(brokerConfig({ authMechanism: 'plain' }), uniqueName('sasl-plaintext'), { consumer: {} }),
            { b1: { username: E2E.saslUser, password: E2E.saslPassword } });
        const s = await nextStatus(helper.getNode('p1'), (st) => / · retry /.test(st.text), 60000);
        assert.ok(s.fill === 'yellow' || s.fill === 'red', JSON.stringify(s));
        assert.ok(calls(helper.getNode('p1'), 'error').some((e) => /Kafka connection failed/.test(e)));
    });
});
