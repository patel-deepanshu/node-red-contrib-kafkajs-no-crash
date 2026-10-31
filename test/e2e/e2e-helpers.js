'use strict';

/**
 * Shared setup for the end-to-end tests against a REAL Kafka broker.
 *
 * The tests run the real nodes (inside node-red-node-test-helper) with the
 * real kafkajs, and check the results with a separate plain kafkajs client.
 * Every test file skips itself when the broker it needs is not configured:
 *
 *   KAFKA_E2E_BROKERS       PLAINTEXT bootstrap list, e.g. 127.0.0.1:19092.
 *                           Without it every e2e test is skipped.
 *   KAFKA_E2E_SASL_BROKERS  SASL_PLAINTEXT listener with PLAIN and SCRAM-SHA-256/512
 *                           enabled, e.g. 127.0.0.1:19093 (sasl.e2e.js)
 *   KAFKA_E2E_SASL_USER / KAFKA_E2E_SASL_PASSWORD
 *                           a user known to every mechanism (default alice / alice-secret,
 *                           as created by scripts/kafka/kafka-start.sh)
 *   KAFKA_E2E_CONTROL=1     the tests may stop, kill, freeze and restart the broker with
 *                           scripts/kafka/*.sh (outage.e2e.js). Needs KAFKA_HOME and a broker
 *                           started by `npm run kafka:start` (same KAFKA_BASE_DIR / ports).
 *
 * Local run (see scripts/kafka/):
 *   export KAFKA_HOME="$(bash scripts/kafka/kafka-download.sh | tail -n1)"
 *   npm run kafka:start
 *   KAFKA_E2E_BROKERS=127.0.0.1:19092 KAFKA_E2E_SASL_BROKERS=127.0.0.1:19093 KAFKA_E2E_CONTROL=1 npm run test:e2e
 *   npm run kafka:stop
 */

const assert = require('node:assert/strict');
const { execFile, execFileSync } = require('node:child_process');
const path = require('node:path');
const helper = require('node-red-node-test-helper');
const factory = require('../../lib/kafka-factory');
const { tmpDir, waitFor } = require('../helpers/common');

helper.init(require.resolve('node-red'));

const list = (v) => String(v || '').split(/[\s,]+/).filter(Boolean);

const E2E = Object.freeze({
    brokers: list(process.env.KAFKA_E2E_BROKERS),
    saslBrokers: list(process.env.KAFKA_E2E_SASL_BROKERS),
    saslUser: process.env.KAFKA_E2E_SASL_USER || 'alice',
    saslPassword: process.env.KAFKA_E2E_SASL_PASSWORD || 'alice-secret',
    control: process.env.KAFKA_E2E_CONTROL === '1' && !!process.env.KAFKA_HOME,
    kafkaHome: process.env.KAFKA_HOME || ''
});

/** Unique per run, so repeated runs against the same broker never see old data. */
const RUN = `${Date.now().toString(36)}${Math.floor(Math.random() * 1296).toString(36)}`;
let topicCounter = 0;

/** A fresh topic (or group) name for this run: e2e-<run>-<label>-<n>. */
function uniqueName(label) {
    topicCounter++;
    return `e2e-${RUN}-${label}-${topicCounter}`;
}

/**
 * Skip the current suite unless the broker(s) it needs are configured. Call
 * from a `before` hook: `before(function () { requireKafka(this, 'sasl'); })`.
 *
 * @param {Mocha.Context} ctx
 * @param {...('sasl'|'control')} needs
 */
function requireKafka(ctx, ...needs) {
    if (E2E.brokers.length === 0) ctx.skip();
    if (needs.includes('sasl') && E2E.saslBrokers.length === 0) ctx.skip();
    if (needs.includes('control') && !E2E.control) ctx.skip();
    factory.setKafkajs(null); // the real kafkajs (with the Snappy/LZ4 codecs registered)
}

/** The real kafkajs module, as the nodes see it (codecs registered). */
function kafkajs() {
    factory.setKafkajs(null);
    return factory.getKafkajs();
}

let client = null;

/** A plain kafkajs client for preparing and checking the tests (not the code under test). */
function testClient() {
    if (!client) {
        const { Kafka, logLevel } = kafkajs();
        client = new Kafka({
            clientId: 'kafkajs-nc-e2e-checker',
            brokers: E2E.brokers,
            logLevel: logLevel.NOTHING,
            retry: { retries: 8, initialRetryTime: 200 }
        });
    }
    return client;
}

async function withAdmin(fn) {
    const admin = testClient().admin();
    await admin.connect();
    try {
        return await fn(admin);
    } finally {
        await admin.disconnect().catch(() => {});
    }
}

/** Create a topic and wait for its leaders. */
function createTopic(topic, numPartitions = 1) {
    return withAdmin((admin) => admin.createTopics({
        topics: [{ topic, numPartitions, replicationFactor: 1 }],
        waitForLeaders: true
    }));
}

/** Produce with a plain kafkajs producer. */
async function produceRaw(topic, messages, { compression } = {}) {
    const producer = testClient().producer({ createPartitioner: kafkajs().Partitioners.DefaultPartitioner });
    await producer.connect();
    try {
        return await producer.send({ topic, messages, compression });
    } finally {
        await producer.disconnect().catch(() => {});
    }
}

/**
 * Read the whole topic (every partition, from the beginning up to the high
 * watermark at call time) with a plain kafkajs consumer in a throw-away group.
 *
 * @returns {Promise<Array<{partition: number, offset: string, key: (string|null), value: (Buffer|null), headers: object}>>}
 */
async function readAll(topic, { timeout = 60000 } = {}) {
    const offsets = await withAdmin((admin) => admin.fetchTopicOffsets(topic));
    const target = new Map(offsets.map((o) => [o.partition, BigInt(o.high)]));
    const total = offsets.reduce((n, o) => n + Number(BigInt(o.high) - BigInt(o.low)), 0);
    const out = [];
    if (total === 0) return out;
    const consumer = testClient().consumer({
        groupId: uniqueName('reader'),
        sessionTimeout: 10000,
        heartbeatInterval: 1000,
        maxWaitTimeInMs: 200
    });
    await consumer.connect();
    try {
        await consumer.subscribe({ topics: [topic], fromBeginning: true });
        const seen = new Map();
        await consumer.run({
            eachMessage: async ({ partition, message }) => {
                out.push({
                    partition,
                    offset: message.offset,
                    key: message.key === null ? null : message.key.toString(),
                    value: message.value === null ? null : Buffer.from(message.value),
                    headers: message.headers
                });
                seen.set(partition, BigInt(message.offset) + 1n);
            }
        });
        await waitFor(() => Array.from(target).every(([p, high]) => high === 0n || (seen.get(p) || 0n) >= high),
            { timeout, interval: 50, message: `reading ${total} messages of ${topic}` });
    } finally {
        await consumer.disconnect().catch(() => {});
    }
    // kafkajs delivers each partition in order; sort by partition then offset for a stable result
    return out.sort((a, b) => (a.partition - b.partition) || (BigInt(a.offset) < BigInt(b.offset) ? -1 : 1));
}

/** The members of a consumer group. */
async function groupMembers(groupId) {
    const { groups } = await withAdmin((admin) => admin.describeGroups([groupId]));
    return groups[0] ? groups[0].members : [];
}

// ---------------------------------------------------------------- broker control (KAFKA_E2E_CONTROL=1)

const SCRIPTS = path.join(__dirname, '..', '..', 'scripts', 'kafka');

/**
 * Run scripts/kafka/<script>; resolves with its output. Asynchronous on
 * purpose: the nodes under test must keep running while the broker restarts.
 */
function kafkaScript(script, ...args) {
    return new Promise((resolve, reject) => {
        execFile('bash', [path.join(SCRIPTS, script), ...args], { env: process.env, encoding: 'utf8', timeout: 180000 },
            (err, stdout, stderr) => {
                if (err) {
                    err.message += `\n${stdout}\n${stderr}`;
                    reject(err);
                } else {
                    resolve(stdout);
                }
            });
    });
}

/** Broker control for outage tests (KAFKA_E2E_CONTROL=1). Every method returns a promise. */
const broker = {
    /** SIGKILL (crash) or SIGTERM (controlled shutdown). */
    stop(kill = true) {
        return kafkaScript('kafka-stop.sh', ...(kill ? ['--kill'] : []));
    },
    /** Start again and wait until both listeners answer. */
    start() {
        return kafkaScript('kafka-start.sh');
    },
    /** Freeze the JVM (SIGSTOP): sockets stay open, nothing answers. */
    pause() {
        return kafkaScript('kafka-pause.sh', 'pause');
    },
    resume() {
        return kafkaScript('kafka-pause.sh', 'resume');
    },
    /** Make sure the broker runs and answers (after a test that may have left it stopped/paused). */
    async ensureUp() {
        await kafkaScript('kafka-pause.sh', 'resume').catch(() => {}); // not running: start() handles it
        return kafkaScript('kafka-start.sh');
    }
};

/**
 * The compression codecs of the record batches stored for partition 0 of
 * `topic` (e.g. ['lz4', 'lz4']), read from the local broker's log segment with
 * kafka-dump-log.sh. Only for the broker of `npm run kafka:start` (control mode).
 */
function storedCodecs(topic) {
    const base = process.env.KAFKA_BASE_DIR || path.join(__dirname, '..', '..', '.kafka-local');
    const segment = path.join(base, 'data', `${topic}-0`, '00000000000000000000.log');
    const out = execFileSync(path.join(E2E.kafkaHome, 'bin', 'kafka-dump-log.sh'), ['--files', segment], {
        encoding: 'utf8',
        timeout: 60000,
        env: { ...process.env, KAFKA_HEAP_OPTS: '-Xmx256m' },
        stdio: ['ignore', 'pipe', 'pipe']
    });
    return Array.from(out.matchAll(/compresscodec: (\w+)/g), (m) => m[1].toLowerCase());
}

// ---------------------------------------------------------------- Node-RED

/** mocha hooks: a fresh Node-RED test runtime (and userDir) for every test. */
function useNodeRed() {
    beforeEach(function (done) {
        helper.settings({ userDir: tmpDir('kafkajs-nc-e2e-') });
        helper.startServer(done);
    });
    afterEach(async function () {
        await helper.unload();
        await helper.stopServer();
    });
}

/** Broker config node for the PLAINTEXT listener, with short backoffs. */
function brokerConfig(extra = {}) {
    return {
        id: 'b1',
        type: 'kafkajs-nc-broker',
        name: 'e2e',
        brokers: E2E.brokers.join(','),
        clientId: 'kafkajs-nc-e2e',
        connectionTimeout: '3000',
        requestTimeout: '15000',
        retries: '3',
        reconnectMin: '300',
        reconnectMax: '2000',
        authRetryMin: '1000',
        logLevel: 'error',
        ...extra
    };
}

/** Catch + Complete nodes for `ids`, wired to the helper nodes 'caught' and 'completed'. */
function outcomeNodes(ids) {
    return [
        { id: 'e2e-catch', type: 'catch', scope: ids, uncaught: false, wires: [['caught']] },
        { id: 'caught', type: 'helper' },
        { id: 'e2e-complete', type: 'complete', scope: ids, uncaught: false, wires: [['completed']] },
        { id: 'completed', type: 'helper' }
    ];
}

/** Every message a helper node receives from now on. */
function collect(id) {
    const received = [];
    helper.getNode(id).on('input', (msg) => received.push(msg));
    return received;
}

let outcomeId = 0;

/**
 * Send `msg` to node `id` and wait until it completes (Complete node) or
 * fails (Catch node); needs outcomeNodes() in the flow.
 *
 * @returns {Promise<{ok?: object, error?: object}>}
 */
function outcome(id, msg, timeout = 30000) {
    outcomeId++;
    const tagged = Object.assign({}, msg, { _e2e: outcomeId });
    return new Promise((resolve, reject) => {
        const completed = helper.getNode('completed');
        const caught = helper.getNode('caught');
        const cleanup = () => {
            clearTimeout(timer);
            completed.removeListener('input', onOk);
            caught.removeListener('input', onError);
        };
        const onOk = (m) => {
            if (m._e2e !== tagged._e2e) return;
            cleanup();
            resolve({ ok: m });
        };
        const onError = (m) => {
            if (m._e2e !== tagged._e2e) return;
            cleanup();
            resolve({ error: m });
        };
        const timer = setTimeout(() => {
            cleanup();
            reject(new Error(`message to ${id} neither completed nor failed within ${timeout} ms`));
        }, timeout);
        completed.on('input', onOk);
        caught.on('input', onError);
        helper.getNode(id).receive(tagged);
    });
}

/** outcome() that must succeed; resolves with the completed msg. */
async function expectOk(id, msg, timeout) {
    const r = await outcome(id, msg, timeout);
    assert.ok(r.ok, `expected success, got: ${r.error && r.error.error && r.error.error.message}`);
    return r.ok;
}

/** Calls of a Node.prototype spy (error/warn/log) made by `node`, as strings. */
function calls(node, method) {
    const spy = node[method];
    if (!spy || !spy.args) return [];
    return spy.args.filter((_, i) => spy.thisValues[i] === node)
        .map((args) => String(args[0] && args[0].message ? args[0].message : args[0]));
}

module.exports = {
    E2E,
    RUN,
    helper,
    uniqueName,
    requireKafka,
    kafkajs,
    testClient,
    withAdmin,
    createTopic,
    produceRaw,
    readAll,
    groupMembers,
    broker,
    storedCodecs,
    useNodeRed,
    brokerConfig,
    outcomeNodes,
    collect,
    outcome,
    expectOk,
    calls
};
