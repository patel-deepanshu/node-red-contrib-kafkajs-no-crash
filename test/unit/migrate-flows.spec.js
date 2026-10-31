'use strict';

/**
 * scripts/migrate-flows.js: converting node-red-contrib-kafkajs flows
 * (kafkajs-client / kafkajs-producer / kafkajs-consumer) to this palette.
 *
 * - The script's copies of our editor defaults and ranges are compared with
 *   nodes/*.html and the runtime, so they cannot drift apart.
 * - Every mapping is checked on nodes shaped like the old editor saves them
 *   (upstream js/*.html defaults; numbers typed into a field become strings).
 * - The command line runs as a child process on temporary files.
 * - Finally a converted realistic flow is loaded into a Node-RED runtime with
 *   our nodes and the fake kafkajs: it must start without errors, connect,
 *   produce and consume.
 */

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const migrate = require('../../scripts/migrate-flows.js');
const clientConfig = require('../../lib/client-config');
const factory = require('../../lib/kafka-factory');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir, waitFor, sleep, statusCalls } = require('../helpers/common');

helper.init(require.resolve('node-red'));

const ROOT = path.join(__dirname, '..', '..');
const SCRIPT = path.join(ROOT, 'scripts', 'migrate-flows.js');
const CORE_DIR = path.dirname(require.resolve('@node-red/nodes/package.json', {
    paths: [path.dirname(require.resolve('node-red'))]
}));
const tlsNode = require(path.join(CORE_DIR, 'core', 'network', '05-tls.js'));
const functionNode = require(path.join(CORE_DIR, 'core', 'function', '10-function.js'));
const debugNode = require(path.join(CORE_DIR, 'core', 'common', '21-debug.js'));

/** Keys any node may have besides its defaults. */
const STANDARD_KEYS = new Set(['id', 'type', 'z', 'g', 'x', 'y', 'wires', 'd', 'l', 'info']);

// ---------------------------------------------------------------- editor definitions

/** registerType(type, definition) calls in an editor .html file, evaluated like the editor does. */
function definitionsFrom(file, defs = {}) {
    const html = fs.readFileSync(file, 'utf8');
    for (const m of html.matchAll(/<script type="text\/javascript">([\s\S]*?)<\/script>/g)) {
        const sandbox = {
            RED: { nodes: { registerType(type, def) { defs[type] = def; } }, _: (key) => key },
            // validators only look at open edit dialogs: there are none here
            $: () => ({ length: 0, val: () => undefined, is: () => false })
        };
        vm.runInNewContext(m[1], sandbox, { filename: path.relative(ROOT, file) });
    }
    return defs;
}

const DEFS = {};
for (const f of ['kafka-broker.html', 'kafka-producer.html', 'kafka-consumer.html']) {
    definitionsFrom(path.join(ROOT, 'nodes', f), DEFS);
}
definitionsFrom(path.join(CORE_DIR, 'core', 'network', '05-tls.html'), DEFS);

/** The editor default values of a type (JSON round trip: objects made in the vm have another realm's prototype). */
const defaultsOf = (type) => JSON.parse(JSON.stringify(
    Object.fromEntries(Object.entries(DEFS[type].defaults).map(([k, d]) => [k, d.value]))));

// ---------------------------------------------------------------- upstream nodes (as the old editor saves them)

/** kafkajs-client with the defaults of upstream js/kafka-client.html, after the dialog was saved once. */
function client(props = {}) {
    return {
        id: 'c1', type: 'kafkajs-client', name: 'Cluster', brokers: 'localhost:9092', clientid: 'node-red',
        connectiontimeout: '3000', requesttimeout: '25000', advancedretry: false, maxretrytime: '30000',
        initialretrytime: '300', factor: '0.2', multiplier: '2', retries: '5', auth: 'none', tlsselfsign: false,
        tlscacert: '', tlsclientcert: '', tlsprivatekey: '', tlspassphrase: '', saslssl: true, saslmechanism: 'plain',
        loglevel: 'error', ...props
    };
}

/** kafkajs-producer with the defaults of upstream js/kafka-producer.html (no output port). */
function producer(props = {}) {
    return {
        id: 'p1', type: 'kafkajs-producer', z: 'tab1', name: 'Producer', client: 'c1', topic: 'orders',
        advancedoptions: false, acknowledge: 'all', partition: null, headeritems: {}, key: null, responsetimeout: 30000,
        transactiontimeout: 60000, metadatamaxage: 300000, allowautotopiccreation: false, x: 300, y: 80, wires: [], ...props
    };
}

/** kafkajs-consumer with the defaults of upstream js/kafka-consumer.html. */
function consumer(props = {}) {
    return {
        id: 'k1', type: 'kafkajs-consumer', z: 'tab1', name: 'Consumer', client: 'c1', groupid: '', topic: 'orders',
        advancedoptions: false, autocommitinterval: 5000, autocommitthreshold: 100, sessiontimeout: 30000,
        rebalancetimeout: 60000, heartbeatinterval: 3000, metadatamaxage: 300000, maxbytesperpartition: 1048576,
        minbytes: 1, maxbytes: 10485760, maxwaittimeinms: 5000, frombeginning: false, clearoffsets: false,
        allowautotopiccreation: false, x: 100, y: 160, wires: [[]], ...props
    };
}

const TAB = { id: 'tab1', type: 'tab', label: 'Kafka' };

/** Deterministic ids for created tls-config nodes. */
function ids() {
    let n = 0;
    return () => `tls${++n}`;
}

function convert(nodes, options = {}) {
    return migrate.convertFlows(nodes, { idGenerator: ids(), ...options });
}

const byId = (flows, id) => flows.find((n) => n.id === id);
const notesFor = (report, id, level) => report.notes.filter((n) => n.id === id && (!level || n.level === level));
const noteText = (report, id, level) => notesFor(report, id, level).map((n) => n.text).join('\n');

/**
 * tls-config properties added after Node-RED 4 (Node-RED 5: client certificates from env variables or a PKCS#12
 * file). Their runtime treats a missing value as the default, so the converted flows leave them out and stay
 * loadable by every supported Node-RED version.
 */
const NEWER_TLS_CONFIG_KEYS = new Set(['certType', 'caEnv', 'certEnv', 'keyEnv', 'p12', 'p12name']);

/** The node has exactly the editor's properties (plus standard ones), all accepted by the editor validators. */
function assertEditorShape(node) {
    const def = DEFS[node.type];
    assert.ok(def, `no editor definition for ${node.type}`);
    for (const [key, d] of Object.entries(def.defaults)) {
        if (node.type === 'tls-config' && NEWER_TLS_CONFIG_KEYS.has(key) && !(key in node)) continue;
        assert.ok(Object.prototype.hasOwnProperty.call(node, key), `${node.type} ${node.id}: missing ${key}`);
        if (typeof d.validate === 'function') {
            const verdict = d.validate.call(node, node[key], { label: key });
            assert.equal(verdict, true, `${node.type} ${node.id}: ${key} = ${JSON.stringify(node[key])}: ${verdict}`);
        }
    }
    for (const key of Object.keys(node)) {
        assert.ok(STANDARD_KEYS.has(key) || Object.prototype.hasOwnProperty.call(def.defaults, key),
            `${node.type} ${node.id}: unknown property ${key}`);
    }
}

// ---------------------------------------------------------------- tests

describe('scripts/migrate-flows.js', function () {
    describe('stays in sync with the nodes', function () {
        it('has the editor defaults of the broker, producer and consumer (nodes/*.html)', function () {
            assert.deepEqual({ ...migrate.BROKER_DEFAULTS }, defaultsOf('kafkajs-nc-broker'));
            assert.deepEqual({ ...migrate.PRODUCER_DEFAULTS, headers: {} }, defaultsOf('kafkajs-nc-producer'));
            assert.deepEqual({ ...migrate.CONSUMER_DEFAULTS }, defaultsOf('kafkajs-nc-consumer'));
        });

        it("has the editor defaults of Node-RED's core tls-config node", function () {
            // The script writes the tls-config properties every supported Node-RED version knows (3.1 - 5.x).
            // Newer versions add optional properties that their runtime defaults when they are missing (Node-RED 5:
            // certType = "files", the env and p12 variants), exactly as for flows exported from older versions.
            const installed = defaultsOf('tls-config');
            for (const [key, value] of Object.entries(migrate.TLS_CONFIG_DEFAULTS)) {
                assert.deepEqual(value, installed[key], `tls-config default ${key}`);
            }
            const extra = Object.keys(installed).filter((k) => !(k in migrate.TLS_CONFIG_DEFAULTS));
            assert.deepEqual(extra.filter((k) => !NEWER_TLS_CONFIG_KEYS.has(k)), [],
                'a new tls-config property: check that older flows still work without it, then add it to NEWER_TLS_CONFIG_KEYS');
        });

        it('clamps to the ranges the runtime uses', function () {
            for (const key of ['connectionTimeout', 'requestTimeout', 'retries', 'initialRetryTime', 'maxRetryTime']) {
                assert.deepEqual(migrate.LIMITS[key], clientConfig.LIMITS[key], key);
            }
            const consumerLimits = consumerNode._internals.LIMITS;
            for (const key of ['autoCommitInterval', 'autoCommitThreshold', 'sessionTimeout', 'heartbeatInterval',
                'rebalanceTimeout', 'maxBytesPerPartition', 'maxBytes', 'maxWaitTimeInMs']) {
                assert.deepEqual(migrate.LIMITS[key], consumerLimits[key], key);
            }
            const { parseConfig } = producerNode._internals;
            assert.equal(parseConfig({ ackTimeout: migrate.LIMITS.ackTimeout.min - 1 }).ackTimeout, migrate.LIMITS.ackTimeout.min);
            assert.equal(parseConfig({ ackTimeout: migrate.LIMITS.ackTimeout.max + 1 }).ackTimeout, migrate.LIMITS.ackTimeout.max);
        });
    });

    describe('kafkajs-client → kafkajs-nc-broker', function () {
        it('maps a client without authentication (upstream defaults)', function () {
            const { flows, report } = convert([client({ z: 'tab1' })]);
            const b = byId(flows, 'c1');
            assertEditorShape(b);
            assert.deepEqual(b, {
                ...migrate.BROKER_DEFAULTS, id: 'c1', type: 'kafkajs-nc-broker', z: 'tab1', name: 'Cluster',
                brokers: 'localhost:9092', clientId: 'node-red', connectionTimeout: 3000, requestTimeout: 25000,
                logLevel: 'error'
            });
            assert.equal(flows.length, 1, 'no tls-config without TLS');
            assert.deepEqual(notesFor(report, 'c1'), []);
        });

        it('normalises the broker list and keeps ${ENV} references', function () {
            const conv = (brokers) => byId(convert([client({ brokers })]).flows, 'c1').brokers;
            assert.equal(conv('b1:9092,b2:9092'), 'b1:9092, b2:9092');
            assert.equal(conv(' b1:9092 , b2:9092,, b3:9093 '), 'b1:9092, b2:9092, b3:9093');
            assert.equal(conv(['b1:9092', 'b2:9092']), 'b1:9092, b2:9092');
            assert.equal(conv('${KAFKA_BROKERS}'), '${KAFKA_BROKERS}');
        });

        it('reports a client without brokers (the old editor default is an empty array)', function () {
            const { flows, report } = convert([client({ brokers: [] })]);
            assert.equal(byId(flows, 'c1').brokers, '');
            assert.match(noteText(report, 'c1', 'action'), /No brokers were configured/);
        });

        it('uses node-red as client id when the old one was empty', function () {
            assert.equal(byId(convert([client({ clientid: '' })]).flows, 'c1').clientId, 'node-red');
            assert.equal(byId(convert([client({ clientid: ' plant-7 ' })]).flows, 'c1').clientId, 'plant-7');
        });

        it('turns string numbers into numbers, clamps them and keeps ${ENV} references', function () {
            const { flows, report } = convert([client({ connectiontimeout: '50', requesttimeout: 'abc' })]);
            const b = byId(flows, 'c1');
            assert.equal(b.connectionTimeout, 100);
            assert.equal(b.requestTimeout, 25000);
            assert.match(noteText(report, 'c1', 'info'), /Connection timeout 50 is outside 100 – 300000: using 100/);
            assert.match(noteText(report, 'c1', 'info'), /Request timeout "abc" is not a number: using 25000/);
            const env = byId(convert([client({ connectiontimeout: '${CONNECT_MS}' })]).flows, 'c1');
            assert.equal(env.connectionTimeout, '${CONNECT_MS}');
        });

        it('only takes the retry settings when "Advanced retry" was ticked (the old node ignored them otherwise)', function () {
            const off = byId(convert([client({ retries: '9', initialretrytime: '500', maxretrytime: '9000' })]).flows, 'c1');
            assert.deepEqual([off.retries, off.initialRetryTime, off.maxRetryTime], [5, 300, 30000]);
            const { flows, report } = convert([client({ advancedretry: true, retries: '9', initialretrytime: '500',
                maxretrytime: '9000', factor: '0.5' })]);
            const on = byId(flows, 'c1');
            assert.deepEqual([on.retries, on.initialRetryTime, on.maxRetryTime], [9, 500, 9000]);
            assert.match(noteText(report, 'c1', 'info'), /factor 0\.5 \/ multiplier 2 are not configurable/);
        });

        it('maps the log level (nothing → off) and falls back to warn', function () {
            const level = (loglevel) => byId(convert([client({ loglevel })]).flows, 'c1').logLevel;
            assert.deepEqual(['error', 'warn', 'info', 'debug', 'nothing'].map(level), ['error', 'warn', 'info', 'debug', 'off']);
            assert.equal(level(undefined), 'warn');
            assert.equal(level('loud'), 'warn');
        });

        for (const mechanism of ['plain', 'scram-sha-256', 'scram-sha-512']) {
            it(`maps SASL ${mechanism} with "Use SSL" to the mechanism plus Use TLS (no tls-config)`, function () {
                const { flows, report } = convert([client({ auth: 'sasl', saslmechanism: mechanism, saslssl: true })]);
                const b = byId(flows, 'c1');
                assertEditorShape(b);
                assert.equal(b.authMechanism, mechanism);
                assert.equal(b.tls, true);
                assert.equal(b.tlsConfig, '');
                assert.equal(flows.length, 1);
                assert.match(noteText(report, 'c1', 'action'), /Enter the SASL username and password again/);
            });
        }

        it('maps "Use SSL" off (or missing, which the old runtime treated as off) to TLS off', function () {
            const { flows, report } = convert([client({ auth: 'sasl', saslmechanism: 'plain', saslssl: false })]);
            assert.equal(byId(flows, 'c1').tls, false);
            assert.match(noteText(report, 'c1', 'info'), /PLAIN without TLS sends the password in clear text/);
            const missing = client({ auth: 'sasl', saslmechanism: 'scram-sha-256' });
            delete missing.saslssl;
            assert.equal(byId(convert([missing]).flows, 'c1').tls, false);
        });

        it('uses PLAIN when no mechanism was saved, and reports an unknown one', function () {
            assert.equal(byId(convert([client({ auth: 'sasl', saslmechanism: '' })]).flows, 'c1').authMechanism, 'plain');
            const { flows, report } = convert([client({ auth: 'sasl', saslmechanism: 'oauthbearer' })]);
            assert.equal(byId(flows, 'c1').authMechanism, 'plain');
            assert.match(noteText(report, 'c1', 'action'), /Unknown SASL mechanism "oauthbearer"/);
        });

        it('creates a tls-config node from the TLS file paths, with the intended meaning of "Self Sign"', function () {
            const { flows, report } = convert([client({
                z: 'tab1', g: 'grp1', auth: 'tls', tlscacert: ' /certs/ca.pem ', tlsclientcert: '/certs/client.pem',
                tlsprivatekey: '/certs/client.key', tlspassphrase: 'secret', tlsselfsign: false
            })]);
            const b = byId(flows, 'c1');
            assert.equal(b.tls, true);
            assert.equal(b.authMechanism, 'none');
            assert.equal(b.tlsConfig, 'tls1');
            assert.deepEqual(flows.map((n) => n.id), ['c1', 'tls1'], 'the tls-config node follows its broker');
            const t = byId(flows, 'tls1');
            assertEditorShape(t);
            assert.deepEqual(t, {
                ...migrate.TLS_CONFIG_DEFAULTS, id: 'tls1', type: 'tls-config', z: 'tab1', name: 'Cluster TLS',
                ca: '/certs/ca.pem', cert: '/certs/client.pem', key: '/certs/client.key', verifyservercert: true
            });
            assert.ok(!JSON.stringify(flows).includes('secret'), 'the passphrase must not stay in the flows file');
            assert.match(noteText(report, 'c1', 'check'), /verifies the broker certificate/);
            assert.match(noteText(report, 'tls1', 'action'), /Enter the private key passphrase again/);
            assert.match(noteText(report, 'tls1', 'check'), /\/certs\/ca\.pem, \/certs\/client\.pem, \/certs\/client\.key/);

            const selfSigned = convert([client({ auth: 'tls', tlscacert: '/ca.pem', tlsselfsign: true })]);
            assert.equal(byId(selfSigned.flows, 'tls1').verifyservercert, false);
            assert.match(noteText(selfSigned.report, 'c1', 'check'), /does NOT verify the broker certificate/);
        });

        it('reports TLS with only a certificate or only a key, and TLS without any file', function () {
            const half = convert([client({ auth: 'tls', tlsclientcert: '/c.pem' })]);
            assert.match(noteText(half.report, 'tls1', 'action'), /Only one of client certificate and private key/);
            const none = convert([client({ auth: 'tls' })]);
            assert.equal(none.flows.length, 1, 'no tls-config without files');
            assert.equal(byId(none.flows, 'c1').tls, true);
            assert.equal(byId(none.flows, 'c1').tlsConfig, '');
            assert.match(noteText(none.report, 'c1', 'action'), /TLS was selected without certificate files/);
        });

        it('gives a created tls-config node an id that is not used yet', function () {
            const taken = convert([client({ auth: 'tls', tlscacert: '/ca.pem' }), { id: 'tls1', type: 'comment' }]);
            const created = taken.flows.find((n) => n.type === 'tls-config');
            assert.notEqual(created.id, 'tls1');
            assert.equal(byId(taken.flows, 'c1').tlsConfig, created.id);
            const real = migrate.convertFlows([client({ auth: 'tls', tlscacert: '/ca.pem' })]);
            assert.match(real.flows[1].id, /^[0-9a-f]{16}$/);
        });

        it('reports an unknown authentication', function () {
            assert.match(noteText(convert([client({ auth: 'kerberos' })]).report, 'c1', 'action'), /Unknown authentication/);
        });
    });

    describe('kafkajs-producer → kafkajs-nc-producer', function () {
        it('maps a producer with the upstream defaults', function () {
            const { flows, report } = convert([client(), producer()]);
            const p = byId(flows, 'p1');
            assertEditorShape(p);
            assert.deepEqual(p, {
                ...migrate.PRODUCER_DEFAULTS, headers: {}, id: 'p1', type: 'kafkajs-nc-producer', z: 'tab1',
                name: 'Producer', broker: 'c1', topic: 'orders', partitioner: 'legacy', x: 300, y: 80, wires: [[]]
            });
            assert.match(noteText(report, 'p1', 'info'), /the old node ignored this setting/);
            assert.equal(notesFor(report, 'p1', 'action').length, 0);
        });

        it('maps the acknowledge setting', function () {
            const acks = (acknowledge) => byId(convert([client(), producer({ acknowledge })]).flows, 'p1').acks;
            assert.deepEqual(['all', 'leader', 'none'].map(acks), ['all', 'leader', 'none']);
            assert.equal(acks(undefined), 'all');
            assert.equal(acks('ALL'), 'all');
            assert.match(noteText(convert([client(), producer({ acknowledge: 'none' })]).report, 'p1', 'info'),
                /fire-and-forget/);
            assert.match(noteText(convert([client(), producer({ acknowledge: 'two' })]).report, 'p1', 'info'),
                /Unknown acknowledge "two": using "all"/);
        });

        it('maps the partition (empty, numbers, numeric strings, ${ENV}) and reports anything else', function () {
            const part = (partition) => byId(convert([client(), producer({ partition })]).flows, 'p1').partition;
            assert.equal(part(null), '');
            assert.equal(part(''), '');
            assert.equal(part('0'), '0');
            assert.equal(part(3), '3');
            assert.equal(part(' 7 '), '7');
            assert.equal(part('${PARTITION}'), '${PARTITION}');
            const { flows, report } = convert([client(), producer({ partition: '-1' })]);
            assert.equal(byId(flows, 'p1').partition, '');
            assert.match(noteText(report, 'p1', 'action'), /Partition "-1" is not a whole number/);
        });

        it('maps the key (null → empty: use msg.key)', function () {
            assert.equal(byId(convert([client(), producer({ key: null })]).flows, 'p1').key, '');
            assert.equal(byId(convert([client(), producer({ key: 'device-1' })]).flows, 'p1').key, 'device-1');
        });

        it('maps static headers and keeps msg.headers ignored when there are any (as the old node did)', function () {
            const { flows, report } = convert([client(), producer({ headeritems: { source: 'node-red', '': '', n: 5 } })]);
            const p = byId(flows, 'p1');
            assert.deepEqual(p.headers, { source: 'node-red', n: '5' });
            assert.equal(p.useMsgHeaders, false);
            assert.match(noteText(report, 'p1', 'info'), /"Also send msg\.headers" is off, as before/);
            const none = byId(convert([client(), producer({ headeritems: {} })]).flows, 'p1');
            assert.equal(none.useMsgHeaders, true);
            const missing = producer();
            delete missing.headeritems;
            assert.deepEqual(byId(convert([client(), missing]).flows, 'p1').headers, {});
        });

        it('maps the response timeout to the ack timeout', function () {
            assert.equal(byId(convert([client(), producer({ responsetimeout: '45000' })]).flows, 'p1').ackTimeout, 45000);
            assert.equal(byId(convert([client(), producer({ responsetimeout: '' })]).flows, 'p1').ackTimeout, 30000);
            assert.equal(byId(convert([client(), producer({ responsetimeout: '10' })]).flows, 'p1').ackTimeout, 100);
        });

        it('keeps the old output-less wiring valid (one empty output) and drops unused upstream fields', function () {
            const p = byId(convert([client(), producer({ advancedoptions: true, transactiontimeout: '1' })]).flows, 'p1');
            assert.deepEqual(p.wires, [[]]);
            for (const old of ['client', 'advancedoptions', 'acknowledge', 'headeritems', 'responsetimeout',
                'transactiontimeout', 'metadatamaxage', 'allowautotopiccreation']) {
                assert.ok(!(old in p), `${old} should be dropped`);
            }
        });

        it('reports topics that the new node would reject, and missing or foreign clients', function () {
            assert.match(noteText(convert([client(), producer({ topic: 'sensors/temp' })]).report, 'p1', 'action'),
                /"sensors\/temp" is not a valid Kafka topic name/);
            assert.equal(notesFor(convert([client(), producer({ topic: '${TOPIC}' })]).report, 'p1', 'action').length, 0);
            assert.match(noteText(convert([producer({ client: '' })]).report, 'p1', 'action'), /No kafkajs-client was selected/);
            assert.match(noteText(convert([producer({ client: 'elsewhere' })]).report, 'p1', 'action'), /not in this file/);
            assert.match(noteText(convert([{ id: 'c1', type: 'mqtt-broker' }, producer()]).report, 'p1', 'action'),
                /is a "mqtt-broker" node/);
        });

        it('keeps position, group, disabled flag and description', function () {
            const p = byId(convert([client(), producer({ g: 'grp', d: true, info: 'docs', l: false })]).flows, 'p1');
            assert.deepEqual([p.g, p.d, p.info, p.l, p.x, p.y], ['grp', true, 'docs', false, 300, 80]);
        });
    });

    describe('kafkajs-consumer → kafkajs-nc-consumer', function () {
        it('uses the new defaults when "Advanced options" was off (the old node ignored those fields)', function () {
            const { flows } = convert([client(), consumer({ frombeginning: true, sessiontimeout: '10000', clearoffsets: true })]);
            const k = byId(flows, 'k1');
            assertEditorShape(k);
            assert.deepEqual(k, {
                ...migrate.CONSUMER_DEFAULTS, id: 'k1', type: 'kafkajs-nc-consumer', z: 'tab1', name: 'Consumer',
                broker: 'c1', topics: 'orders', x: 100, y: 160, wires: [[]]
            });
        });

        it('maps the advanced options, turning strings into numbers', function () {
            const { flows, report } = convert([client(), consumer({
                advancedoptions: true, frombeginning: true, autocommitinterval: '2000', autocommitthreshold: '50',
                sessiontimeout: '20000', rebalancetimeout: '40000', heartbeatinterval: '2000', maxbytesperpartition: '2048',
                maxbytes: '4096', maxwaittimeinms: '500', allowautotopiccreation: true
            })]);
            const k = byId(flows, 'k1');
            assertEditorShape(k);
            assert.deepEqual(
                [k.fromBeginning, k.autoCommitInterval, k.autoCommitThreshold, k.sessionTimeout, k.rebalanceTimeout,
                    k.heartbeatInterval, k.maxBytesPerPartition, k.maxBytes, k.maxWaitTimeInMs, k.allowAutoTopicCreation],
                [true, 2000, 50, 20000, 40000, 2000, 2048, 4096, 500, true]);
            assert.equal(notesFor(report, 'k1', 'info').filter((n) => !/Group ID was empty/.test(n.text)).length, 0);
        });

        it('keeps the upstream values of fields left at their defaults (5 s max wait, threshold 100)', function () {
            const k = byId(convert([client(), consumer({ advancedoptions: true })]).flows, 'k1');
            assert.equal(k.maxWaitTimeInMs, 5000);
            assert.equal(k.autoCommitThreshold, 100);
        });

        it('reports a heartbeat interval that is not lower than the session timeout', function () {
            const { report } = convert([client(), consumer({ advancedoptions: true, sessiontimeout: '3000', heartbeatinterval: '5000' })]);
            assert.match(noteText(report, 'k1', 'info'), /Heartbeat interval 5000 ms is not lower than the session timeout 3000 ms/);
        });

        it('reports "Clear offsets" and changed metadata max age / min bytes', function () {
            const { report } = convert([client(), consumer({ advancedoptions: true, clearoffsets: true, minbytes: '10', metadatamaxage: 300000 })]);
            assert.match(noteText(report, 'k1', 'action'), /"Clear offsets" .* does not exist any more/);
            assert.match(noteText(report, 'k1', 'info'), /Min bytes is not configurable/);
            assert.doesNotMatch(noteText(report, 'k1', 'info'), /Metadata max age/);
        });

        it('keeps an empty group id empty (stable default node-red-<id>) and says that old offsets do not carry over', function () {
            const empty = convert([client(), consumer({ groupid: '' })]);
            assert.equal(byId(empty.flows, 'k1').groupId, '');
            assert.match(noteText(empty.report, 'k1', 'info'), /"node-red-k1".*Offsets of the old random groups do not carry over/s);
            const named = convert([client(), consumer({ groupid: ' billing ' })]);
            assert.equal(byId(named.flows, 'k1').groupId, 'billing');
            assert.doesNotMatch(noteText(named.report, 'k1'), /Group ID/);
        });

        it('maps the topic and reports a missing or invalid one', function () {
            assert.match(noteText(convert([client(), consumer({ topic: '' })]).report, 'k1', 'action'), /No topic was configured/);
            assert.match(noteText(convert([client(), consumer({ topic: 'a/b' })]).report, 'k1', 'action'), /not a valid Kafka topic/);
            assert.equal(byId(convert([client(), consumer({ topic: ' telemetry ' })]).flows, 'k1').topics, 'telemetry');
        });

        it('keeps the wires and finds nodes after the consumer that read the old output shape', function () {
            const flows = [
                client(),
                consumer({ wires: [['fn1', 'sw1']] }),
                { id: 'fn1', type: 'function', z: 'tab1', func: 'msg.payload = msg.payload.value;\nreturn msg;', wires: [['lo1']] },
                { id: 'sw1', type: 'switch', z: 'tab1', property: 'payload.headers.type', propertyType: 'msg', rules: [], wires: [[]] },
                { id: 'lo1', type: 'link out', z: 'tab1', links: ['li1'], wires: [] },
                { id: 'li1', type: 'link in', z: 'tab1', links: ['lo1'], wires: [['ch1']] },
                { id: 'ch1', type: 'change', z: 'tab1', rules: [{ t: 'set', p: 'key', pt: 'msg', to: 'payload["key"]', tot: 'msg' }], wires: [[]] },
                { id: 'other', type: 'function', z: 'tab1', func: 'return msg.payload.value;', wires: [[]] }
            ];
            const { flows: out, report } = convert(flows);
            assert.deepEqual(byId(out, 'k1').wires, [['fn1', 'sw1']]);
            const text = noteText(report, 'k1', 'action');
            assert.match(text, /function \(fn1\) uses msg\.payload\.value/);
            assert.match(text, /switch \(sw1\) uses msg\.payload\.headers/);
            assert.match(text, /change \(ch1\) uses msg\.payload\.key/);
            assert.doesNotMatch(text, /other/, 'not connected to the consumer');
            const plain = convert([client(), consumer({ wires: [['dbg']] }), { id: 'dbg', type: 'debug', z: 'tab1', complete: 'payload', wires: [] }]);
            assert.equal(notesFor(plain.report, 'k1', 'action').length, 0);
            assert.match(noteText(plain.report, 'k1', 'check'), /msg\.payload is now the record value/);
        });

        it('follows messages into subflows', function () {
            const flows = [
                client(),
                consumer({ wires: [['inst']] }),
                { id: 'sf', type: 'subflow', name: 'decode', in: [{ x: 0, y: 0, wires: [{ id: 'sfn' }] }], out: [] },
                { id: 'sfn', type: 'function', z: 'sf', func: 'return { payload: msg.payload.value };', wires: [] },
                { id: 'inst', type: 'subflow:sf', z: 'tab1', wires: [] }
            ];
            assert.match(noteText(convert(flows).report, 'k1', 'action'), /function \(sfn\) uses msg\.payload\.value/);
        });
    });

    describe('the whole file', function () {
        const realistic = () => [
            TAB,
            client({ id: 'c1' }),
            client({ id: 'c2', name: 'Cloud', auth: 'sasl', saslmechanism: 'scram-sha-256' }),
            producer({ id: 'p1', client: 'c1' }),
            consumer({ id: 'k1', client: 'c2', wires: [['dbg']] }),
            { id: 'dbg', type: 'debug', z: 'tab1', name: '', active: true, complete: 'payload', x: 400, y: 160, wires: [] }
        ];

        it('converts only the old Kafka nodes, keeps everything else and the order, and does not touch the input', function () {
            const input = realistic();
            const before = structuredClone(input);
            const { flows, report } = convert(input);
            assert.deepEqual(input, before, 'input modified');
            assert.deepEqual(flows.map((n) => n.type),
                ['tab', 'kafkajs-nc-broker', 'kafkajs-nc-broker', 'kafkajs-nc-producer', 'kafkajs-nc-consumer', 'debug']);
            assert.equal(flows[0], input[0]);
            assert.equal(flows[5], input[5]);
            assert.equal(report.migrated, 4);
            assert.deepEqual(report.counts, { brokers: 2, producers: 1, consumers: 1, tlsConfigs: 0 });
            for (const n of flows.filter((x) => DEFS[x.type])) assertEditorShape(n);
        });

        it('accepts the admin API format { rev, flows }', function () {
            const { flows } = convert({ rev: 'abc', flows: realistic() });
            assert.equal(flows.rev, 'abc');
            assert.equal(flows.flows.length, 6);
            assert.equal(byId(flows.flows, 'p1').type, 'kafkajs-nc-producer');
        });

        it('rejects input that is not a flows file', function () {
            assert.throws(() => migrate.convertFlows({ nodes: [] }), TypeError);
            assert.throws(() => migrate.convertFlows('flows'), TypeError);
        });

        it('is idempotent: a converted file has nothing left to migrate', function () {
            const once = convert(realistic()).flows;
            const twice = convert(once);
            assert.equal(twice.report.migrated, 0);
            assert.deepEqual(twice.flows, once);
            assert.match(migrate.formatReport(twice.report), /^Nothing to migrate/);
        });

        it('formats a report with one section per level', function () {
            const { report } = convert(realistic());
            const text = migrate.formatReport(report, { output: '/x/flows.migrated.json' });
            assert.match(text, /^Converted 2 kafkajs-clients → kafkajs-nc-broker, 1 kafkajs-producer → kafkajs-nc-producer, 1 kafkajs-consumer/);
            assert.match(text, /Converted flows written to: +\/x\/flows\.migrated\.json/);
            assert.match(text, /Needs your attention \(\d+\):\n {2}- kafkajs-nc-broker "Cloud" \(c2\): Enter the SASL username/);
            assert.match(text, /Check after deploying/);
            assert.match(text, /Good to know/);
            assert.match(text, /Credentials: not supplied/);
            assert.match(text, /Partitioner "Legacy"/);
        });
    });

    describe('credentials', function () {
        it('renames the SASL credentials and moves the TLS passphrase into the tls-config node', function () {
            const creds = {
                c1: { saslusername: 'alice', saslpassword: 'alice-secret' },
                c2: { saslusername: 'ignored-user' },
                other: { user: 'x', password: 'y' }
            };
            const before = structuredClone(creds);
            const result = convert([
                client({ id: 'c1', auth: 'sasl' }),
                client({ id: 'c2', auth: 'tls', tlscacert: '/ca.pem', tlspassphrase: 'key-pass' })
            ], { credentials: creds });
            assert.deepEqual(creds, before, 'input modified');
            assert.equal(result.credentialsStatus, 'converted');
            assert.deepEqual(result.credentials, {
                c1: { username: 'alice', password: 'alice-secret' },
                c2: { username: 'ignored-user' },
                other: { user: 'x', password: 'y' },
                tls1: { passphrase: 'key-pass' }
            });
            assert.equal(notesFor(result.report, 'c1', 'action').length, 0, 'credentials were converted');
            assert.equal(notesFor(result.report, 'tls1', 'action').length, 0, 'passphrase was converted');
        });

        it('still asks for credentials that the file does not contain', function () {
            const result = convert([client({ auth: 'sasl' })], { credentials: { c1: { saslusername: 'alice' } } });
            assert.match(noteText(result.report, 'c1', 'action'), /Enter the SASL username and password again/);
        });

        it('detects an encrypted credentials file and leaves it alone', function () {
            const result = convert([client({ auth: 'sasl' })], { credentials: { $: '0123abcd' } });
            assert.equal(result.credentialsStatus, 'encrypted');
            assert.equal(result.credentials, null);
            assert.ok(result.report.notes.some((n) => n.level === 'action' && /credentials file is encrypted/.test(n.text)));
            assert.match(migrate.formatReport(result.report), /the file is encrypted and was not changed/);
        });

        it('convertCredentials() handles missing, invalid and empty entries', function () {
            assert.deepEqual(migrate.convertCredentials(null, []), { status: 'none', credentials: null });
            assert.deepEqual(migrate.convertCredentials([], []), { status: 'invalid', credentials: null });
            assert.deepEqual(migrate.convertCredentials({ b: {} }, [{ brokerId: 'b', tlsId: null, passphrase: '' }]),
                { status: 'converted', credentials: {} });
        });

        it('leaves credentials alone when the file has no kafkajs-client node', function () {
            const result = convert([producer({ client: 'elsewhere' })], { credentials: { x: { password: 'p' } } });
            assert.equal(result.credentialsStatus, 'unchanged');
            assert.equal(result.credentials, null);
        });
    });

    describe('command line', function () {
        function run(args, cwd) {
            const r = spawnSync(process.execPath, [SCRIPT, ...args], { cwd, encoding: 'utf8' });
            return { code: r.status, stdout: r.stdout, stderr: r.stderr };
        }

        function setup(flows, creds, pretty = true) {
            const dir = tmpDir('kafkajs-nc-migrate-');
            const flowsFile = path.join(dir, 'flows.json');
            fs.writeFileSync(flowsFile, JSON.stringify(flows, null, pretty ? 4 : undefined));
            let credFile = null;
            if (creds) {
                credFile = path.join(dir, 'flows_cred.json');
                fs.writeFileSync(credFile, JSON.stringify(creds, null, 4));
            }
            return { dir, flowsFile, credFile };
        }

        const FLOWS = [TAB, client({ auth: 'sasl', saslmechanism: 'scram-sha-512' }), producer(), consumer()];

        it('parseArgs() accepts the documented options and rejects the rest', function () {
            assert.deepEqual(migrate.parseArgs(['f.json']), { help: false, inPlace: false, input: 'f.json' });
            assert.deepEqual(migrate.parseArgs(['f.json', '--out', 'o.json', '--credentials', 'c.json']),
                { help: false, inPlace: false, input: 'f.json', out: 'o.json', credentials: 'c.json' });
            assert.deepEqual(migrate.parseArgs(['--in-place', '--credentials=c.json', 'f.json']),
                { help: false, inPlace: true, input: 'f.json', credentials: 'c.json' });
            assert.equal(migrate.parseArgs(['--help']).help, true);
            for (const bad of [[], ['--bogus', 'f.json'], ['a.json', 'b.json'], ['f.json', '--out'],
                ['f.json', '--out', 'o.json', '--in-place'], ['f.json', '--credentials', '--in-place']]) {
                assert.throws(() => migrate.parseArgs(bad), migrate.UsageError, JSON.stringify(bad));
            }
            assert.equal(migrate.credentialsFileFor('/a/flows.migrated.json'), path.join('/a', 'flows.migrated_cred.json'));
            assert.equal(migrate.defaultOutput('/a/flows_host.json'), path.join('/a', 'flows_host.migrated.json'));
        });

        it('writes <flows>.migrated.json, prints the report and never changes the input', function () {
            const { dir, flowsFile } = setup(FLOWS);
            const original = fs.readFileSync(flowsFile, 'utf8');
            const r = run(['flows.json'], dir);
            assert.equal(r.code, 0, r.stderr);
            assert.match(r.stdout, /Converted 1 kafkajs-client → kafkajs-nc-broker/);
            assert.match(r.stdout, /Needs your attention/);
            assert.equal(fs.readFileSync(flowsFile, 'utf8'), original);
            const outText = fs.readFileSync(path.join(dir, 'flows.migrated.json'), 'utf8');
            assert.ok(outText.includes('\n    {'), 'pretty input stays pretty (4 spaces)');
            const out = JSON.parse(outText);
            assert.deepEqual(out.map((n) => n.type), ['tab', 'kafkajs-nc-broker', 'kafkajs-nc-producer', 'kafkajs-nc-consumer']);
            assert.ok(!fs.existsSync(path.join(dir, 'flows.migrated_cred.json')), 'no credentials without --credentials');
        });

        it('keeps a compact flows file compact and honours --out', function () {
            const { dir } = setup(FLOWS, null, false);
            const r = run(['flows.json', '--out', path.join(dir, 'sub-out.json')], dir);
            assert.equal(r.code, 0, r.stderr);
            const text = fs.readFileSync(path.join(dir, 'sub-out.json'), 'utf8');
            assert.ok(!text.includes('\n'), 'compact');
            assert.match(r.stdout, /sub-out\.json/);
        });

        it('converts an unencrypted credentials file into <out>_cred.json (owner-only permissions)', function () {
            const { dir, credFile } = setup(FLOWS, { c1: { saslusername: 'alice', saslpassword: 'alice-secret' } });
            const r = run(['flows.json', '--credentials', 'flows_cred.json', '--out', 'new.json'], dir);
            assert.equal(r.code, 0, r.stderr);
            const creds = JSON.parse(fs.readFileSync(path.join(dir, 'new_cred.json'), 'utf8'));
            assert.deepEqual(creds, { c1: { username: 'alice', password: 'alice-secret' } });
            assert.deepEqual(JSON.parse(fs.readFileSync(credFile, 'utf8')), { c1: { saslusername: 'alice', saslpassword: 'alice-secret' } });
            if (process.platform !== 'win32') {
                assert.equal(fs.statSync(path.join(dir, 'new_cred.json')).mode & 0o777, 0o600);
            }
            assert.doesNotMatch(r.stdout, /Enter the SASL username/);
            assert.doesNotMatch(r.stdout, /alice-secret/, 'never print secrets');
        });

        it('--in-place keeps .bak copies, replaces the files, and a second run changes nothing', function () {
            const creds = { c1: { saslusername: 'alice', saslpassword: 'alice-secret' } };
            const { dir, flowsFile, credFile } = setup(FLOWS, creds);
            const original = fs.readFileSync(flowsFile, 'utf8');
            const originalCreds = fs.readFileSync(credFile, 'utf8');
            const r = run(['flows.json', '--in-place', '--credentials', 'flows_cred.json'], dir);
            assert.equal(r.code, 0, r.stderr);
            assert.equal(fs.readFileSync(`${flowsFile}.bak`, 'utf8'), original);
            assert.equal(fs.readFileSync(`${credFile}.bak`, 'utf8'), originalCreds);
            assert.equal(JSON.parse(fs.readFileSync(flowsFile, 'utf8'))[1].type, 'kafkajs-nc-broker');
            assert.deepEqual(JSON.parse(fs.readFileSync(credFile, 'utf8')), { c1: { username: 'alice', password: 'alice-secret' } });
            assert.match(r.stdout, /Backup of the original flows: +.*flows\.json\.bak/);

            const again = run(['flows.json', '--in-place', '--credentials', 'flows_cred.json'], dir);
            assert.equal(again.code, 0, again.stderr);
            assert.match(again.stdout, /^Nothing to migrate/);
            assert.ok(!fs.existsSync(`${flowsFile}.bak.1`), 'no second backup when nothing changed');
            assert.equal(fs.readFileSync(`${flowsFile}.bak`, 'utf8'), original, 'the first backup is kept');
        });

        it('--in-place picks a free backup name when flows.json.bak exists', function () {
            const { dir, flowsFile } = setup(FLOWS);
            fs.writeFileSync(`${flowsFile}.bak`, 'older backup');
            const r = run(['flows.json', '--in-place'], dir);
            assert.equal(r.code, 0, r.stderr);
            assert.equal(fs.readFileSync(`${flowsFile}.bak`, 'utf8'), 'older backup');
            assert.ok(fs.existsSync(`${flowsFile}.bak.1`));
        });

        it('leaves an encrypted credentials file alone and says so', function () {
            const { dir, credFile } = setup(FLOWS, { $: 'a1b2c3' });
            const r = run(['flows.json', '--credentials', 'flows_cred.json'], dir);
            assert.equal(r.code, 0, r.stderr);
            assert.match(r.stdout, /encrypted/);
            assert.ok(!fs.existsSync(path.join(dir, 'flows.migrated_cred.json')));
            assert.deepEqual(JSON.parse(fs.readFileSync(credFile, 'utf8')), { $: 'a1b2c3' });
        });

        it('writes nothing when there is nothing to migrate', function () {
            const { dir } = setup([TAB, { id: 'dbg', type: 'debug', z: 'tab1', wires: [] }]);
            const r = run(['flows.json'], dir);
            assert.equal(r.code, 0, r.stderr);
            assert.match(r.stdout, /^Nothing to migrate/);
            assert.deepEqual(fs.readdirSync(dir).sort(), ['flows.json']);
        });

        it('refuses to overwrite the input without --in-place', function () {
            const { dir, flowsFile } = setup(FLOWS);
            const original = fs.readFileSync(flowsFile, 'utf8');
            const r = run(['flows.json', '--out', './flows.json'], dir);
            assert.equal(r.code, 1);
            assert.match(r.stderr, /would overwrite the input file/);
            assert.equal(fs.readFileSync(flowsFile, 'utf8'), original);
        });

        it('exits 1 for files it cannot use and 2 for wrong usage', function () {
            const { dir } = setup(FLOWS);
            fs.writeFileSync(path.join(dir, 'broken.json'), '[{');
            fs.writeFileSync(path.join(dir, 'object.json'), '{"nodes": []}');
            fs.writeFileSync(path.join(dir, 'array_cred.json'), '[]');
            assert.equal(run(['missing.json'], dir).code, 1);
            assert.match(run(['broken.json'], dir).stderr, /not valid JSON/);
            assert.match(run(['object.json'], dir).stderr, /Not a Node-RED flows file/);
            assert.match(run(['flows.json', '--credentials', 'array_cred.json'], dir).stderr, /does not look like/);
            const usage = run(['--frobnicate'], dir);
            assert.equal(usage.code, 2);
            assert.match(usage.stderr, /Unknown option --frobnicate[\s\S]*Usage:/);
            const help = run(['--help'], dir);
            assert.equal(help.code, 0);
            assert.match(help.stdout, /^Usage: kafkajs-nc-migrate-flows <flows\.json>/);
            assert.match(help.stdout, /node migrate-flows\.js <flows\.json>/);
            assert.match(help.stdout, /npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows/);
        });

        it('is shipped as the kafkajs-nc-migrate-flows command (package.json bin)', function () {
            const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
            assert.equal(pkg.bin['kafkajs-nc-migrate-flows'], 'scripts/migrate-flows.js');
            assert.ok(pkg.files.includes('scripts/migrate-flows.js'), 'the script is in the published files');
            assert.match(fs.readFileSync(SCRIPT, 'utf8'), /^#!\/usr\/bin\/env node\n/);
            if (process.platform !== 'win32') assert.ok(fs.statSync(SCRIPT).mode & 0o111, 'executable');
        });
    });

    describe('converted flows in a Node-RED runtime (fake kafkajs)', function () {
        const CA = '-----BEGIN CERTIFICATE-----\nMIIBfake-ca\n-----END CERTIFICATE-----\n';
        const CERT = '-----BEGIN CERTIFICATE-----\nMIIBfake-cert\n-----END CERTIFICATE-----\n';
        const KEY = '-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIBfake-key\n-----END ENCRYPTED PRIVATE KEY-----\n';
        const HARNESS_LOG = [
            /Deprecated call to RED\.runtime\.nodes\.registerType/,
            /^\[runtime\] nodes\.credentials\.unencrypted$/
        ];
        let fake;
        let certDir;

        /**
         * An upstream flow with every client variant (none, SASL SCRAM over SSL,
         * SASL PLAIN without SSL, TLS client certificates), producers and
         * consumers using them, a function node reading the old output shape
         * and debug/helper nodes.
         */
        function upstreamFlow() {
            return [
                TAB,
                client({ id: 'c_none', name: 'Local' }),
                client({ id: 'c_scram', name: 'Cloud', auth: 'sasl', saslmechanism: 'scram-sha-256', saslssl: true,
                    advancedretry: true, retries: '3', loglevel: 'nothing' }),
                client({ id: 'c_plain', name: 'Plain', auth: 'sasl', saslmechanism: 'plain', saslssl: false }),
                client({ id: 'c_tls', name: 'mTLS', auth: 'tls', tlscacert: path.join(certDir, 'ca.pem'),
                    tlsclientcert: path.join(certDir, 'client.pem'), tlsprivatekey: path.join(certDir, 'client.key'),
                    tlspassphrase: 'key-pass', tlsselfsign: false }),
                producer({ id: 'p_none', client: 'c_none', topic: 'orders', wires: [] }),
                producer({ id: 'p_scram', client: 'c_scram', topic: '', acknowledge: 'leader', partition: '0',
                    headeritems: { source: 'node-red' }, key: 'k', responsetimeout: '20000', y: 120 }),
                producer({ id: 'p_tls', client: 'c_tls', topic: 'audit', acknowledge: 'none', y: 160 }),
                consumer({ id: 'k_plain', client: 'c_plain', topic: 'orders', groupid: 'billing', wires: [['fn']] }),
                consumer({ id: 'k_tls', client: 'c_tls', topic: 'audit', advancedoptions: true, sessiontimeout: '10000',
                    heartbeatinterval: '3000', frombeginning: true, wires: [['out_tls']], y: 220 }),
                { id: 'fn', type: 'function', z: 'tab1', name: 'old shape', func: 'msg.value = msg.payload.value;\nreturn msg;',
                    outputs: 1, x: 300, y: 160, wires: [['out_plain', 'dbg']] },
                { id: 'dbg', type: 'debug', z: 'tab1', name: '', active: false, tosidebar: true, console: false,
                    tostatus: false, complete: 'payload', targetType: 'msg', x: 500, y: 160, wires: [] },
                { id: 'out_plain', type: 'helper', z: 'tab1', wires: [] },
                { id: 'out_tls', type: 'helper', z: 'tab1', wires: [] }
            ];
        }

        /** Every node.error/warn and runtime warning since the flow was loaded (harness noise removed). */
        function problems(nodes) {
            const any = nodes.map((n) => helper.getNode(n.id)).find(Boolean);
            const out = [];
            for (const level of ['error', 'warn']) {
                const spy = any[level];
                spy.args.forEach((args, i) => {
                    const node = spy.thisValues[i] || {};
                    const a = args[0];
                    out.push(`${level} [${node.type}:${node.id}] ${a && a.message ? a.message : String(a)}`);
                });
            }
            const log = helper.log();
            for (const args of log.args) {
                const e = args[0] || {};
                if (e.id || typeof e.level !== 'number' || e.level > log.WARN) continue;
                out.push(`[runtime] ${e.msg && e.msg.message ? e.msg.message : String(e.msg)}`);
            }
            return out.filter((p) => !HARNESS_LOG.some((re) => re.test(p.replace(/^(error|warn) /, ''))));
        }

        async function loadConverted({ withCredentials }) {
            const upstream = upstreamFlow();
            const creds = withCredentials ? { c_scram: { saslusername: 'alice', saslpassword: 'alice-secret' },
                c_plain: { saslusername: 'bob', saslpassword: 'bob-secret' } } : null;
            const result = migrate.convertFlows(upstream, { credentials: creds });
            const nodes = result.flows;
            await helper.load([brokerNode, producerNode, consumerNode, tlsNode, functionNode, debugNode], nodes,
                result.credentials || {});
            return { nodes, result };
        }

        before(function () {
            certDir = tmpDir('kafkajs-nc-migrate-certs-');
            fs.writeFileSync(path.join(certDir, 'ca.pem'), CA);
            fs.writeFileSync(path.join(certDir, 'client.pem'), CERT);
            fs.writeFileSync(path.join(certDir, 'client.key'), KEY);
            fake = createFakeKafkajs();
            factory.setKafkajs(fake);
        });

        after(function () {
            factory.setKafkajs(null);
        });

        beforeEach(function (done) {
            fake.reset();
            fake.behaviour.clusterTopics = ['orders', 'audit'];
            helper.settings({ userDir: tmpDir('kafkajs-nc-migrate-ud-') });
            helper.startServer(done);
        });

        afterEach(async function () {
            await helper.unload();
            await helper.stopServer();
        });

        it('loads without errors, connects, produces and consumes (credentials converted)', async function () {
            this.timeout(20000);
            const { nodes, result } = await loadConverted({ withCredentials: true });
            assert.equal(result.credentialsStatus, 'converted');
            for (const n of nodes) {
                if (n.type === 'tab') continue;
                assert.ok(helper.getNode(n.id), `${n.type} ${n.id} was not created`);
            }
            for (const id of ['c_none', 'c_scram', 'c_plain', 'c_tls']) {
                const b = helper.getNode(id);
                assert.equal(b.configError, null, `${id}: ${b.configError && b.configError.message}`);
            }
            const tlsId = byId(nodes, 'c_tls').tlsConfig;
            assert.equal(helper.getNode(tlsId).valid, true);

            const producers = ['p_none', 'p_scram', 'p_tls'];
            const consumers = ['k_plain', 'k_tls'];
            await waitFor(() => producers.every((id) => {
                const s = helper.getNode(id).producerRuntime.snapshot();
                return s.state === 'connected' && s.mode === 'live';
            }) && consumers.every((id) => helper.getNode(id).consumerRuntime.snapshot().state === 'connected'),
            { timeout: 5000, message: 'the converted nodes did not connect' });

            // the converted options reached kafkajs
            const scram = fake.instances.map((k) => k.options).find((o) => o.sasl && o.sasl.mechanism === 'scram-sha-256');
            assert.deepEqual(scram.sasl, { mechanism: 'scram-sha-256', username: 'alice', password: 'alice-secret' });
            assert.equal(scram.ssl, true);
            assert.equal(scram.retry.retries, 3);
            const plain = fake.instances.map((k) => k.options).find((o) => o.sasl && o.sasl.mechanism === 'plain');
            assert.deepEqual([plain.sasl.username, plain.ssl], ['bob', false]);
            const tlsOptions = fake.instances.map((k) => k.options).find((o) => o.ssl && o.ssl.ca);
            assert.ok(tlsOptions, 'no client got the TLS files');
            assert.equal(String(tlsOptions.ssl.ca), CA);
            assert.equal(String(tlsOptions.ssl.cert), CERT);
            assert.equal(String(tlsOptions.ssl.key), KEY);
            assert.equal(tlsOptions.ssl.passphrase, 'key-pass');
            assert.equal(tlsOptions.ssl.rejectUnauthorized, true);

            // produce through every converted producer
            helper.getNode('p_none').receive({ payload: { n: 1 } });
            helper.getNode('p_scram').receive({ topic: 'orders', payload: 'two', headers: { ignored: 'yes' } });
            helper.getNode('p_tls').receive({ payload: 'three' });
            await waitFor(() => fake.sent.length >= 3, { timeout: 3000, message: 'messages were not sent' });
            const sent = (value) => fake.sent.find((m) => String(m.value) === value);
            assert.equal(sent('{"n":1}').topic, 'orders');
            assert.deepEqual([sent('two').topic, sent('two').partition, String(sent('two').key), sent('two').acks],
                ['orders', 0, 'k', 1]);
            assert.deepEqual(Object.keys(sent('two').headers), ['source'], 'msg.headers ignored like before');
            assert.deepEqual([sent('three').topic, sent('three').acks], ['audit', 0]);

            // consume: msg.payload is the value now
            const tlsOut = helper.getNode('out_tls');
            const got = new Promise((resolve) => tlsOut.on('input', resolve));
            const kTls = helper.getNode('k_tls').consumerRuntime;
            assert.equal(kTls.cfg.groupId, 'node-red-k_tls');
            assert.equal(kTls.cfg.fromBeginning, true);
            await kTls.consumer.deliver({ topic: 'audit', message: { key: 'a', value: 'hello' } });
            const msg = await got;
            assert.equal(msg.payload, 'hello');
            assert.equal(msg.key, 'a');

            await sleep(50);
            const expected = [/warn \[kafkajs-nc-broker:c_plain\] SASL PLAIN without TLS sends the password in clear text/];
            const reported = problems(nodes);
            assert.deepEqual(reported.filter((p) => !expected.some((re) => re.test(p))), [], 'unexpected errors or warnings');
            for (const re of expected) assert.ok(reported.some((p) => re.test(p)), `expected ${re}`);
        });

        it('without credentials the SASL brokers report "config error" and the producers queue (no crash)', async function () {
            this.timeout(20000);
            const { nodes, result } = await loadConverted({ withCredentials: false });
            assert.equal(result.credentialsStatus, 'none');
            for (const id of ['c_scram', 'c_plain']) {
                assert.match(String(helper.getNode(id).configError && helper.getNode(id).configError.message),
                    /SASL username is required; SASL password is required/);
            }
            assert.equal(helper.getNode('c_none').configError, null);
            // without the passphrase the TLS client still starts (the key may be unencrypted)
            assert.equal(helper.getNode('c_tls').configError, null);

            const p = helper.getNode('p_scram');
            await waitFor(() => statusCalls(p).some((s) => /^config error/.test(s.text || '')),
                { timeout: 3000, message: 'no config error status' });
            p.receive({ topic: 'orders', payload: 'kept' });
            await waitFor(() => p.producerRuntime.snapshot().queued === 1, { timeout: 3000, message: 'message not queued' });
            const k = helper.getNode('k_plain').consumerRuntime;
            assert.equal(k.snapshot().state, 'idle');
            assert.equal(k.fatal.status, 'config error');
            assert.ok(problems(nodes).some((x) => /Invalid Kafka broker configuration: SASL username is required/.test(x)));
        });
    });
});
