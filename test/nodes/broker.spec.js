'use strict';

/**
 * kafkajs-nc-broker (config node): option building, validation, tls-config
 * integration, safety net reference counting and kafkajs log routing.
 * Uses the fake kafkajs; no Kafka needed.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const brokerNode = require('../../nodes/kafka-broker.js');
const factory = require('../../lib/kafka-factory');
const safetyNet = require('../../lib/safety-net');
const logger = require('../../lib/logger');
const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir } = require('../helpers/common');

// The core tls-config node, loaded from node-red's own dependencies.
const tlsNode = require(require.resolve('@node-red/nodes/core/network/05-tls.js', {
    paths: [path.dirname(require.resolve('node-red'))]
}));

helper.init(require.resolve('node-red'));

const PRODUCER = { type: 'kafkajs-nc-producer', id: 'p1', name: 'My producer' };
const CA_PEM = '-----BEGIN CERTIFICATE-----\nMIIBfake-ca\n-----END CERTIFICATE-----\n';
const CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBfake-cert\n-----END CERTIFICATE-----\n';
const KEY_PEM = '-----BEGIN PRIVATE KEY-----\nMIIBfake-key\n-----END PRIVATE KEY-----\n';

/** A broker config node as the editor saves it (strings everywhere). */
function broker(props = {}) {
    return Object.assign({ id: 'b1', type: 'kafkajs-nc-broker', name: 'B', brokers: 'localhost:9092' }, props);
}

/** A tls-config node as the editor saves it (file paths empty: PEM data comes from credentials). */
function tlsConfig(props = {}) {
    return Object.assign({
        id: 'tls1', type: 'tls-config', name: 'TLS', cert: '', key: '', ca: '', certname: '', keyname: '', caname: '',
        servername: '', verifyservercert: true, alpnprotocol: ''
    }, props);
}

/** Calls of a Node.prototype spy (status/error/warn/...) made by this node only. */
function callsOf(node, method) {
    const spy = node[method];
    return spy.args.filter((_, i) => spy.thisValues[i] === node).map((args) => args[0]);
}

function logged(level) {
    const spy = helper.log();
    return spy.args.map((a) => a[0]).filter((e) => e.level === spy[level]).map((e) => String(e.msg));
}

/**
 * The editor definition of the broker (nodes/kafka-broker.html), evaluated like the editor does, with a
 * minimal jQuery stand-in: `fields` holds the form values by element id, as Node-RED fills them in before
 * oneditprepare (a select keeps a value, a checkbox is checked when the property is truthy).
 */
function editorDefinition() {
    const html = fs.readFileSync(path.join(__dirname, '..', '..', 'nodes', 'kafka-broker.html'), 'utf8');
    const script = /<script type="text\/javascript">([\s\S]*?)<\/script>/.exec(html)[1];
    const fields = {};
    const element = (selector) => {
        const id = String(selector).replace(/^#/, '');
        const el = {
            length: 1,
            val(v) {
                if (v === undefined) return fields[id];
                fields[id] = v;
                return el;
            },
            prop(name, v) {
                if (v === undefined) return fields[id];
                fields[id] = v;
                return el;
            },
            is: () => !!fields[id],
            on: () => el,
            toggle: () => el,
            trigger: () => el,
            each: () => el,
            attr: () => '',
            children: () => el,
            hide: () => el,
            show: () => el
        };
        return el;
    };
    let def;
    vm.runInNewContext(script, {
        RED: { nodes: { registerType(type, d) { def = d; } }, tabs: { create: () => ({ addTab() {}, resize() {} }) } },
        $: element,
        setTimeout: () => {}
    });
    return { def, fields };
}

describe('kafkajs-nc-broker config node', function () {
    let fake;
    const savedEnv = {};

    before(function () {
        fake = createFakeKafkajs();
        factory.setKafkajs(fake);
    });

    after(function () {
        factory.setKafkajs(null);
    });

    beforeEach(function (done) {
        fake.reset();
        logger._resetDedupe();
        helper.settings({ userDir: tmpDir('kafkajs-nc-broker-') });
        helper.startServer(done);
    });

    afterEach(async function () {
        await helper.unload();
        await helper.stopServer();
        for (const [k, v] of Object.entries(savedEnv)) {
            if (v === undefined) delete process.env[k];
            else process.env[k] = v;
            delete savedEnv[k];
        }
    });

    function setEnv(name, value) {
        if (!(name in savedEnv)) savedEnv[name] = process.env[name];
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
    }

    describe('valid configuration', function () {
        it('builds kafkajs options with numbers (not strings), trimmed brokers, SASL and TLS', async function () {
            await helper.load(brokerNode, [broker({
                brokers: '  b1:9092 ,b2:9093\n SASL_SSL://b3 , b1:9092 ',
                clientId: ' my-client ',
                authMechanism: 'scram-sha-256',
                tls: true,
                connectionTimeout: '5000',
                authenticationTimeout: '6000',
                requestTimeout: '40000',
                retries: '3',
                initialRetryTime: '200',
                maxRetryTime: '20000',
                reconnectMin: '2000',
                reconnectMax: '30000',
                authRetryMin: '90000',
                logLevel: 'info',
                safetyNet: true
            })], { b1: { username: 'alice', password: 's3cret' } });
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            assert.deepEqual(b1.warnings, []);
            assert.deepEqual(b1.reconnect, { min: 2000, max: 30000, authMin: 90000 });

            const kafka = b1.createKafka(PRODUCER);
            assert.ok(kafka instanceof fake.Kafka);
            assert.equal(fake.instances.length, 1);
            const o = fake.instances[0].options;
            assert.deepEqual(o.brokers, ['b1:9092', 'b2:9093', 'b3:9092']);
            assert.equal(o.clientId, 'my-client');
            assert.strictEqual(o.connectionTimeout, 5000);
            assert.strictEqual(o.authenticationTimeout, 6000);
            assert.strictEqual(o.requestTimeout, 40000);
            assert.strictEqual(o.enforceRequestTimeout, true);
            assert.deepEqual(o.retry, { retries: 3, initialRetryTime: 200, maxRetryTime: 20000, factor: 0.2, multiplier: 2 });
            assert.strictEqual(o.ssl, true);
            assert.deepEqual(o.sasl, { mechanism: 'scram-sha-256', username: 'alice', password: 's3cret' });
            assert.equal(typeof o.socketFactory, 'function');
            assert.equal(typeof o.logCreator, 'function');
            assert.strictEqual(o.logLevel, fake.logLevel.INFO);
            for (const key of ['connectionTimeout', 'authenticationTimeout', 'requestTimeout', 'logLevel']) {
                assert.equal(typeof o[key], 'number', key);
            }
            for (const value of Object.values(o.retry)) assert.equal(typeof value, 'number');
        });

        it('applies the documented defaults to a minimal configuration', async function () {
            await helper.load(brokerNode, [{ id: 'b1', type: 'kafkajs-nc-broker' }]);
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            const o = fake.instances[0].options;
            assert.deepEqual(o.brokers, ['localhost:9092']);
            assert.equal(o.clientId, 'node-red');
            assert.strictEqual(o.connectionTimeout, 10000);
            assert.strictEqual(o.authenticationTimeout, 10000);
            assert.strictEqual(o.requestTimeout, 30000);
            assert.deepEqual(o.retry, { retries: 5, initialRetryTime: 300, maxRetryTime: 30000, factor: 0.2, multiplier: 2 });
            assert.strictEqual(o.ssl, false);
            assert.equal('sasl' in o, false, 'no sasl key without SASL');
            assert.strictEqual(o.logLevel, fake.logLevel.WARN);
            assert.deepEqual(b1.reconnect, { min: 1000, max: 60000, authMin: 60000 });
        });

        it('exposes only the documented API to client nodes (no unused aliases)', async function () {
            await helper.load(brokerNode, [broker()]);
            const b1 = helper.getNode('b1');
            for (const gone of ['reconnectMin', 'reconnectMax', 'authRetryMin', 'logLevelName', 'summary', 'brokerList']) {
                assert.equal(b1[gone], undefined, gone);
            }
            assert.equal(typeof b1.createKafka, 'function');
        });

        it('warns when a broker prefix contradicts the TLS/SASL settings, instead of looping on "connection closed" (CFG-1)',
            async function () {
                await helper.load(brokerNode, [broker({ brokers: 'SASL_SSL://pkc-1.eu-west-1.aws.confluent.cloud:9092' })]);
                const b1 = helper.getNode('b1');
                assert.equal(b1.configError, null);
                assert.deepEqual(callsOf(b1, 'warn').map(String), [
                    'Brokers are given as SASL_SSL:// but "Use TLS" is off',
                    'Brokers are given as SASL_SSL:// but no SASL mechanism is selected'
                ]);
            });

        it('creates a NEW kafkajs client for every call (one per connect attempt)', async function () {
            await helper.load(brokerNode, [broker()]);
            const b1 = helper.getNode('b1');
            const k1 = b1.createKafka(PRODUCER);
            const k2 = b1.createKafka(PRODUCER);
            assert.notEqual(k1, k2);
            assert.equal(fake.instances.length, 2);
            assert.notEqual(fake.instances[0].options.socketFactory, fake.instances[1].options.socketFactory);
        });

        it('clamps out-of-range numbers and warns about values that are not numbers', async function () {
            await helper.load(brokerNode, [broker({
                requestTimeout: '5', connectionTimeout: 'abc', retries: '999', reconnectMin: '5000', reconnectMax: '10', logLevel: 'loud'
            })]);
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            const o = fake.instances[0].options;
            assert.strictEqual(o.requestTimeout, 1000);
            assert.strictEqual(o.connectionTimeout, 10000);
            assert.strictEqual(o.retry.retries, 50);
            assert.strictEqual(o.logLevel, fake.logLevel.WARN);
            assert.deepEqual(b1.reconnect, { min: 5000, max: 5000, authMin: 60000 }, 'reconnectMax is never below reconnectMin');
            const warns = callsOf(b1, 'warn').map(String);
            assert.ok(warns.some((w) => /connectionTimeout "abc" is not a number/.test(w)), warns.join('\n'));
            assert.ok(warns.some((w) => /Unknown log level "loud"/.test(w)), warns.join('\n'));
            assert.deepEqual(b1.warnings, warns);
        });

        it('warns (but works) for SASL PLAIN without TLS', async function () {
            await helper.load(brokerNode, [broker({ authMechanism: 'plain', tls: false })], { b1: { username: 'u', password: 'p' } });
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            assert.ok(callsOf(b1, 'warn').some((w) => /SASL PLAIN without TLS/.test(w)));
            b1.createKafka(PRODUCER);
            assert.deepEqual(fake.instances[0].options.sasl, { mechanism: 'plain', username: 'u', password: 'p' });
        });

        it('does not crash and builds nothing invalid from odd input types (array brokers, numbers, booleans)', async function () {
            await helper.load(brokerNode, [broker({
                brokers: ['h1:1', 'h2'], connectionTimeout: 3000, tls: 'true', safetyNet: 'false', retries: true, clientId: 42
            })]);
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            const o = fake.instances[0].options;
            assert.deepEqual(o.brokers, ['h1:1', 'h2:9092']);
            assert.strictEqual(o.connectionTimeout, 3000);
            assert.strictEqual(o.ssl, true);
            assert.strictEqual(o.retry.retries, 5);
            assert.equal(o.clientId, '42');
        });
    });

    describe('invalid configuration', function () {
        for (const [title, creds, expected] of [
            ['password missing (undefined)', { username: 'alice' }, ['SASL password is required']],
            ['password empty', { username: 'alice', password: '' }, ['SASL password is required']],
            ['username and password empty', { username: '', password: '' }, ['SASL username is required', 'SASL password is required']],
            ['no credentials at all', undefined, ['SASL username is required', 'SASL password is required']],
            ['username blank', { username: '   ', password: 'x' }, ['SASL username is required']]
        ]) {
            it(`SASL with ${title} => configError, node.error, no Kafka client, no crash`, async function () {
                await helper.load(brokerNode, [broker({ authMechanism: 'scram-sha-512', tls: true })], creds ? { b1: creds } : {});
                const b1 = helper.getNode('b1');
                assert.ok(b1.configError, 'configError is set');
                assert.equal(b1.configError.name, 'KafkaNcConfigError');
                assert.deepEqual(b1.configError.details, expected);
                const errors = callsOf(b1, 'error').map(String);
                assert.equal(errors.length, 1, 'reported exactly once');
                assert.match(errors[0], /^Invalid Kafka broker configuration: /);
                for (const text of expected) assert.ok(errors[0].includes(text), errors[0]);
                assert.throws(() => b1.createKafka(PRODUCER), (err) => {
                    assert.equal(err.name, 'KafkaNcConfigError');
                    assert.deepEqual(err.details, expected);
                    return true;
                });
                assert.equal(fake.instances.length, 0, 'no kafkajs client was created');
            });
        }

        it('reports every problem at once (brokers, mechanism, credentials)', async function () {
            await helper.load(brokerNode, [broker({ brokers: '[::1]:9092, good:9092, bad:0', authMechanism: 'gssapi' })]);
            const b1 = helper.getNode('b1');
            const details = b1.configError.details;
            assert.ok(details.some((d) => d.startsWith('IPv6 literal brokers are not supported by kafkajs')), details.join('\n'));
            assert.ok(details.some((d) => /Invalid broker "bad:0"/.test(d)), details.join('\n'));
            assert.ok(details.some((d) => /Unknown SASL mechanism "gssapi"/.test(d)), details.join('\n'));
            assert.equal(callsOf(b1, 'error').length, 1);
        });

        it('an empty broker list is an error', async function () {
            await helper.load(brokerNode, [broker({ brokers: ' , ' })]);
            assert.deepEqual(helper.getNode('b1').configError.details, ['No brokers configured']);
        });

        it('wraps a synchronous throw of the kafkajs Kafka constructor in a ConfigError', async function () {
            await helper.load(brokerNode, [broker()]);
            const b1 = helper.getNode('b1');
            const boom = new TypeError('logCreator is not a function');
            factory.setKafkajs({ Kafka: function () { throw boom; } });
            try {
                assert.throws(() => b1.createKafka(PRODUCER), (err) => {
                    assert.equal(err.name, 'KafkaNcConfigError');
                    assert.match(err.message, /Could not create the Kafka client: logCreator is not a function/);
                    assert.equal(err.cause, boom);
                    return true;
                });
            } finally {
                factory.setKafkajs(fake);
            }
        });

        it('createKafka() refuses to create clients after the node was closed', async function () {
            await helper.load(brokerNode, [broker()]);
            const b1 = helper.getNode('b1');
            await helper.unload();
            assert.throws(() => b1.createKafka(PRODUCER), /has been stopped/);
            assert.equal(fake.instances.length, 0);
        });
    });

    describe('environment variables', function () {
        it('uses ${VAR} values that are set (config and credentials)', async function () {
            setEnv('KAFKA_NC_TEST_BROKERS', 'envhost:9095,envhost2');
            setEnv('KAFKA_NC_TEST_USER', 'env-user');
            setEnv('KAFKA_NC_TEST_PASSWORD', 'env-secret');
            await helper.load(brokerNode, [broker({ brokers: '${KAFKA_NC_TEST_BROKERS}', authMechanism: 'plain', tls: true })],
                { b1: { username: '${KAFKA_NC_TEST_USER}', password: '${KAFKA_NC_TEST_PASSWORD}' } });
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            const o = fake.instances[0].options;
            assert.deepEqual(o.brokers, ['envhost:9095', 'envhost2:9092']);
            assert.deepEqual(o.sasl, { mechanism: 'plain', username: 'env-user', password: 'env-secret' });
        });

        it('reports ${VAR} leftovers (variables that are not set) as config errors', async function () {
            setEnv('KAFKA_NC_TEST_UNSET_BROKERS', undefined);
            setEnv('KAFKA_NC_TEST_UNSET_PW', undefined);
            await helper.load(brokerNode, [broker({ brokers: '${KAFKA_NC_TEST_UNSET_BROKERS}', authMechanism: 'plain', tls: true })],
                { b1: { username: 'alice', password: '${KAFKA_NC_TEST_UNSET_PW}' } });
            const b1 = helper.getNode('b1');
            assert.deepEqual(b1.configError.details, [
                'environment variable KAFKA_NC_TEST_UNSET_BROKERS is not set',
                'environment variable KAFKA_NC_TEST_UNSET_PW is not set'
            ]);
            assert.throws(() => b1.createKafka(PRODUCER), /environment variable KAFKA_NC_TEST_UNSET_BROKERS is not set/);
            assert.equal(fake.instances.length, 0);
        });

        it('a leftover ${VAR} in a numeric field falls back to the default with a warning', async function () {
            setEnv('KAFKA_NC_TEST_UNSET_RT', undefined);
            await helper.load(brokerNode, [broker({ requestTimeout: '${KAFKA_NC_TEST_UNSET_RT}' })]);
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            assert.ok(b1.warnings.some((w) => /requestTimeout: environment variable KAFKA_NC_TEST_UNSET_RT is not set/.test(w)));
            b1.createKafka(PRODUCER);
            assert.strictEqual(fake.instances[0].options.requestTimeout, 30000);
        });
    });

    describe('tls-config integration', function () {
        it('passes CA, server name and verification from a tls-config node (PEM in credentials)', async function () {
            await helper.load([tlsNode, brokerNode], [
                tlsConfig({ servername: 'kafka.internal', verifyservercert: false }),
                broker({ tls: true, tlsConfig: 'tls1' })
            ], { tls1: { cadata: CA_PEM } });
            const b1 = helper.getNode('b1');
            assert.equal(helper.getNode('tls1').valid, true);
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            assert.deepEqual(fake.instances[0].options.ssl, { ca: CA_PEM, servername: 'kafka.internal', rejectUnauthorized: false });
        });

        it('passes a client certificate, key and passphrase (mutual TLS) and verifies by default', async function () {
            await helper.load([tlsNode, brokerNode], [
                tlsConfig({ verifyservercert: true }),
                broker({ tls: 'true', tlsConfig: 'tls1', authMechanism: 'plain' })
            ], { tls1: { certdata: CERT_PEM, keydata: KEY_PEM, cadata: CA_PEM, passphrase: 'pw' }, b1: { username: 'u', password: 'p' } });
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            const o = fake.instances[0].options;
            assert.deepEqual(o.ssl, { key: KEY_PEM, cert: CERT_PEM, ca: CA_PEM, passphrase: 'pw', rejectUnauthorized: true });
            assert.deepEqual(o.sasl, { mechanism: 'plain', username: 'u', password: 'p' });
        });

        it('an invalid tls-config node (certificate without key) => configError, no client', async function () {
            await helper.load([tlsNode, brokerNode], [
                tlsConfig(),
                broker({ tls: true, tlsConfig: 'tls1' })
            ], { tls1: { certdata: CERT_PEM } });
            assert.equal(helper.getNode('tls1').valid, false);
            const b1 = helper.getNode('b1');
            assert.ok(b1.configError);
            assert.deepEqual(b1.configError.details, ['TLS configuration is invalid (check its certificate/key files)']);
            assert.ok(callsOf(b1, 'error').some((e) => /TLS configuration is invalid/.test(e)));
            assert.throws(() => b1.createKafka(PRODUCER), /TLS configuration is invalid/);
            assert.equal(fake.instances.length, 0);
        });

        it('a tls-config node that does not exist is detected when connecting', async function () {
            await helper.load(brokerNode, [broker({ tls: true, tlsConfig: 'missing-tls' })]);
            const b1 = helper.getNode('b1');
            // it may simply not be created yet while config nodes start: no error at construction
            assert.equal(b1.configError, null);
            assert.throws(() => b1.createKafka(PRODUCER), (err) => {
                assert.equal(err.name, 'KafkaNcConfigError');
                assert.deepEqual(err.details, ['TLS configuration node not found']);
                return true;
            });
            assert.equal(fake.instances.length, 0);
            assert.deepEqual(b1.configError.details, ['TLS configuration node not found'], 'remembered for the client nodes');
        });

        it('ignores the tls-config reference when TLS is off', async function () {
            await helper.load([tlsNode, brokerNode], [
                tlsConfig(),
                broker({ tls: false, tlsConfig: 'tls1' })
            ], { tls1: { certdata: CERT_PEM } }); // invalid, but unused
            const b1 = helper.getNode('b1');
            assert.equal(b1.configError, null);
            b1.createKafka(PRODUCER);
            assert.strictEqual(fake.instances[0].options.ssl, false);
        });

        it('TLS without a tls-config node uses the system CAs and verifies (ssl: true)', async function () {
            await helper.load(brokerNode, [broker({ tls: true, tlsConfig: '' })]);
            helper.getNode('b1').createKafka(PRODUCER);
            assert.strictEqual(fake.instances[0].options.ssl, true);
        });
    });

    describe('safety net', function () {
        let savedGrace;
        beforeEach(function () {
            savedGrace = safetyNet._setReleaseGrace(0); // remove at once; the grace period has its own test
        });
        afterEach(function () {
            safetyNet._setReleaseGrace(savedGrace);
        });

        it('stays installed while client nodes are still closing after the broker closed (CRASH-2)', async function () {
            const base = process.listenerCount('unhandledRejection');
            safetyNet._setReleaseGrace(300);
            await helper.load(brokerNode, [broker()]);
            await helper.unload();
            assert.equal(process.listenerCount('unhandledRejection'), base + 1, 'still installed after the broker closed');
            assert.equal(safetyNet.isInstalled(), true);
            assert.equal(safetyNet.handleRejection(fake.errors.connectionRefused(), 1, null, 'throw'), 'swallowed');
            await new Promise((r) => setTimeout(r, 400));
            assert.equal(process.listenerCount('unhandledRejection'), base, 'removed after the grace period');
        });

        it('is acquired by default and released on close (ref-counted across broker nodes)', async function () {
            const base = process.listenerCount('unhandledRejection');
            const baseRefs = safetyNet.references();
            await helper.load(brokerNode, [broker(), broker({ id: 'b2', brokers: 'other:9092' })]);
            assert.equal(process.listenerCount('unhandledRejection'), base + 1, 'one process listener for both nodes');
            assert.equal(safetyNet.isInstalled(), true);
            assert.equal(safetyNet.references(), baseRefs + 2);
            await helper.unload();
            assert.equal(safetyNet.references(), baseRefs);
            assert.equal(process.listenerCount('unhandledRejection'), base);
            assert.equal(safetyNet.isInstalled(), baseRefs > 0);
        });

        it('is not installed when disabled in the configuration', async function () {
            const base = process.listenerCount('unhandledRejection');
            await helper.load(brokerNode, [broker({ safetyNet: false }), broker({ id: 'b2', safetyNet: 'false' })]);
            assert.equal(process.listenerCount('unhandledRejection'), base);
            assert.equal(safetyNet.references(), 0);
        });

        it('is released when the node is removed by a deploy, and the broker is installed again on re-add', async function () {
            const base = process.listenerCount('unhandledRejection');
            await helper.load(brokerNode, [broker()]);
            assert.equal(process.listenerCount('unhandledRejection'), base + 1);
            await helper.setFlows([], 'full');
            assert.equal(process.listenerCount('unhandledRejection'), base);
            await helper.setFlows([broker()], 'full');
            assert.equal(process.listenerCount('unhandledRejection'), base + 1);
        });

        it('swallows an unhandled KafkaJS rejection while installed (logged as a warning, no crash)', async function () {
            await helper.load(brokerNode, [broker()]);
            safetyNet._resetDedupe();
            // what Node would do with an unhandled rejection: call the listeners
            safetyNet._processListener(fake.errors.connectionRefused());
            assert.ok(logged('WARN').some((m) => m.startsWith('[kafkajs-nc] Prevented a crash: unhandled KafkaJS error')));
        });
    });

    describe('editor (nodes/kafka-broker.html)', function () {
        it('accepts every SASL mechanism spelling the runtime accepts (VALID-2)', function () {
            const { def, fields } = editorDefinition();
            fields['node-config-input-username'] = 'alice';
            const valid = def.defaults.authMechanism.validate;
            for (const v of ['none', 'NONE', '', 'plain', 'PLAIN', 'SCRAM-SHA-256', 'scram-sha-512', ' Scram-Sha-512 ']) {
                assert.equal(valid(v, {}), true, v);
            }
            assert.equal(valid('gssapi', {}), 'unknown SASL mechanism');
        });

        it('shows what the runtime will use for hand-written values, so open + Done changes nothing (VALID-2)', function () {
            const { def, fields } = editorDefinition();
            // what Node-RED puts into the form from the node's properties before oneditprepare
            Object.assign(fields, {
                'node-config-input-authMechanism': 'PLAIN', 'node-config-input-logLevel': 'DEBUG',
                'node-config-input-safetyNet': 'false', 'node-config-input-tls': 'false'
            });
            def.oneditprepare.call({ authMechanism: 'PLAIN', logLevel: 'DEBUG', safetyNet: 'false', tls: 'false' });
            assert.equal(fields['node-config-input-authMechanism'], 'plain');
            assert.equal(fields['node-config-input-logLevel'], 'debug');
            assert.equal(fields['node-config-input-safetyNet'], false, "the string 'false' is off at runtime");
            assert.equal(fields['node-config-input-tls'], false);
            def.oneditprepare.call({ authMechanism: undefined, logLevel: 'nothing', safetyNet: undefined, tls: 'true' });
            assert.equal(fields['node-config-input-authMechanism'], 'none');
            assert.equal(fields['node-config-input-logLevel'], 'off');
            assert.equal(fields['node-config-input-safetyNet'], true, 'on by default');
            assert.equal(fields['node-config-input-tls'], true);
        });

        it('validates brokers like the runtime (semicolons, credentials, exact ${VAR})', function () {
            const valid = editorDefinition().def.defaults.brokers.validate;
            assert.equal(valid('kafka1:9092;kafka2:9092', {}), true);
            assert.equal(valid('${KAFKA_BROKERS}', {}), true);
            assert.match(String(valid(' ${KAFKA_BROKERS}', {})), /invalid broker address/, 'not replaced by Node-RED');
            const credentials = valid('SASL_SSL://user:secret@b1:9093', {});
            assert.equal(credentials, 'credentials do not belong in the broker address: set them on the Security tab');
            assert.match(String(valid('b1:9092:9093', {})), /invalid broker address b1:9092:9093/);
        });

        it('says that the safety net covers promise rejections only (DOC-3)', function () {
            const html = fs.readFileSync(path.join(__dirname, '..', '..', 'nodes', 'kafka-broker.html'), 'utf8');
            assert.match(html, /Crash safety net for unhandled kafkajs promise rejections/);
            assert.match(html, /covers promise rejections only, not uncaught\s+exceptions/);
            assert.match(html, /--unhandled-rejections=strict/);
        });
    });

    describe('kafkajs logging', function () {
        it('routes kafkajs log lines to RED.log, labelled with the client node', async function () {
            await helper.load(brokerNode, [broker({ logLevel: 'debug' })]);
            helper.getNode('b1').createKafka(PRODUCER);
            const instance = fake.instances[0];
            instance.logFn({
                namespace: 'Connection', level: fake.logLevel.ERROR, label: 'ERROR',
                log: { message: 'Connection error: connect ECONNREFUSED 127.0.0.1:9092', broker: 'localhost:9092' }
            });
            instance.logger().info('Starting');
            instance.logger().debug('details');
            const warns = logged('WARN');
            assert.ok(warns.includes('[kafkajs:Connection] [kafkajs-nc-producer:My producer] Connection error: connect ' +
                'ECONNREFUSED 127.0.0.1:9092 (broker localhost:9092)'), warns.join('\n'));
            assert.ok(logged('INFO').some((m) => m.includes('[kafkajs-nc-producer:My producer] Starting')));
            assert.ok(logged('DEBUG').some((m) => m.includes('[kafkajs-nc-producer:My producer] details')));
        });

        it('honours the configured level (kafkajs filters with the numeric level)', async function () {
            await helper.load(brokerNode, [broker({ logLevel: 'error' })]);
            helper.getNode('b1').createKafka(PRODUCER);
            const log = fake.instances[0].logger();
            log.warn('a warning that must be filtered');
            log.error('an error that must pass');
            const warns = logged('WARN');
            assert.ok(!warns.some((m) => m.includes('a warning that must be filtered')));
            assert.ok(warns.some((m) => m.includes('an error that must pass')));
        });

        it('labels lines with the broker itself when no client node is given', async function () {
            await helper.load(brokerNode, [broker({ name: 'Cluster A' })]);
            helper.getNode('b1').createKafka();
            fake.instances[0].logger().error('hello');
            assert.ok(logged('WARN').some((m) => m.includes('[kafkajs-nc-broker:Cluster A] hello')));
        });

        it('a log creator never throws, even for hostile entries', async function () {
            await helper.load(brokerNode, [broker()]);
            helper.getNode('b1').createKafka(PRODUCER);
            const circular = {};
            circular.self = circular;
            const logFn = fake.instances[0].logFn;
            assert.doesNotThrow(() => logFn(undefined));
            assert.doesNotThrow(() => logFn({ level: 1, log: { message: circular, error: circular } }));
            assert.doesNotThrow(() => logFn({ level: 1, get log() { throw new Error('getter'); } }));
        });
    });
});
