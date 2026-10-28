'use strict';

const assert = require('node:assert/strict');
const kafkajs = require('kafkajs');
const {
    DEFAULTS, parseBrokers, normalizeBrokerConfig, validateBrokerConfig, buildClientOptions, summarize
} = require('../../lib/client-config');

/**
 * Construct the REAL core Node-RED tls-config node (05-tls.js) with a minimal
 * fake RED, so addTLSOptions()/valid behave exactly as in Node-RED.
 */
function realTlsConfigNode(config, credentials) {
    let Ctor;
    const errors = [];
    const RED = {
        nodes: {
            createNode(node, conf) {
                node.id = conf.id;
                node.credentials = credentials || {};
                node.error = (e) => errors.push(e);
            },
            registerType(type, ctor) {
                Ctor = ctor;
            }
        },
        _: (k) => k
    };
    require('@node-red/nodes/core/network/05-tls.js')(RED);
    const node = new Ctor({ id: 'tls1', cert: '', key: '', ca: '', verifyservercert: true, servername: '', alpnprotocol: '',
        ...config });
    node._errors = errors;
    return node;
}

describe('lib/client-config', function () {
    describe('parseBrokers', function () {
        const ok = (input, expected) => {
            const r = parseBrokers(input);
            assert.deepEqual(r.errors, [], `errors for ${JSON.stringify(input)}`);
            assert.deepEqual(r.brokers, expected, `brokers for ${JSON.stringify(input)}`);
        };

        it('splits on commas and/or whitespace, ignoring empty entries', function () {
            ok('localhost:9092', ['localhost:9092']);
            ok('a:1,b:2', ['a:1', 'b:2']);
            ok(' a:1 , b:2 ', ['a:1', 'b:2']);
            ok('a:1,,b:2,', ['a:1', 'b:2']);
            ok('a:1 b:2\nc:3\t d:4', ['a:1', 'b:2', 'c:3', 'd:4']);
            ok(',a:1,', ['a:1']);
        });

        it('accepts arrays (also with comma lists inside)', function () {
            ok(['a:1', 'b:2,c:3', ' '], ['a:1', 'b:2', 'c:3']);
        });

        it('strips scheme prefixes and trailing slashes', function () {
            ok('PLAINTEXT://a:1', ['a:1']);
            ok('SSL://a:1', ['a:1']);
            ok('sasl_ssl://a:1', ['a:1']);
            ok('SASL_PLAINTEXT://a:1', ['a:1']);
            ok('kafka://a:1/', ['a:1']);
            ok('tcp://a:1', ['a:1']);
            ok('SASL_SSL://pkc-123.eu-west-1.aws.confluent.cloud:9092', ['pkc-123.eu-west-1.aws.confluent.cloud:9092']);
        });

        it('rejects unknown schemes', function () {
            const r = parseBrokers('https://a:1');
            assert.deepEqual(r.brokers, []);
            assert.match(r.errors[0], /unsupported scheme "https:\/\/"/);
        });

        it('appends :9092 when the port is missing', function () {
            ok('kafka', ['kafka:9092']);
            ok('10.0.0.5', ['10.0.0.5:9092']);
            ok('SSL://broker', ['broker:9092']);
        });

        it('normalises ports and removes duplicates', function () {
            ok('a:09092', ['a:9092']);
            ok('a:9092,a:9092, a', ['a:9092']);
        });

        it('rejects IPv6 literals (kafkajs splits on ":")', function () {
            for (const v of ['[::1]:9092', '::1', 'fe80::1:9092', '[2001:db8::1]']) {
                const r = parseBrokers(v);
                assert.deepEqual(r.brokers, []);
                assert.equal(r.errors.length, 1);
                assert.ok(r.errors[0].startsWith('IPv6 literal brokers are not supported by kafkajs'), r.errors[0]);
            }
        });

        it('reports leftover ${VAR} references', function () {
            assert.deepEqual(parseBrokers('${KAFKA_BROKERS}'),
                { brokers: [], errors: ['environment variable KAFKA_BROKERS is not set'] });
            assert.deepEqual(parseBrokers('a:1,${B2}').errors, ['environment variable B2 is not set']);
            assert.deepEqual(parseBrokers('$(OLD_STYLE)').errors, ['environment variable OLD_STYLE is not set']);
            assert.deepEqual(parseBrokers('kafka-${STAGE}:9092').errors, ['environment variable STAGE is not set'],
                'a partial reference can never form a valid host');
        });

        it('reports invalid ports and hosts', function () {
            for (const v of ['a:0', 'a:65536', 'a:abc', 'a:', 'a:-1', 'a:1.5', 'a:123456']) {
                const r = parseBrokers(v);
                assert.deepEqual(r.brokers, [], v);
                assert.match(r.errors[0], /port must be a number between 1 and 65535/, v);
            }
            assert.match(parseBrokers(':9092').errors[0], /missing host/);
            assert.match(parseBrokers('a/b:9092').errors[0], /invalid host/);
            assert.match(parseBrokers('ho$t:1').errors[0], /invalid host/);
        });

        it('keeps the valid brokers next to the errors', function () {
            const r = parseBrokers('good:1, bad:0, also-good');
            assert.deepEqual(r.brokers, ['good:1', 'also-good:9092']);
            assert.equal(r.errors.length, 1);
        });

        it('reports an empty list', function () {
            for (const v of ['', '   ', ' , , ', undefined, null, [], [''], {}]) {
                assert.deepEqual(parseBrokers(v), { brokers: [], errors: ['No brokers configured'] }, JSON.stringify(v));
            }
        });

        it('never throws', function () {
            assert.doesNotThrow(() => parseBrokers({ toString() { throw new Error('x'); } }));
            assert.doesNotThrow(() => parseBrokers([Symbol('x')]));
            assert.doesNotThrow(() => parseBrokers(12345));
        });
    });

    describe('validateBrokerConfig', function () {
        it('accepts the defaults', function () {
            assert.deepEqual(validateBrokerConfig({}, {}), { errors: [], warnings: [] });
            assert.deepEqual(validateBrokerConfig(DEFAULTS, undefined), { errors: [], warnings: [] });
            assert.deepEqual(validateBrokerConfig(undefined, undefined), { errors: [], warnings: [] });
        });

        it('requires SASL username and password (the upstream crash)', function () {
            const r = validateBrokerConfig({ authMechanism: 'scram-sha-256' }, {});
            assert.deepEqual(r.errors, ['SASL username is required', 'SASL password is required']);
        });

        it("treats '' and undefined credentials alike", function () {
            const a = validateBrokerConfig({ authMechanism: 'plain', tls: true }, { username: '', password: '' });
            const b = validateBrokerConfig({ authMechanism: 'plain', tls: true }, { username: undefined, password: undefined });
            const c = validateBrokerConfig({ authMechanism: 'plain', tls: true }, {});
            assert.deepEqual(a, b);
            assert.deepEqual(a, c);
            assert.deepEqual(a.errors, ['SASL username is required', 'SASL password is required']);
        });

        it('reports only what is missing', function () {
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true }, { username: 'alice' }).errors,
                ['SASL password is required']);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true }, { password: 'pw' }).errors,
                ['SASL username is required']);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true }, { username: '  ', password: 'pw' })
                .errors, ['SASL username is required']);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true }, { username: 'a', password: ' ' })
                .errors, [], 'a password of spaces is a password');
        });

        it('reports leftover ${VAR} in credentials', function () {
            const r = validateBrokerConfig({ authMechanism: 'plain', tls: true },
                { username: '${KAFKA_USER}', password: '${KAFKA_PASSWORD}' });
            assert.deepEqual(r.errors, ['environment variable KAFKA_USER is not set',
                'environment variable KAFKA_PASSWORD is not set']);
        });

        it('accepts credentials that merely contain "${" (Node-RED only substitutes whole values)', function () {
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true },
                { username: 'user', password: 'pa${ss}word' }).errors, []);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true },
                { username: 'user', password: ' ${KAFKA_PASSWORD} ' }).errors, ['environment variable KAFKA_PASSWORD is not set']);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: true },
                { username: '$(KUSER)', password: 'x' }).errors, ['environment variable KUSER is not set']);
        });

        it('ignores credentials when SASL is off', function () {
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'none' }, { username: '', password: '${X}' }).errors, []);
        });

        it('rejects unknown mechanisms and accepts any case', function () {
            assert.match(validateBrokerConfig({ authMechanism: 'oauthbearer' }, { username: 'u', password: 'p' }).errors[0],
                /Unknown SASL mechanism "oauthbearer"/);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'SCRAM-SHA-512' }, { username: 'u', password: 'p' }).errors,
                []);
            assert.deepEqual(validateBrokerConfig({ authMechanism: '' }, {}).errors, [], "'' means none");
        });

        it('collects ALL errors at once', function () {
            const r = validateBrokerConfig({ brokers: '[::1]:9092, ${B}', authMechanism: 'plain' }, {});
            assert.equal(r.errors.length, 4);
        });

        it('warns about SASL PLAIN without TLS', function () {
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain' }, { username: 'u', password: 'p' }).warnings,
                ['SASL PLAIN without TLS sends the password in clear text']);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'plain', tls: 'true' }, { username: 'u', password: 'p' })
                .warnings, []);
            assert.deepEqual(validateBrokerConfig({ authMechanism: 'scram-sha-256' }, { username: 'u', password: 'p' })
                .warnings, []);
        });

        it('checks the tls-config node only when asked (pass tlsNode, even null)', function () {
            const config = { tls: true, tlsConfig: 'tls1' };
            assert.deepEqual(validateBrokerConfig(config, {}).errors, []);
            assert.deepEqual(validateBrokerConfig(config, {}, { tlsNode: null }).errors, ['TLS configuration node not found']);
            assert.deepEqual(validateBrokerConfig(config, {}, { tlsNode: { valid: false } }).errors,
                ['TLS configuration is invalid (check its certificate/key files)']);
            assert.deepEqual(validateBrokerConfig(config, {}, { tlsNode: { valid: true } }).errors, []);
            assert.deepEqual(validateBrokerConfig({ tls: false, tlsConfig: 'tls1' }, {}, { tlsNode: null }).errors, [],
                'ignored when TLS is off');
        });

        it('warns about non-numeric numbers and unknown log levels', function () {
            const r = validateBrokerConfig({ requestTimeout: 'abc', retries: '${RETRIES}', logLevel: 'verbose' }, {});
            assert.deepEqual(r.errors, []);
            assert.deepEqual(r.warnings, [
                'requestTimeout "abc" is not a number; using 30000',
                'retries: environment variable RETRIES is not set; using 5',
                'Unknown log level "verbose"; using "warn"'
            ]);
        });

        it('never throws', function () {
            const hostile = new Proxy({}, { get() { throw new Error('trap'); } });
            const r = validateBrokerConfig(hostile, hostile);
            assert.ok(Array.isArray(r.errors) && Array.isArray(r.warnings));
            assert.doesNotThrow(() => validateBrokerConfig('x', 5));
        });
    });

    describe('normalizeBrokerConfig (numeric coercion)', function () {
        it('turns editor strings into numbers', function () {
            const n = normalizeBrokerConfig({
                connectionTimeout: '5000', authenticationTimeout: '7000', requestTimeout: '30000', retries: '3',
                initialRetryTime: '200', maxRetryTime: '20000', reconnectMin: '2000', reconnectMax: '30000', authRetryMin: '90000'
            }, {});
            assert.equal(n.connectionTimeout, 5000);
            assert.equal(n.authenticationTimeout, 7000);
            assert.equal(n.requestTimeout, 30000);
            assert.equal(n.retries, 3);
            assert.equal(n.initialRetryTime, 200);
            assert.equal(n.maxRetryTime, 20000);
            assert.deepEqual(n.reconnect, { min: 2000, max: 30000, authMin: 90000 });
            for (const k of ['connectionTimeout', 'authenticationTimeout', 'requestTimeout', 'retries', 'initialRetryTime',
                'maxRetryTime']) {
                assert.equal(typeof n[k], 'number', k);
            }
        });

        it('uses defaults for blank/invalid values and clamps out-of-range values', function () {
            const n = normalizeBrokerConfig({
                connectionTimeout: '', authenticationTimeout: '50', requestTimeout: '99999999', retries: '-1',
                initialRetryTime: 'NaN', maxRetryTime: null, reconnectMin: '1', authRetryMin: '500'
            }, {});
            assert.equal(n.connectionTimeout, 10000);
            assert.equal(n.authenticationTimeout, 100);
            assert.equal(n.requestTimeout, 600000);
            assert.equal(n.retries, 0);
            assert.equal(n.initialRetryTime, 300);
            assert.equal(n.maxRetryTime, 30000);
            assert.equal(n.reconnect.min, 100);
            assert.equal(n.reconnect.authMin, 1000);
            assert.equal(normalizeBrokerConfig({ retries: '100' }, {}).retries, 50);
            assert.equal(normalizeBrokerConfig({ retries: '2.7' }, {}).retries, 2);
        });

        it('keeps reconnectMax >= reconnectMin', function () {
            assert.deepEqual(normalizeBrokerConfig({ reconnectMin: '5000', reconnectMax: '1000' }, {}).reconnect.max, 5000);
            assert.deepEqual(normalizeBrokerConfig({ reconnectMin: '600000' }, {}).reconnect.max, 600000);
            assert.deepEqual(normalizeBrokerConfig({}, {}).reconnect, { min: 1000, max: 60000, authMin: 60000 });
        });

        it('normalises clientId, log level, tls and safetyNet', function () {
            assert.equal(normalizeBrokerConfig({ clientId: '' }, {}).clientId, 'node-red');
            assert.equal(normalizeBrokerConfig({ clientId: '  my-app ' }, {}).clientId, 'my-app');
            const v = normalizeBrokerConfig({ clientId: '${CID}' }, {});
            assert.equal(v.clientId, 'node-red');
            assert.match(v.warnings[0], /clientId: environment variable CID is not set/);
            assert.equal(normalizeBrokerConfig({ logLevel: 'DEBUG' }, {}).logLevelName, 'debug');
            assert.equal(normalizeBrokerConfig({ logLevel: 'nothing' }, {}).logLevelName, 'off');
            assert.equal(normalizeBrokerConfig({}, {}).logLevelName, 'warn');
            assert.equal(normalizeBrokerConfig({ tls: 'true' }, {}).tls, true);
            assert.equal(normalizeBrokerConfig({ tls: 'false' }, {}).tls, false);
            assert.equal(normalizeBrokerConfig({}, {}).safetyNet, true);
            assert.equal(normalizeBrokerConfig({ safetyNet: false }, {}).safetyNet, false);
            assert.equal(normalizeBrokerConfig({ safetyNet: 'false' }, {}).safetyNet, false);
        });
    });

    describe('buildClientOptions', function () {
        it('builds kafkajs options with numbers only (defaults)', function () {
            const { options, error, summary } = buildClientOptions({}, {});
            assert.equal(error, null);
            assert.deepEqual(options, {
                clientId: 'node-red',
                brokers: ['localhost:9092'],
                connectionTimeout: 10000,
                authenticationTimeout: 10000,
                requestTimeout: 30000,
                enforceRequestTimeout: true,
                retry: { retries: 5, initialRetryTime: 300, maxRetryTime: 30000, factor: 0.2, multiplier: 2 },
                ssl: false,
                logLevel: kafkajs.logLevel.WARN
            });
            assert.equal(summary, 'localhost:9092');
        });

        it('adds SASL, socketFactory, logCreator and an explicit logLevel', function () {
            const socketFactory = () => {};
            const logCreator = () => () => {};
            const { options, error, summary } = buildClientOptions(
                { brokers: 'b1:9092, b2:9092', authMechanism: 'scram-sha-256', tls: true, logLevel: 'info' },
                { username: 'alice', password: 'alice-secret' }, { socketFactory, logCreator, logLevel: 5 });
            assert.equal(error, null);
            assert.deepEqual(options.sasl, { mechanism: 'scram-sha-256', username: 'alice', password: 'alice-secret' });
            assert.equal(options.ssl, true);
            assert.equal(options.socketFactory, socketFactory);
            assert.equal(options.logCreator, logCreator);
            assert.equal(options.logLevel, 5);
            assert.equal(summary, 'b1:9092, b2:9092 · SASL/SCRAM-SHA-256 · TLS');
            assert.equal(buildClientOptions({ logLevel: 'info' }, {}).options.logLevel, 4);
            assert.equal(buildClientOptions({ logLevel: 'off' }, {}).options.logLevel, 0);
        });

        it('omits non-function socketFactory/logCreator', function () {
            const { options } = buildClientOptions({}, {}, { socketFactory: 'x', logCreator: null });
            assert.ok(!('socketFactory' in options));
            assert.ok(!('logCreator' in options));
            assert.ok(!('sasl' in options));
        });

        it('returns a ConfigError listing every problem (and still returns options)', function () {
            const { options, error } = buildClientOptions({ brokers: '', authMechanism: 'plain' }, { username: '' });
            assert.ok(error);
            assert.equal(error.name, 'KafkaNcConfigError');
            assert.deepEqual(error.details, ['No brokers configured', 'SASL username is required',
                'SASL password is required']);
            assert.equal(error.message, error.details.join('; '));
            assert.equal(typeof options, 'object');
        });

        it('uses the tls-config node options (stub)', function () {
            const tlsNode = {
                valid: true,
                addTLSOptions(opts) {
                    opts.ca = 'CA';
                    opts.servername = 'kafka.local';
                    opts.rejectUnauthorized = false;
                    return opts;
                }
            };
            const { options, error } = buildClientOptions({ tls: true, tlsConfig: 'tls1' }, {}, { tlsNode });
            assert.equal(error, null);
            assert.deepEqual(options.ssl, { ca: 'CA', servername: 'kafka.local', rejectUnauthorized: false });
        });

        it('coerces a non-boolean rejectUnauthorized from tls-config (default: verify)', function () {
            const make = (v) => ({ valid: true, addTLSOptions: (o) => Object.assign(o, { rejectUnauthorized: v }) });
            const ssl = (v) => buildClientOptions({ tls: true, tlsConfig: 't' }, {}, { tlsNode: make(v) }).options.ssl;
            assert.equal(ssl(undefined).rejectUnauthorized, true);
            assert.equal(ssl('false').rejectUnauthorized, false);
            assert.equal(ssl('true').rejectUnauthorized, true);
            assert.equal(ssl(false).rejectUnauthorized, false);
        });

        it('reports a tls-config node with valid=false (stub)', function () {
            let called = false;
            const tlsNode = { valid: false, addTLSOptions() { called = true; return {}; } };
            const { options, error } = buildClientOptions({ tls: true, tlsConfig: 'tls1' }, {}, { tlsNode });
            assert.deepEqual(error.details, ['TLS configuration is invalid (check its certificate/key files)']);
            assert.equal(called, false);
            assert.equal(options.ssl, true);
        });

        it('reports a missing tls-config node', function () {
            assert.deepEqual(buildClientOptions({ tls: true, tlsConfig: 'gone' }, {}).error.details,
                ['TLS configuration node not found']);
            assert.deepEqual(buildClientOptions({ tls: true, tlsConfig: 'gone' }, {}, { tlsNode: null }).error.details,
                ['TLS configuration node not found']);
        });

        it('reports a throwing addTLSOptions', function () {
            const tlsNode = { valid: true, addTLSOptions() { throw new Error('boom'); } };
            const { error, options } = buildClientOptions({ tls: true, tlsConfig: 't' }, {}, { tlsNode });
            assert.deepEqual(error.details, ['TLS configuration is invalid (boom)']);
            assert.equal(options.ssl, true);
        });

        it('works with the REAL Node-RED tls-config node', function () {
            const good = realTlsConfigNode({ verifyservercert: false, servername: 'kafka.example' }, { cadata: 'CA-PEM' });
            const r = buildClientOptions({ tls: true, tlsConfig: 'tls1' }, {}, { tlsNode: good });
            assert.equal(r.error, null);
            assert.deepEqual(r.options.ssl, { ca: 'CA-PEM', servername: 'kafka.example', rejectUnauthorized: false });

            const broken = realTlsConfigNode({ cert: '/does/not/exist.pem', key: '/does/not/exist.key' });
            assert.equal(broken.valid, false);
            assert.deepEqual(buildClientOptions({ tls: true, tlsConfig: 'tls1' }, {}, { tlsNode: broken }).error.details,
                ['TLS configuration is invalid (check its certificate/key files)']);
        });

        it('never throws for garbage input', function () {
            const hostile = new Proxy({}, { get() { throw new Error('trap'); } });
            for (const [c, cr, x] of [[null, null, null], ['x', 'y', 'z'], [hostile, hostile, {}], [{}, {}, hostile]]) {
                const r = buildClientOptions(c, cr, x);
                assert.ok(r && typeof r.options === 'object');
            }
            assert.ok(buildClientOptions(hostile, {}).error);
        });

        it('produces options real kafkajs accepts even when every field is a string', function () {
            const config = {
                brokers: 'PLAINTEXT://127.0.0.1:1, 127.0.0.2', clientId: 'test', authMechanism: 'none', tls: 'false',
                connectionTimeout: '1000', authenticationTimeout: '1000', requestTimeout: '30000', retries: '0',
                initialRetryTime: '100', maxRetryTime: '1000', logLevel: 'error'
            };
            const { options, error } = buildClientOptions(config, {}, { logCreator: () => () => {} });
            assert.equal(error, null);
            const kafka = new kafkajs.Kafka(options);
            // these threw synchronously with string requestTimeout upstream
            assert.doesNotThrow(() => kafka.producer());
            assert.doesNotThrow(() => kafka.consumer({ groupId: 'g', sessionTimeout: 30000, heartbeatInterval: 3000 }));
            assert.doesNotThrow(() => kafka.admin());
        });
    });

    describe('summarize', function () {
        it('describes brokers, SASL and TLS', function () {
            assert.equal(summarize({ brokers: ['a:1'], mechanism: 'none', tls: false }), 'a:1');
            assert.equal(summarize({ brokers: ['a:1'], mechanism: 'plain', tls: true }), 'a:1 · SASL/PLAIN · TLS');
            assert.equal(summarize({ brokers: ['a:1', 'b:1', 'c:1', 'd:1', 'e:1'], mechanism: 'scram-sha-512', tls: false }),
                'a:1, b:1, c:1 +2 more · SASL/SCRAM-SHA-512');
            assert.equal(summarize({ brokers: [], mechanism: 'none', tls: false }), 'no brokers');
        });
    });
});
