'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const { execFile } = require('node:child_process');
const K = require('kafkajs/src/errors');
const safetyNet = require('../../lib/safety-net');
const { MessageError } = require('../../lib/errors');
const registry = require('../../lib/queue-registry');

const SAFETY_NET = path.resolve(__dirname, '../../lib/safety-net.js');
const KAFKAJS = require.resolve('kafkajs');

function fakeRED() {
    const warnings = [];
    return { warnings, log: { warn: (t) => warnings.push(t) } };
}

function kafkaError() {
    const cause = new K.KafkaJSConnectionError('Connection error: connect ECONNREFUSED 127.0.0.1:9092',
        { broker: '127.0.0.1:9092', code: 'ECONNREFUSED' });
    return new K.KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 100 });
}

/** Run a script in a child Node process; resolves { code, stdout, stderr }. */
function runChild(script, nodeArgs = []) {
    const env = { ...process.env };
    delete env.NODE_OPTIONS; // the child must run with Node's default rejection mode
    return new Promise((resolve) => {
        execFile(process.execPath, [...nodeArgs, '-e', script], { timeout: 10000, env }, (err, stdout, stderr) => {
            resolve({ code: err ? (typeof err.code === 'number' ? err.code : 1) : 0, stdout, stderr });
        });
    });
}

describe('lib/safety-net', function () {
    beforeEach(function () {
        safetyNet._resetDedupe();
    });

    describe('acquire / release', function () {
        const ours = () => process.listeners('unhandledRejection').filter((l) => l === safetyNet._processListener).length;

        // Other suites in the same mocha run (node tests) may hold references, so
        // everything is asserted relative to the starting reference count.
        it('installs one listener on first acquire and removes it on last release', function () {
            const base = safetyNet.references();
            assert.equal(ours(), base > 0 ? 1 : 0);
            const r1 = safetyNet.acquire(fakeRED());
            assert.equal(ours(), 1);
            const r2 = safetyNet.acquire(fakeRED());
            assert.equal(ours(), 1, 'only one listener for several references');
            assert.equal(safetyNet.references(), base + 2);
            r1();
            r1(); // idempotent: must not release r2's reference
            assert.equal(safetyNet.references(), base + 1);
            assert.equal(ours(), 1);
            assert.equal(safetyNet.isInstalled(), true);
            r2();
            assert.equal(safetyNet.references(), base);
            assert.equal(ours(), base > 0 ? 1 : 0);
            assert.equal(safetyNet.isInstalled(), base > 0);
            r2();
            assert.equal(safetyNet.references(), base, 'a double release changes nothing');
        });

        it('can be installed again after a full release', function () {
            if (safetyNet.references() > 0) this.skip(); // another suite holds a reference
            const r = safetyNet.acquire(fakeRED());
            assert.equal(ours(), 1);
            r();
            assert.equal(ours(), 0);
            const again = safetyNet.acquire(null);
            assert.equal(ours(), 1);
            again();
            assert.equal(ours(), 0);
        });
    });

    describe('handleRejection (decision logic)', function () {
        it('swallows and logs KafkaJS errors, whatever the listener count', function () {
            const RED = fakeRED();
            for (const count of [1, 2, 5]) {
                safetyNet._resetDedupe();
                assert.doesNotThrow(() => safetyNet.handleRejection(kafkaError(), count, RED));
            }
            assert.equal(RED.warnings.length, 3);
            assert.match(RED.warnings[0], /^\[kafkajs-nc\] Prevented a crash: unhandled KafkaJS error: KafkaJSNumberOfRetriesExceeded/);
            assert.match(RED.warnings[0], /ECONNREFUSED/);
        });

        it('recognises kafka-like errors by name and by kafkajs stack frames', function () {
            const RED = fakeRED();
            const named = new Error('kafka-like');
            named.name = 'KafkaJSConnectionError';
            const byStack = new TypeError('x');
            byStack.stack = 'TypeError: x\n    at Runner.start (/app/node_modules/kafkajs/src/consumer/runner.js:1:1)';
            assert.doesNotThrow(() => safetyNet.handleRejection(named, 1, RED));
            assert.doesNotThrow(() => safetyNet.handleRejection(byStack, 1, RED));
            assert.equal(RED.warnings.length, 2);
        });

        it('swallows errors of this palette (own error classes and errors thrown in lib/)', function () {
            const RED = fakeRED();
            assert.doesNotThrow(() => safetyNet.handleRejection(new MessageError('bad'), 1, RED));
            let fromLib;
            const release = registry.acquire('/tmp/safety-net-spec-dir', 'a');
            try {
                registry.acquire('/tmp/safety-net-spec-dir', 'b');
            } catch (e) {
                fromLib = e;
            } finally {
                release();
            }
            assert.ok(fromLib, 'queue-registry threw');
            assert.equal(safetyNet.isPaletteError(fromLib), true);
            assert.doesNotThrow(() => safetyNet.handleRejection(fromLib, 1, RED));
            assert.equal(RED.warnings.length, 2);
            assert.match(RED.warnings[0], /unhandled error in kafkajs-nc nodes/);
        });

        it('re-throws unrelated errors when it is the only listener', function () {
            const plain = new Error('not ours');
            assert.throws(() => safetyNet.handleRejection(plain, 1, fakeRED(), 'throw'), (e) => e === plain);
            assert.throws(() => safetyNet.handleRejection(plain, 1, fakeRED(), 'strict'), (e) => e === plain);
            assert.throws(() => safetyNet.handleRejection('a string', 1, fakeRED(), 'throw'), (e) => e === 'a string');
            assert.throws(() => safetyNet.handleRejection(undefined, 1, fakeRED(), 'throw'), (e) => e === undefined);
        });

        it('does not re-throw when Node itself would not crash (--unhandled-rejections=warn|none)', function () {
            for (const mode of ['warn', 'none', 'warn-with-error-code']) {
                assert.doesNotThrow(() => safetyNet.handleRejection(new Error('not ours'), 1, fakeRED(), mode), mode);
            }
        });

        it('detects the --unhandled-rejections mode (default throw)', function () {
            const savedArgv = process.execArgv;
            const savedOptions = process.env.NODE_OPTIONS;
            try {
                Object.defineProperty(process, 'execArgv', { value: [], configurable: true, writable: true });
                delete process.env.NODE_OPTIONS;
                assert.equal(safetyNet.unhandledRejectionsMode(), 'throw');
                process.env.NODE_OPTIONS = '--max-old-space-size=100 --unhandled-rejections=warn';
                assert.equal(safetyNet.unhandledRejectionsMode(), 'warn');
                process.execArgv = ['--unhandled-rejections=strict'];
                assert.equal(safetyNet.unhandledRejectionsMode(), 'strict', 'command line wins over NODE_OPTIONS');
            } finally {
                process.execArgv = savedArgv;
                if (savedOptions === undefined) delete process.env.NODE_OPTIONS;
                else process.env.NODE_OPTIONS = savedOptions;
            }
        });

        it('leaves unrelated errors to other listeners', function () {
            const RED = fakeRED();
            assert.doesNotThrow(() => safetyNet.handleRejection(new Error('not ours'), 2, RED, 'throw'));
            assert.doesNotThrow(() => safetyNet.handleRejection(new TypeError('not ours'), 3, RED, 'throw'));
            assert.equal(RED.warnings.length, 0, 'nothing logged for errors that are not ours');
        });

        it('de-duplicates identical warnings for 60 s', function () {
            const RED = fakeRED();
            const realNow = Date.now;
            let now = 5000000;
            Date.now = () => now;
            try {
                for (let i = 0; i < 10; i++) safetyNet.handleRejection(kafkaError(), 1, RED);
                assert.equal(RED.warnings.length, 1);
                now += 60001;
                safetyNet.handleRejection(kafkaError(), 1, RED);
                assert.equal(RED.warnings.length, 2);
            } finally {
                Date.now = realNow;
            }
        });

        it('never throws for our errors even if logging fails or RED is missing', function () {
            const broken = { log: { warn() { throw new Error('log broken'); } } };
            assert.doesNotThrow(() => safetyNet.handleRejection(kafkaError(), 1, broken));
            assert.doesNotThrow(() => safetyNet.handleRejection(kafkaError(), 1, undefined));
            assert.doesNotThrow(() => safetyNet.handleRejection(kafkaError(), 1, { log: {} }));
        });

        it('isPaletteError never throws', function () {
            assert.equal(safetyNet.isPaletteError(new Proxy({}, { get() { throw new Error('trap'); } })), false);
            assert.equal(safetyNet.isPaletteError(null), false);
            assert.equal(safetyNet.isPaletteError(new Error('plain')), false);
        });
    });

    describe('end-to-end in a child process', function () {
        this.timeout(20000);

        const prelude = `
            const net = require(${JSON.stringify(SAFETY_NET)});
            const K = require(${JSON.stringify(KAFKAJS)});
            const RED = { log: { warn: (t) => process.stdout.write('WARN ' + t + '\\n') } };
        `;

        it('a KafkaJS rejection does not crash the process', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED);
                Promise.reject(new K.KafkaJSNumberOfRetriesExceeded(
                    new K.KafkaJSConnectionError('Connection error: connect ECONNREFUSED 127.0.0.1:9', { code: 'ECONNREFUSED' }),
                    { retryCount: 5, retryTime: 1 }));
                const kafkaLike = new Error('kafka-like'); kafkaLike.name = 'KafkaJSConnectionError';
                Promise.reject(kafkaLike);
                setTimeout(() => process.exit(0), 200);
            `);
            assert.equal(r.code, 0, `stderr: ${r.stderr}`);
            assert.match(r.stdout, /WARN \[kafkajs-nc\] Prevented a crash: unhandled KafkaJS error/);
            assert.match(r.stdout, /kafka-like/);
        });

        it('baseline: without the safety net the same rejection kills the process', async function () {
            const r = await runChild(`${prelude}
                const kafkaLike = new Error('kafka-like'); kafkaLike.name = 'KafkaJSConnectionError';
                Promise.reject(kafkaLike);
                setTimeout(() => process.exit(0), 200);
            `);
            assert.notEqual(r.code, 0);
        });

        it('a plain Error rejection still crashes the process (Node default preserved)', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED);
                Promise.reject(new Error('plain boom'));
                setTimeout(() => process.exit(0), 200);
            `);
            assert.notEqual(r.code, 0);
            assert.match(r.stderr, /plain boom/);
            assert.doesNotMatch(r.stdout, /Prevented a crash/);
        });

        it('a plain Error reaches a Node-RED style uncaughtException handler', async function () {
            const r = await runChild(`${prelude}
                process.on('uncaughtException', (err) => {
                    process.stdout.write('UNCAUGHT ' + err.message + '\\n');
                    process.exit(1);
                });
                net.acquire(RED);
                Promise.reject(new Error('plain boom'));
                setTimeout(() => process.exit(0), 200);
            `);
            assert.equal(r.code, 1);
            assert.match(r.stdout, /UNCAUGHT plain boom/);
        });

        it('with --unhandled-rejections=warn a plain Error does not crash (as without the safety net)', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED);
                Promise.reject(new Error('plain boom'));
                setTimeout(() => process.exit(0), 200);
            `, ['--unhandled-rejections=warn']);
            assert.equal(r.code, 0, r.stderr);
        });

        it('after the last release a KafkaJS rejection crashes again (listener removed)', async function () {
            const r = await runChild(`${prelude}
                const release = net.acquire(RED);
                release();
                const kafkaLike = new Error('kafka-like'); kafkaLike.name = 'KafkaJSConnectionError';
                Promise.reject(kafkaLike);
                setTimeout(() => process.exit(0), 200);
            `);
            assert.notEqual(r.code, 0);
        });
    });
});
