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
const KAFKAJS_DIR = path.dirname(require.resolve('kafkajs/package.json'));

// node-red/red.js: Node-RED's uncaughtException handler (it reads err.stack without a null check)
const NODE_RED_HANDLER = `
    process.on('uncaughtException', function (err, origin) {
        console.log('[red] Uncaught Exception: origin=' + origin);
        if (err.stack) console.log('STACK ' + err.stack.split('\\n')[0]);
        else console.log('NO STACK ' + err);
        process.exit(1);
    });`;

function fakeRED() {
    const warnings = [];
    return { warnings, log: { warn: (t) => warnings.push(t) } };
}

function kafkaError() {
    const cause = new K.KafkaJSConnectionError('Connection error: connect ECONNREFUSED 127.0.0.1:9092',
        { broker: '127.0.0.1:9092', code: 'ECONNREFUSED' });
    return new K.KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 100 });
}

/** Run a script in a child Node process; resolves { code, stdout, stderr, ms }. */
function runChild(script, nodeArgs = []) {
    const env = { ...process.env };
    delete env.NODE_OPTIONS; // the child must run with Node's default rejection mode
    const start = Date.now();
    return new Promise((resolve) => {
        execFile(process.execPath, [...nodeArgs, '-e', script], { timeout: 10000, env }, (err, stdout, stderr) => {
            resolve({ code: err ? (typeof err.code === 'number' ? err.code : 1) : 0, stdout, stderr, ms: Date.now() - start });
        });
    });
}

/** Output of a child run without what differs between runs (pids, rejection ids, stack frames). */
function normalized(r) {
    const clean = (t) => t.replace(/\(node:\d+\)/g, '(node:N)').replace(/rejection id: \d+/g, 'rejection id: N')
        .split('\n').filter((l) => !/^\s+at /.test(l) && !l.startsWith('WARN ')).join('\n');
    return `exit ${r.code}\n${clean(r.stdout)}\n${clean(r.stderr)}`;
}

describe('lib/safety-net', function () {
    beforeEach(function () {
        safetyNet._resetDedupe();
    });

    describe('acquire / release', function () {
        const ours = () => process.listeners('unhandledRejection').filter((l) => l === safetyNet._processListener).length;
        let savedGrace;
        before(function () {
            savedGrace = safetyNet._setReleaseGrace(0); // release at once; the grace period is tested below
        });
        after(function () {
            safetyNet._setReleaseGrace(savedGrace);
        });

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

        it('stays installed for a grace period after the last release: client nodes are still closing (CRASH-2)', async function () {
            if (safetyNet.references() > 0) this.skip(); // another suite holds a reference
            assert.equal(safetyNet.RELEASE_GRACE_MS, 30000, 'longer than the clients\' close budget (10 s)');
            safetyNet._setReleaseGrace(150);
            try {
                const RED = fakeRED();
                safetyNet.acquire(RED)();
                assert.equal(ours(), 1, 'still installed right after the last release');
                assert.equal(safetyNet.handleRejection(kafkaError(), 1), 'swallowed');
                assert.equal(RED.warnings.length, 1, 'logged through the RED of the released reference');
                await new Promise((r) => setTimeout(r, 250));
                assert.equal(ours(), 0, 'removed once the grace period is over');

                // a new reference within the grace period (redeploy) keeps it installed
                safetyNet.acquire(RED)();
                const again = safetyNet.acquire(RED);
                await new Promise((r) => setTimeout(r, 250));
                assert.equal(ours(), 1);
                again();
            } finally {
                safetyNet._setReleaseGrace(0); // also removes it now
            }
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
            byStack.stack = `TypeError: x\n    at Runner.start (${path.join(KAFKAJS_DIR, 'src', 'consumer', 'runner.js')}:1:1)`;
            assert.equal(safetyNet.handleRejection(named, 1, RED, 'throw'), 'swallowed');
            assert.equal(safetyNet.handleRejection(byStack, 1, RED, 'throw'), 'swallowed');
            assert.equal(RED.warnings.length, 2);
        });

        it('does not swallow errors of other nodes that merely live under a folder named "kafkajs" (SAFETY-1)', function () {
            const RED = fakeRED();
            const other = new TypeError("Cannot read properties of undefined (reading 'toUpperCase')");
            other.stack = 'TypeError: x\n    at Function.x (/home/pi/kafkajs/.node-red/node_modules/node-red-contrib-foo/foo.js:1:1)';
            assert.equal(safetyNet.isPaletteError(other), false);
            assert.equal(safetyNet.handleRejection(other, 1, RED, 'throw'), 'hand-back');
            assert.equal(RED.warnings.length, 0);
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

        it('hands unrelated rejections back to Node when it is the only listener and Node would act on them', function () {
            // throw: Node crashes; warn-with-error-code: Node warns and sets the exit code (CRASH-5)
            for (const mode of ['throw', 'warn-with-error-code']) {
                for (const reason of [new Error('not ours'), 'a string', undefined, null, { a: 1 }]) {
                    assert.equal(safetyNet.handleRejection(reason, 1, fakeRED(), mode), 'hand-back', `${mode} ${reason}`);
                }
            }
        });

        it('leaves unrelated rejections alone when the outcome does not depend on listeners (warn, none, strict)', function () {
            // warn always warns, none never does anything, strict raises before any listener runs (CRASH-6)
            for (const mode of ['warn', 'none', 'strict']) {
                assert.equal(safetyNet.handleRejection(new Error('not ours'), 1, fakeRED(), mode), 'ignored', mode);
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
                process.execArgv = ['--unhandled-rejections', 'none'];
                assert.equal(safetyNet.unhandledRejectionsMode(), 'none', 'the value may follow as a separate argument');
            } finally {
                process.execArgv = savedArgv;
                if (savedOptions === undefined) delete process.env.NODE_OPTIONS;
                else process.env.NODE_OPTIONS = savedOptions;
            }
        });

        it('leaves unrelated errors to other listeners', function () {
            const RED = fakeRED();
            assert.equal(safetyNet.handleRejection(new Error('not ours'), 2, RED, 'throw'), 'ignored');
            assert.equal(safetyNet.handleRejection(new TypeError('not ours'), 3, RED, 'warn-with-error-code'), 'ignored');
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
                process.on('uncaughtException', (err, origin) => {
                    process.stdout.write('UNCAUGHT ' + err.message + ' origin=' + origin + '\\n');
                    process.exit(1);
                });
                net.acquire(RED);
                Promise.reject(new Error('plain boom'));
                setTimeout(() => process.exit(0), 200);
            `);
            assert.equal(r.code, 1);
            assert.match(r.stdout, /UNCAUGHT plain boom origin=unhandledRejection/);
        });

        it('a rejection without an Error reaches Node-RED\'s handler wrapped like Node does it (CRASH-4)', async function () {
            for (const reason of ['', 'null', "'a string'"]) {
                const r = await runChild(`${prelude}
                    ${NODE_RED_HANDLER}
                    net.acquire(RED);
                    Promise.reject(${reason});
                    setTimeout(() => process.exit(0), 200);
                `);
                // with a raw re-throw of undefined, red.js' handler itself threw on err.stack (exit 7)
                assert.equal(r.code, 1, `Promise.reject(${reason}): ${r.stdout}${r.stderr}`);
                assert.match(r.stdout, /\[red\] Uncaught Exception: origin=unhandledRejection/);
                assert.match(r.stdout, /STACK UnhandledPromiseRejection: This error originated either by throwing/);
            }
        });

        it('behaves exactly like Node without the net for rejections that are not ours (CRASH-4, CRASH-5)', async function () {
            this.timeout(60000);
            for (const mode of ['throw', 'warn-with-error-code', 'warn', 'none']) {
                for (const reason of ["new Error('foreign bug')", '', "'a string'", '{ a: 1 }']) {
                    for (const handler of [NODE_RED_HANDLER, '']) {
                        const script = (withNet) => `${prelude}
                            ${handler}
                            ${withNet ? 'net.acquire(RED);' : ''}
                            Promise.reject(${reason});
                            setTimeout(() => console.log('alive, exitCode ' + process.exitCode), 100);`;
                        const args = [`--unhandled-rejections=${mode}`];
                        const [without, withNet] = await Promise.all([runChild(script(false), args), runChild(script(true), args)]);
                        const what = `mode ${mode}, reason ${reason || 'undefined'}, ${handler ? 'Node-RED handler' : 'no handler'}`;
                        // Without a handler Node prints the source line of the crash above the error; for the
                        // re-raised rejection it points into Node itself. Everything else must be the same.
                        const noSourceHint = (r) => ({ ...r, stderr: r.stderr.replace(/^[\s\S]*?\n\s*\^+\s*\n/, '') });
                        assert.equal(normalized(noSourceHint(withNet)), normalized(noSourceHint(without)), what);
                    }
                }
            }
        });

        it('with --unhandled-rejections=warn-with-error-code another node\'s rejection still warns and sets the exit code (CRASH-5)',
            async function () {
                const r = await runChild(`${prelude}
                    net.acquire(RED);
                    Promise.reject(new Error('foreign palette bug'));
                `, ['--unhandled-rejections=warn-with-error-code']);
                assert.equal(r.code, 1, r.stderr);
                assert.match(r.stderr, /UnhandledPromiseRejectionWarning: Error: foreign palette bug/);
            });

        it('is not installed with --unhandled-rejections=strict (it could not help) and says so (CRASH-6)', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED);
                net.acquire(RED);
                console.log('installed=' + net.isInstalled() + ' listeners=' + process.listenerCount('unhandledRejection'));
            `, ['--unhandled-rejections=strict']);
            assert.equal(r.code, 0, r.stderr);
            assert.match(r.stdout, /installed=false listeners=0/);
            assert.equal(r.stdout.match(/WARN \[kafkajs-nc\] The crash safety net is not active: with --unhandled-rejections=strict/g)
                .length, 1, 'warned once');
        });

        it('with --unhandled-rejections=warn a plain Error does not crash (as without the safety net)', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED);
                Promise.reject(new Error('plain boom'));
                setTimeout(() => process.exit(0), 200);
            `, ['--unhandled-rejections=warn']);
            assert.equal(r.code, 0, r.stderr);
        });

        it('a KafkaJS rejection right after the last release (clients still closing) is still caught (CRASH-2)',
            async function () {
                const r = await runChild(`${prelude}
                    const release = net.acquire(RED);
                    release();
                    setTimeout(() => {
                        const kafkaLike = new Error('kafka-like'); kafkaLike.name = 'KafkaJSConnectionError';
                        Promise.reject(kafkaLike);
                    }, 100);
                    setTimeout(() => process.exit(0), 300);
                `);
                assert.equal(r.code, 0, r.stderr);
                assert.match(r.stdout, /Prevented a crash/);
            });

        it('after the grace period a KafkaJS rejection crashes again (listener removed)', async function () {
            const r = await runChild(`${prelude}
                net._setReleaseGrace(50);
                const release = net.acquire(RED);
                release();
                setTimeout(() => {
                    const kafkaLike = new Error('kafka-like'); kafkaLike.name = 'KafkaJSConnectionError';
                    Promise.reject(kafkaLike);
                }, 150);
                setTimeout(() => process.exit(0), 400);
            `);
            assert.notEqual(r.code, 0);
        });

        it('the grace period never keeps the process alive', async function () {
            const r = await runChild(`${prelude}
                net.acquire(RED)();
            `);
            assert.equal(r.code, 0, r.stderr);
            assert.ok(r.ms < 5000, `exited after ${r.ms} ms`);
        });
    });
});
