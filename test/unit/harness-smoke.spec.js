'use strict';

/**
 * Smoke tests of the test harness itself:
 * - the node-red-node-test-helper recipe the node tests rely on (SPEC §1);
 * - test/helpers/common.js;
 * - the crash trap of test/helpers/setup.js, proven in a child mocha process
 *   (a deliberately failing test cannot live in this suite).
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFile } = require('node:child_process');
const helper = require('node-red-node-test-helper');
const common = require('../helpers/common');
const setup = require('../helpers/setup');

const { tmpDir, sleep, waitFor, nextStatus, statusCalls, statusMark } = common;

helper.init(require.resolve('node-red'));

const SETUP = path.resolve(__dirname, '../helpers/setup.js');
const COMMON = path.resolve(__dirname, '../helpers/common.js');
const MOCHA = require.resolve('mocha/bin/mocha.js');

/** A tiny node type, defined inline: credentials, input/done, slow close, statuses. */
function smokeNodes(RED) {
    function SmokeNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.seenUserDir = RED.settings.userDir;
        node.seenCredentials = { ...node.credentials };
        node.closedWith = null;
        node.on('input', (msg, send, done) => {
            if (msg.fail) {
                done(new Error('smoke failure'));
                return;
            }
            msg.handledBy = node.id;
            send(msg);
            done();
        });
        node.on('close', (removed, done) => {
            // asynchronous close: helper.unload() must wait for it
            setTimeout(() => {
                node.closedWith = { removed };
                done();
            }, 50);
        });
        // like the palette's supervisors: statuses after construction
        setImmediate(() => {
            node.status({ fill: 'yellow', shape: 'ring', text: 'connecting…' });
            setImmediate(() => node.status({ fill: 'green', shape: 'dot', text: `connected ${node.id}` }));
        });
    }
    RED.nodes.registerType('kafkajs-nc-test-smoke', SmokeNode, {
        credentials: { username: { type: 'text' }, password: { type: 'password' } }
    });
}

const flow = [
    { id: 's1', type: 'kafkajs-nc-test-smoke', name: 'S1', wires: [['h1']] },
    { id: 's2', type: 'kafkajs-nc-test-smoke', name: 'S2', wires: [] },
    { id: 'h1', type: 'helper' },
    { id: 'c1', type: 'catch', scope: null, uncaught: false, wires: [['h2']] },
    { id: 'h2', type: 'helper' }
];
const credentials = { s1: { username: 'alice', password: 's3cret' } };

/** Resolve with the next message a helper node receives. */
function nextMessage(id, timeout = 2000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`no message on ${id} within ${timeout} ms`)), timeout);
        helper.getNode(id).once('input', (msg) => {
            clearTimeout(timer);
            resolve(msg);
        });
    });
}

/** Run mocha in a child process; resolves { code, stdout, stderr }. */
function runMocha(args, cwd) {
    const env = { ...process.env };
    delete env.NODE_OPTIONS;
    return new Promise((resolve) => {
        execFile(process.execPath, [MOCHA, '--no-config', '--no-package', ...args], { cwd, env, timeout: 30000 }, (err, stdout, stderr) => {
            resolve({ code: err ? (typeof err.code === 'number' ? err.code : -1) : 0, stdout, stderr });
        });
    });
}

describe('test harness smoke tests', function () {
    describe('node-red-node-test-helper recipe', function () {
        let userDir;

        beforeEach(function (done) {
            userDir = tmpDir('kafkajs-nc-smoke-');
            helper.settings({ userDir }); // BEFORE helper.load
            // block body: startServer is async, returning its promise AND using done is "overspecified"
            helper.startServer(done);
        });

        afterEach(async function () {
            await helper.unload();
            await helper.stopServer();
        });

        it('loads an inline node type with credentials and RED.settings.userDir', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const s1 = helper.getNode('s1');
            const s2 = helper.getNode('s2');
            assert.deepEqual(s1.credentials, { username: 'alice', password: 's3cret' });
            assert.deepEqual(s1.seenCredentials, { username: 'alice', password: 's3cret' }, 'visible in the constructor');
            assert.deepEqual(s2.credentials, {}, 'no stored credentials => {}');
            assert.equal(s1.seenUserDir, userDir);
            assert.ok(fs.existsSync(userDir));
        });

        it('delivers send() to the wired node', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const received = nextMessage('h1');
            helper.getNode('s1').receive({ payload: 'p' });
            const msg = await received;
            assert.equal(msg.payload, 'p');
            assert.equal(msg.handledBy, 's1');
        });

        it('done(err) reaches a Catch node with the original message', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const caught = nextMessage('h2');
            helper.getNode('s1').receive({ payload: 'p', topic: 't', fail: true });
            const msg = await caught;
            assert.equal(msg.payload, 'p');
            assert.equal(msg.topic, 't');
            assert.equal(msg.error.message, 'Error: smoke failure');
            assert.equal(msg.error.source.id, 's1');
            assert.equal(msg.error.source.type, 'kafkajs-nc-test-smoke');
        });

        it('node.error(text) without a msg is logged but does not reach Catch', async function () {
            await helper.load(smokeNodes, flow, credentials);
            let caught = 0;
            helper.getNode('h2').on('input', () => caught++);
            helper.getNode('s1').error('log only');
            helper.getNode('s1').error('with msg', { payload: 1 });
            await sleep(50);
            assert.equal(caught, 1);
            const logged = helper.log().args.filter((a) => a[0].level === helper.log().ERROR).map((a) => String(a[0].msg));
            assert.ok(logged.includes('log only'));
        });

        it('helper.unload() waits for close(removed, done)', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const s1 = helper.getNode('s1');
            const s2 = helper.getNode('s2');
            const started = Date.now();
            await helper.unload();
            assert.ok(Date.now() - started >= 40, 'the 50 ms close was awaited');
            assert.deepEqual(s1.closedWith, { removed: false });
            assert.deepEqual(s2.closedWith, { removed: false });
        });

        it('helper.setFlows() without a node closes it with removed=true', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const s1 = helper.getNode('s1');
            await helper.setFlows(flow.filter((n) => n.id !== 's1'), 'full');
            assert.deepEqual(s1.closedWith, { removed: true });
        });

        it('statusCalls()/nextStatus() only see the given node (the helper spy is shared by all nodes)', async function () {
            await helper.load(smokeNodes, flow, credentials);
            const s1 = helper.getNode('s1');
            const s2 = helper.getNode('s2');
            assert.equal(s1.status, s2.status, 'one sinon spy on Node.prototype');
            assert.deepEqual(statusCalls(s1), [], 'setImmediate statuses come after helper.load() resolves');
            const status = await nextStatus(s1, (s) => s.fill === 'green');
            assert.equal(status.text, 'connected s1');
            await nextStatus(s2, (s) => s.fill === 'green');
            assert.deepEqual(statusCalls(s1).map((s) => s.text), ['connecting…', 'connected s1']);
            assert.deepEqual(statusCalls(s2).map((s) => s.text), ['connecting…', 'connected s2']);
            assert.equal(s1.status.args.length, 4);
            // the current status matches immediately
            assert.equal((await nextStatus(s1, (s) => s.fill === 'green', 100)).text, 'connected s1');
        });
    });

    describe('common.js', function () {
        describe('tmpDir()', function () {
            it('creates distinct empty directories under os.tmpdir() with a safe prefix', function () {
                const a = tmpDir();
                const b = tmpDir('my prefix/../x-');
                assert.notEqual(a, b);
                for (const dir of [a, b]) {
                    assert.ok(fs.statSync(dir).isDirectory());
                    assert.deepEqual(fs.readdirSync(dir), []);
                    assert.equal(path.dirname(dir), os.tmpdir(), 'the prefix cannot escape os.tmpdir()');
                }
                assert.ok(path.basename(a).startsWith('kafkajs-nc-test-'));
                assert.ok(path.basename(b).startsWith('my_prefix_.._x-'));
            });

            it('cleanupTmpDirs() and the exit fallback remove the directories (child process)', async function () {
                const script = `
                    const fs = require('fs');
                    const common = require(${JSON.stringify(COMMON)});
                    const a = common.tmpDir('kafkajs-nc-cleanup-');
                    fs.mkdirSync(a + '/nested/deep', { recursive: true });
                    fs.writeFileSync(a + '/nested/deep/file.txt', 'x');
                    const result = common.cleanupTmpDirs();
                    const b = common.tmpDir('kafkajs-nc-exit-');
                    console.log(JSON.stringify({ a, b, removed: result.removed, failed: result.failed.length, aExists: fs.existsSync(a) }));
                `;
                const stdout = await new Promise((resolve, reject) => {
                    execFile(process.execPath, ['-e', script], { timeout: 10000 }, (err, out) => (err ? reject(err) : resolve(out)));
                });
                const r = JSON.parse(stdout);
                assert.equal(r.removed, 1);
                assert.equal(r.failed, 0);
                assert.equal(r.aExists, false);
                assert.equal(fs.existsSync(r.b), false, 'removed by the process exit fallback');
            });
        });

        describe('waitFor() and sleep()', function () {
            it('sleep() waits', async function () {
                const t = Date.now();
                await sleep(20);
                assert.ok(Date.now() - t >= 15);
            });

            it('resolves with the first truthy value, synchronously available or async', async function () {
                assert.equal(await waitFor(() => 'now'), 'now');
                let n = 0;
                assert.equal(await waitFor(() => (++n >= 3 ? n : 0), { interval: 1 }), 3);
                let m = 0;
                assert.deepEqual(await waitFor(async () => (++m >= 2 ? { m } : null), { interval: 1 }), { m: 2 });
            });

            it('polls assertions: a throw means "not yet"', async function () {
                let ready = false;
                setTimeout(() => {
                    ready = true;
                }, 20);
                await waitFor(() => {
                    assert.equal(ready, true);
                    return true;
                });
            });

            it('rejects on timeout with the message, attempts and last error as cause', async function () {
                await assert.rejects(
                    waitFor(() => {
                        throw new Error('still red');
                    }, { timeout: 40, interval: 5, message: 'status never turned green' }),
                    (e) => /waitFor timed out after 40 ms \(\d+ attempts\): status never turned green — last error: Error: still red/.test(e.message) &&
                        e.cause instanceof Error && e.cause.message === 'still red'
                );
                await assert.rejects(waitFor(() => false, { timeout: 20 }), /waitFor timed out after 20 ms/);
            });

            it('still times out when fn returns a promise that never settles', async function () {
                const t = Date.now();
                await assert.rejects(waitFor(() => new Promise(() => {}), { timeout: 30 }), /timed out/);
                assert.ok(Date.now() - t < 1000);
            });
        });

        describe('statusCalls() / statusMark() / nextStatus() on plain objects', function () {
            function fakeNode() {
                return { id: 'n1', status() {} };
            }

            it('records calls once installed and normalises string statuses', function () {
                const node = fakeNode();
                assert.deepEqual(statusCalls(node), []);
                node.status({ fill: 'red', shape: 'ring', text: 'down' });
                node.status('plain text');
                node.status({});
                assert.deepEqual(statusCalls(node), [{ fill: 'red', shape: 'ring', text: 'down' }, { text: 'plain text' }, {}]);
                assert.equal(statusMark(node), 3);
                const bare = { id: 'n2' };
                assert.deepEqual(statusCalls(bare), []);
                bare.status({ text: 'x' });
                assert.deepEqual(statusCalls(bare), [{ text: 'x' }]);
            });

            it('waits until the current or a later status matches', async function () {
                const node = fakeNode();
                statusCalls(node);
                node.status({ fill: 'green', text: 'connected' });
                assert.equal((await nextStatus(node, (s) => s.fill === 'green', 100)).text, 'connected', 'current status');
                node.status({ fill: 'yellow', text: 'waiting' });
                const p = nextStatus(node, (s) => s.fill === 'green', 500);
                setTimeout(() => node.status({ fill: 'green', text: 'connected again' }), 10);
                assert.equal((await p).text, 'connected again', 'an older matching status is ignored');
            });

            it('since: statusMark() ignores the current status; since: 0 includes the history', async function () {
                const node = fakeNode();
                statusCalls(node);
                node.status({ fill: 'green', text: 'a' });
                node.status({ fill: 'yellow', text: 'b' });
                assert.equal((await nextStatus(node, (s) => s.fill === 'green', { since: 0, timeout: 100 })).text, 'a');
                const mark = statusMark(node);
                await assert.rejects(nextStatus(node, (s) => s.fill === 'yellow', { since: mark, timeout: 30 }), /no status of node n1 matched/);
            });

            it('explains a timeout, including earlier matches', async function () {
                const node = fakeNode();
                statusCalls(node);
                node.status({ fill: 'green', text: 'old' });
                node.status({ fill: 'red', text: 'now' });
                await assert.rejects(nextStatus(node, (s) => s.fill === 'green', 30), (e) => /within 30 ms/.test(e.message) &&
                    /"now"/.test(e.message) && /1 earlier status call\(s\) matched — pass \{ since: 0 \}/.test(e.message));
            });
        });
    });

    describe('setup.js crash trap', function () {
        this.timeout(60000);

        const FIXTURE = `
'use strict';
const assert = require('node:assert/strict');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

describe('trap', function () {
    it('sync test with an unhandled rejection', function () {
        Promise.reject(new Error('trap-me-sync'));
    });
    it('clean test after a trapped one', function () {});
    it('async test with a rejection from a timer', async function () {
        setTimeout(() => { Promise.reject(new Error('trap-me-timer')); }, 5);
        await sleep(40);
    });
    it('callback test with an unhandled rejection', function (done) {
        Promise.reject(new Error('trap-me-callback'));
        setTimeout(done, 20);
    });
    it('failing assertion plus an unhandled rejection', async function () {
        Promise.reject(new Error('trap-me-too'));
        await sleep(5);
        assert.equal(1, 2);
    });
    // never calls done: mocha's own uncaught handler completes (fails) the test
    it('uncaught exception from a timer (reported by mocha itself)', function (done) {
        setTimeout(() => { throw new Error('trap-me-uncaught'); }, 5);
    });
    it('clean test after an uncaught exception', function () {});
    it('handled rejection is fine', async function () {
        await assert.rejects(Promise.reject(new Error('handled')));
    });
    it('skipped test', function () { this.skip(); });
});

describe('trap with a timed-out test', function () {
    it('times out, its body settles later', async function () {
        this.timeout(30);
        await sleep(80);
    });
    it('rejection while the timed-out body still runs', async function () {
        await sleep(20);
        Promise.reject(new Error('trap-me-after-timeout'));
        await sleep(100); // the timed-out body settles meanwhile: it must not take this record
    });
});

describe('trap in hooks', function () {
    afterEach(function () {
        Promise.reject(new Error('trap-me-in-afterEach'));
    });
    it('clean body, rejection in afterEach', function () {});
});
`;

        function failureTitles(report) {
            return report.failures.map((f) => f.fullTitle);
        }

        function failureFor(report, fragment) {
            return report.failures.find((f) => f.fullTitle.includes(fragment));
        }

        it('fails exactly the tests that leaked an error and keeps running the others', async function () {
            const dir = tmpDir('kafkajs-nc-trap-');
            fs.writeFileSync(path.join(dir, 'trap.fixture.js'), FIXTURE);
            const { stdout, code } = await runMocha(['--require', SETUP, '--reporter', 'json', '--timeout', '5000', '--exit', 'trap.fixture.js'], dir);
            const report = JSON.parse(stdout);

            assert.deepEqual(report.passes.map((t) => t.title).sort(), [
                'clean test after a trapped one',
                'clean test after an uncaught exception',
                'handled rejection is fine',
                // its body passed; the error that came after it fails the root afterEach hook below
                'clean body, rejection in afterEach'
            ].sort());
            assert.deepEqual(report.pending.map((t) => t.title), ['skipped test']);
            assert.deepEqual(failureTitles(report).sort(), [
                'trap async test with a rejection from a timer',
                'trap callback test with an unhandled rejection',
                'trap failing assertion plus an unhandled rejection',
                'trap sync test with an unhandled rejection',
                'trap uncaught exception from a timer (reported by mocha itself)',
                'trap with a timed-out test times out, its body settles later',
                'trap with a timed-out test rejection while the timed-out body still runs',
                // the failing hook is the ROOT afterEach of setup.js
                '"after each" hook: afterEach for "clean body, rejection in afterEach"'
            ].sort());
            assert.equal(code, report.failures.length, 'exit code = number of failures');

            const sync = failureFor(report, 'sync test with an unhandled rejection');
            assert.match(sync.err.message, /1 unhandled error\(s\) detected/);
            assert.match(sync.err.message, /unhandledRejection during test "trap sync test with an unhandled rejection": Error: trap-me-sync/);
            assert.match(failureFor(report, 'rejection from a timer').err.message, /trap-me-timer/);
            assert.match(failureFor(report, 'callback test').err.message, /trap-me-callback/);
            const both = failureFor(report, 'failing assertion plus').err.message;
            assert.match(both, /^Test failed: AssertionError/);
            assert.match(both, /trap-me-too/);
            assert.match(failureFor(report, 'uncaught exception from a timer').err.message, /trap-me-uncaught/);
            assert.match(failureFor(report, 'times out').err.message, /Timeout of 30ms exceeded/);
            assert.match(failureFor(report, 'timed-out body still runs').err.message,
                /during test "trap with a timed-out test rejection while the timed-out body still runs": Error: trap-me-after-timeout/);
            const hook = failureFor(report, '"after each" hook');
            assert.match(hook.err.message, /trap-me-in-afterEach/);
            assert.match(hook.err.message, /after test "trap in hooks clean body, rejection in afterEach" finished/);
        });

        it('without setup.js mocha would let the same unhandled rejection pass', async function () {
            const dir = tmpDir('kafkajs-nc-trap-');
            fs.writeFileSync(path.join(dir, 'plain.fixture.js'), `
                describe('plain', function () {
                    it('unhandled rejection', function () { Promise.reject(new Error('silently ignored')); });
                });
            `);
            const { stdout } = await runMocha(['--reporter', 'json', '--exit', 'plain.fixture.js'], dir);
            const report = JSON.parse(stdout);
            assert.equal(report.passes.length, 1, 'this is why the root hooks exist');
            assert.equal(report.failures.length, 0);
        });

        it('is installed in this run', function () {
            assert.equal(setup._internal.isInstalled(), true);
            assert.ok(process.listeners('unhandledRejection').length >= 1);
        });

        it('buildTrapError() keeps the test failure as cause and its diff fields', function () {
            const failure = new assert.AssertionError({ actual: 1, expected: 2, operator: 'strictEqual' });
            const records = [
                { kind: 'unhandledRejection', error: new Error('boom'), where: 'during test "x"' },
                { kind: 'unhandledRejection', error: 'a string', where: 'after test "x" finished' }
            ];
            const err = setup._internal.buildTrapError(records, failure);
            assert.equal(err.name, 'CrashTrapError');
            assert.equal(err.cause, failure);
            assert.equal(err.actual, 1);
            assert.equal(err.expected, 2);
            assert.match(err.message, /^Test failed: AssertionError/);
            assert.match(err.message, /2 unhandled error\(s\) detected/);
            assert.match(err.message, /1\) unhandledRejection during test "x": Error: boom/);
            assert.match(err.message, /2\) unhandledRejection after test "x" finished: string a string/);
            assert.deepEqual(err.records, records);
            assert.doesNotMatch(err.message, /\n\s+at /, 'frames are in the stack, not in the message');
            assert.match(err.stack, /^CrashTrapError: Test failed/);
            assert.match(err.stack, /\n {4}1\) Error: boom\n {8}at /);
            const alone = setup._internal.buildTrapError([records[0]]);
            assert.equal(alone.cause, undefined);
            assert.doesNotMatch(alone.message, /Test failed/);
        });

        it('wraps each test only once', function () {
            const test = this.test;
            const wrapped = test.fn;
            setup._internal.wrapTest(test);
            assert.equal(test.fn, wrapped);
        });
    });
});
