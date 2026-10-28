'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const util = require('../../lib/util');
const { tmpDir } = require('../helpers/common');

describe('lib/util', function () {
    describe('toNumber / toInt', function () {
        it('parses numbers and numeric strings', function () {
            assert.equal(util.toNumber(5, 1), 5);
            assert.equal(util.toNumber('5', 1), 5);
            assert.equal(util.toNumber(' 2.5 ', 1), 2.5);
            assert.equal(util.toNumber('1e3', 1), 1000);
            assert.equal(util.toNumber(10n, 1), 10);
            assert.equal(util.toInt('2.9', 1), 2);
            assert.equal(util.toInt(-2.9, 1), -2);
            assert.equal(util.toInt('30000', 1), 30000);
        });

        it('returns the default for unusable input', function () {
            for (const v of [undefined, null, '', '   ', 'abc', NaN, Infinity, -Infinity, '1e400', true, false,
                {}, [], [5], () => 1, Symbol('x'), '${X}']) {
                assert.equal(util.toNumber(v, 42), 42, `toNumber(${String(v)})`);
                assert.equal(util.toInt(v, 42), 42, `toInt(${String(v)})`);
            }
            assert.equal(util.toInt(undefined, undefined), undefined);
        });

        it('clamps to min/max but never clamps the default', function () {
            assert.equal(util.toInt('50', 1, { min: 100, max: 200 }), 100);
            assert.equal(util.toInt('500', 1, { min: 100, max: 200 }), 200);
            assert.equal(util.toInt('150.7', 1, { min: 100, max: 200 }), 150);
            assert.equal(util.toInt('x', 1, { min: 100, max: 200 }), 1);
            assert.equal(util.toNumber(0.5, 1, { min: 0, max: 1 }), 0.5);
            assert.equal(util.toNumber(-3, 1, { min: 0 }), 0);
        });

        it('never returns -0 and never throws on hostile objects', function () {
            assert.ok(Object.is(util.toInt(-0.4, 1), 0));
            const hostile = { valueOf() { throw new Error('boom'); }, toString() { throw new Error('boom'); } };
            assert.equal(util.toNumber(hostile, 7), 7);
            assert.equal(util.toInt(hostile, 7), 7);
        });
    });

    describe('toBool', function () {
        it('recognises true values', function () {
            for (const v of [true, 'true', 'TRUE', ' on ', 1, '1', 'yes']) assert.equal(util.toBool(v, false), true, String(v));
        });
        it('recognises false values', function () {
            for (const v of [false, 'false', 'False', 0, '0', '', 'off', 'no']) assert.equal(util.toBool(v, true), false, String(v));
        });
        it('returns the default otherwise', function () {
            for (const v of [undefined, null, 'maybe', 2, {}, []]) assert.equal(util.toBool(v, 'def'), 'def', String(v));
        });
    });

    describe('once', function () {
        it('calls the function at most once and keeps its result', function () {
            let calls = 0;
            const f = util.once((a) => { calls++; return a * 2; });
            assert.equal(f(2), 4);
            assert.equal(f(5), 4);
            assert.equal(calls, 1);
        });
        it('keeps `this`', function () {
            const obj = { v: 3, f: util.once(function () { return this.v; }) };
            assert.equal(obj.f(), 3);
        });
    });

    describe('safeOn', function () {
        it('swallows a synchronous throw and reports it', function () {
            const em = new EventEmitter();
            const errors = [];
            util.safeOn(em, 'x', () => { throw new Error('sync boom'); }, (e) => errors.push(e));
            assert.doesNotThrow(() => em.emit('x', 1));
            assert.equal(errors.length, 1);
            assert.equal(errors[0].message, 'sync boom');
        });

        it('reports an async rejection instead of leaving it unhandled', async function () {
            const em = new EventEmitter();
            const errors = [];
            util.safeOn(em, 'x', async () => { throw new Error('async boom'); }, (e) => errors.push(e));
            em.emit('x');
            await new Promise((r) => setImmediate(r));
            assert.equal(errors.length, 1);
            assert.equal(errors[0].message, 'async boom');
        });

        it('handles a thenable whose rejection handler is called', async function () {
            const em = new EventEmitter();
            const errors = [];
            const thenable = { then(ok, fail) { fail(new Error('thenable boom')); } };
            util.safeOn(em, 'x', () => thenable, (e) => errors.push(e));
            em.emit('x');
            assert.equal(errors.length, 1);
        });

        it('passes the event arguments and `this`', function () {
            const em = new EventEmitter();
            let seen;
            util.safeOn(em, 'x', function (a, b) { seen = [this, a, b]; });
            em.emit('x', 1, 2);
            assert.deepEqual(seen, [em, 1, 2]);
        });

        it('survives a throwing onError and a missing onError', function () {
            const em = new EventEmitter();
            util.safeOn(em, 'x', () => { throw new Error('a'); }, () => { throw new Error('b'); });
            util.safeOn(em, 'y', () => { throw new Error('a'); });
            assert.doesNotThrow(() => em.emit('x'));
            assert.doesNotThrow(() => em.emit('y'));
        });

        it('returns a remover for EventEmitters', function () {
            const em = new EventEmitter();
            let n = 0;
            const remove = util.safeOn(em, 'x', () => { n++; });
            em.emit('x');
            remove();
            remove(); // idempotent
            em.emit('x');
            assert.equal(n, 1);
            assert.equal(em.listenerCount('x'), 0);
        });

        it('returns the kafkajs-style remover (on() returning a function)', function () {
            let removed = 0;
            let listener;
            const kafkaLike = { on(ev, fn) { listener = fn; return () => { removed++; }; } };
            const remove = util.safeOn(kafkaLike, 'consumer.crash', () => { throw new Error('x'); });
            assert.doesNotThrow(() => listener({ payload: {} }));
            remove();
            remove();
            assert.equal(removed, 1);
        });

        it('never throws when emitter.on throws (invalid kafkajs event name)', function () {
            const errors = [];
            const bad = { on() { throw new Error('Event name should be one of ...'); } };
            const remove = util.safeOn(bad, 'nope', () => {}, (e) => errors.push(e));
            assert.equal(typeof remove, 'function');
            assert.doesNotThrow(remove);
            assert.equal(errors.length, 1);
            assert.doesNotThrow(() => util.safeOn(null, 'x', () => {}));
        });

        it('swallows a throwing remover', function () {
            const em = { on() { return () => { throw new Error('remove failed'); }; } };
            const remove = util.safeOn(em, 'x', () => {});
            assert.doesNotThrow(remove);
        });
    });

    describe('withTimeout', function () {
        it('resolves with the value when the promise wins', async function () {
            assert.equal(await util.withTimeout(Promise.resolve(5), 1000, 'timeout'), 5);
            assert.equal(await util.withTimeout(7, 1000, 'timeout'), 7);
        });

        it('resolves with the timeout value when time is up', async function () {
            const never = new Promise(() => {});
            const start = Date.now();
            assert.equal(await util.withTimeout(never, 30, 'timeout'), 'timeout');
            assert.ok(Date.now() - start >= 25);
        });

        it('never rejects: a rejection resolves to the timeout value', async function () {
            assert.equal(await util.withTimeout(Promise.reject(new Error('x')), 1000, 'fallback'), 'fallback');
            const thenable = { then() { throw new Error('then throws'); } };
            assert.equal(await util.withTimeout(thenable, 1000, 'fb'), 'fb');
        });

        it('does not leave a late rejection unhandled', async function () {
            let reject;
            const p = new Promise((_, r) => { reject = r; });
            assert.equal(await util.withTimeout(p, 5, 'late'), 'late');
            reject(new Error('late rejection'));
            await new Promise((r) => setTimeout(r, 10)); // would trip an unhandledRejection trap
        });

        it('tolerates invalid ms values', async function () {
            assert.equal(await util.withTimeout(new Promise(() => {}), 'abc', 'x'), 'x');
            assert.equal(await util.withTimeout(new Promise(() => {}), -5, 'y'), 'y');
        });

        it('clears its timer and does not keep the process alive', async function () {
            const realSet = global.setTimeout;
            const realClear = global.clearTimeout;
            const created = [];
            const cleared = [];
            global.setTimeout = function (...args) {
                const t = realSet(...args);
                created.push(t);
                return t;
            };
            global.clearTimeout = function (t) {
                cleared.push(t);
                return realClear(t);
            };
            try {
                await util.withTimeout(Promise.resolve(1), 60000, 'x');
            } finally {
                global.setTimeout = realSet;
                global.clearTimeout = realClear;
            }
            assert.equal(created.length, 1);
            assert.equal(created[0].hasRef(), false, 'timer must be unref()ed');
            assert.ok(cleared.includes(created[0]), 'timer must be cleared');
        });
    });

    describe('noop', function () {
        it('returns undefined', function () {
            assert.equal(util.noop(1, 2), undefined);
        });
    });

    describe('sanitizeName', function () {
        it('replaces unsafe characters', function () {
            assert.equal(util.sanitizeName('a b/c\\d:e*f'), 'a_b_c_d_e_f');
            assert.equal(util.sanitizeName('node.id-1_2'), 'node.id-1_2');
            assert.equal(util.sanitizeName('ünï'), '_n_');
        });
        it('handles empty and non-string input', function () {
            assert.equal(util.sanitizeName(''), 'default');
            assert.equal(util.sanitizeName(undefined), 'default');
            assert.equal(util.sanitizeName(null), 'default');
            assert.equal(util.sanitizeName(123), '123');
        });
        it('cannot produce path traversal or hidden names', function () {
            assert.equal(util.sanitizeName('..'), '__');
            assert.equal(util.sanitizeName('.'), '_');
            assert.equal(util.sanitizeName('../../etc'), '___.._etc');
            assert.ok(!util.sanitizeName('../../etc').startsWith('.'));
            assert.ok(!util.sanitizeName('../../etc').includes('/'));
            assert.equal(util.sanitizeName('.hidden'), '_hidden');
        });
        it('limits the length to 100', function () {
            assert.equal(util.sanitizeName('x'.repeat(500)).length, 100);
        });
    });

    describe('resolveDir', function () {
        it('normalises absolute paths', function () {
            const abs = path.resolve('/tmp/a/../b');
            assert.equal(util.resolveDir(path.join(path.sep, 'tmp', 'a', '..', 'b'), '/base'), abs);
        });
        it('resolves relative paths against baseDir', function () {
            assert.equal(util.resolveDir('queue', '/data/nr'), path.resolve('/data/nr', 'queue'));
            assert.equal(util.resolveDir('', '/data/nr'), path.resolve('/data/nr'));
        });
        it('expands ~', function () {
            assert.equal(util.resolveDir('~/q', '/x'), path.join(os.homedir(), 'q'));
        });
        it('never throws', function () {
            assert.equal(typeof util.resolveDir(undefined, undefined), 'string');
            assert.equal(typeof util.resolveDir({}, 5), 'string');
        });
    });

    describe('truncate', function () {
        it('keeps short strings and shortens long ones with an ellipsis', function () {
            assert.equal(util.truncate('abc', 5), 'abc');
            assert.equal(util.truncate('abcde', 5), 'abcde');
            assert.equal(util.truncate('abcdef', 5), 'abcd…');
            assert.equal(util.truncate('abcdef', 5).length, 5);
            assert.equal(util.truncate(12345678, 4), '123…');
            assert.equal(util.truncate('abc', 0), '');
            assert.equal(util.truncate('abc', 1), '…');
        });
        it('does not split surrogate pairs', function () {
            const s = 'ab😀cdef';
            const t = util.truncate(s, 4);
            assert.equal(t, 'ab…');
        });
        it('never throws', function () {
            assert.equal(util.truncate({ toString() { throw new Error('x'); } }, 5), '');
            assert.equal(util.truncate(undefined, 20), 'undefined');
        });
    });

    describe('userDir', function () {
        const KEYS = ['NODE_RED_HOME', 'HOMEPATH', 'HOME', 'USERPROFILE'];
        let saved;
        beforeEach(function () {
            saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
        });
        afterEach(function () {
            for (const k of KEYS) {
                if (saved[k] === undefined) delete process.env[k];
                else process.env[k] = saved[k];
            }
        });
        it('prefers RED.settings.userDir', function () {
            assert.equal(util.userDir({ settings: { userDir: '/u' } }), '/u');
        });
        it('falls back to ~/.node-red, not to the Node-RED install directory in NODE_RED_HOME (PATH-1)', function () {
            // red.js always sets NODE_RED_HOME to the node-red package directory
            const install = tmpDir('kafkajs-nc-nr-install-');
            process.env.NODE_RED_HOME = install;
            delete process.env.HOMEPATH;
            const home = path.join(process.env.HOME || process.env.USERPROFILE || os.homedir(), '.node-red');
            assert.equal(util.userDir({ settings: {} }), home);
            delete process.env.NODE_RED_HOME;
            assert.equal(util.userDir(undefined), home);
            const throwing = { get settings() { throw new Error('x'); } };
            assert.equal(util.userDir(throwing), home);
        });
        it('uses NODE_RED_HOME or HOMEPATH/.node-red when they hold a Node-RED configuration (as Node-RED does)', function () {
            const nrHome = tmpDir('kafkajs-nc-nr-home-');
            fs.writeFileSync(path.join(nrHome, '.config.json'), '{}');
            process.env.NODE_RED_HOME = nrHome;
            assert.equal(util.userDir({ settings: {} }), nrHome);
            const winHome = tmpDir('kafkajs-nc-nr-homepath-');
            fs.mkdirSync(path.join(winHome, '.node-red'));
            fs.writeFileSync(path.join(winHome, '.node-red', '.config.nodes.json'), '{}');
            process.env.NODE_RED_HOME = tmpDir('kafkajs-nc-nr-install-');
            process.env.HOMEPATH = winHome;
            assert.equal(util.userDir({ settings: {} }), path.join(winHome, '.node-red'));
        });
    });

    describe('envReference / envProblem (the one ${VAR} rule of every node)', function () {
        it('a whole-value reference left over means the variable is not set', function () {
            assert.deepEqual(util.envReference('${KAFKA_BROKERS}'), { name: 'KAFKA_BROKERS', whole: true });
            assert.deepEqual(util.envReference('$(OLD_STYLE)'), { name: 'OLD_STYLE', whole: true });
            assert.equal(util.envProblem('${KAFKA_BROKERS}'), 'environment variable KAFKA_BROKERS is not set');
            assert.equal(util.envProblem('$(OLD)', { embedded: false }), 'environment variable OLD is not set');
        });
        it('a reference inside a longer value is never replaced by Node-RED, whatever the environment', function () {
            for (const [value, name] of [['kafka-${REGION}:9092', 'REGION'], [' ${KB}', 'KB'], ['${KB} ', 'KB'],
                ['a:1,${B2}', 'B2'], ['x${}y', '?']]) {
                assert.deepEqual(util.envReference(value), { name, whole: false }, value);
                assert.equal(util.envProblem(value), `\${${name}} is only replaced by Node-RED when it is the whole value`);
                assert.equal(util.envProblem(value, { embedded: false }), null, 'ignored on request (passwords)');
            }
        });
        it('anything else is no reference', function () {
            for (const v of ['localhost:9092', '', '$', '$HOME', 'pa$$word', '$(not closed', 42, null, undefined, {}]) {
                assert.equal(util.envReference(v), null, String(v));
                assert.equal(util.envProblem(v), null, String(v));
            }
        });
    });

    describe('formatDelay', function () {
        it('formats delays for status texts', function () {
            assert.equal(util.formatDelay(100), '0.1s');
            assert.equal(util.formatDelay(1000), '1s');
            assert.equal(util.formatDelay(8040), '8s');
            assert.equal(util.formatDelay(9960), '10s');
            assert.equal(util.formatDelay(60000), '60s');
            assert.equal(util.formatDelay(300000), '5m');
            assert.equal(util.formatDelay('2500'), '2.5s');
        });
        it('never throws and never shows a negative or non-finite delay', function () {
            for (const v of [NaN, -5, undefined, null, Infinity, {}, Symbol('x')]) assert.equal(util.formatDelay(v), '0s');
        });
    });
});
