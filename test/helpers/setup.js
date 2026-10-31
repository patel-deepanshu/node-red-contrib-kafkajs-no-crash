'use strict';

/**
 * Mocha root hooks (loaded through .mocharc.json "require"): the crash trap.
 *
 * Node-RED exits the process on any uncaught exception or unhandled
 * rejection, so either one is a crash bug in this palette. Mocha 11, however,
 * silently ignores unhandled rejections from user code (it re-emits them on
 * `process` and, with no other listener, nothing happens). These hooks make
 * every such error fail a test:
 *
 * - beforeAll: listen to 'unhandledRejection' and 'uncaughtException' on
 *   `process` and record what arrives (with "where" it happened).
 * - beforeEach: wrap the test function so that, once the test body has
 *   finished (plus one event-loop turn, which lets Node report rejections
 *   created by the body's last steps), anything recorded fails THAT test.
 *   Failing the test itself, rather than a hook, matters: when a root
 *   afterEach hook fails, mocha skips every remaining test of the run.
 * - afterEach: backstop for errors that arrive after the body (in the test
 *   file's own afterEach hooks, e.g. node close handlers during
 *   helper.unload(), or from timers). It fails the hook, which stops the run;
 *   that is acceptable for a real crash bug and impossible to miss.
 * - afterAll: last check, remove the listeners, delete tmpDir() directories.
 *
 * Uncaught exceptions: while tests run, mocha's own listener already fails
 * the current test or hook, so ours only records when it is the sole
 * listener (otherwise every failure would be reported twice).
 */

const { cleanupTmpDirs } = require('./common');

const WRAPPED = Symbol('kafkajs-nc-test:crash-trap-wrapped');

/** @type {Array<{kind: string, error: *, where: string}>} */
const recorded = [];
// Mocha's own 'unhandledRejection' listener re-emits rejections on `process`,
// so the same rejection reaches our listener twice; dedupe by promise.
const seenPromises = new WeakSet();
let phase = 'outside any test';
let installed = false;

function isMochaInternalError(reason) {
    // Mocha forwards its own errors (code ERR_MOCHA_*) to its uncaught handler.
    return reason != null && typeof reason === 'object' &&
        typeof reason.code === 'string' && reason.code.startsWith('ERR_MOCHA_');
}

function onUnhandledRejection(reason, promise) {
    if (promise !== null && typeof promise === 'object') {
        if (seenPromises.has(promise)) return;
        seenPromises.add(promise);
    }
    if (isMochaInternalError(reason)) return;
    recorded.push({ kind: 'unhandledRejection', error: reason, where: phase });
}

function onUncaughtException(error) {
    if (process.listenerCount('uncaughtException') > 1) return; // mocha reports it
    recorded.push({ kind: 'uncaughtException', error, where: phase });
}

function install() {
    if (installed) return;
    installed = true;
    process.on('unhandledRejection', onUnhandledRejection);
    process.on('uncaughtException', onUncaughtException);
}

function uninstall() {
    if (!installed) return;
    installed = false;
    process.removeListener('unhandledRejection', onUnhandledRejection);
    process.removeListener('uncaughtException', onUncaughtException);
}

/** Remove and return everything recorded so far. */
function takeRecorded() {
    return recorded.splice(0, recorded.length);
}

function describeError(value) {
    try {
        if (value instanceof Error) return `${value.name}: ${value.message}`;
        if (value !== null && typeof value === 'object' && typeof value.message === 'string') {
            return `${value.name || 'Object'}: ${value.message}`;
        }
        return `${typeof value} ${String(value)}`;
    } catch (_) {
        return '<unprintable value>';
    }
}

function stackFrames(value, max = 8) {
    try {
        if (!value || typeof value.stack !== 'string') return [];
        return value.stack.split('\n').filter((line) => /^\s+at /.test(line)).slice(0, max).map((l) => l.trim());
    } catch (_) {
        return [];
    }
}

/**
 * Build the error that fails a test. The message lists what happened and
 * where; the stack adds the frames of each recorded error (mocha rewrites
 * paths in stacks, so frames must not be part of the message or it prints
 * everything twice). When the test itself also failed, its error is kept as
 * `cause` (mocha prints it) and its assertion diff fields are copied.
 *
 * @param {Array<{kind: string, error: *, where: string}>} records
 * @param {*} [testFailure]
 * @returns {Error} with `records` (the recorded errors)
 */
function buildTrapError(records, testFailure) {
    const failed = testFailure !== undefined && testFailure !== null;
    const lines = [];
    if (failed) {
        lines.push(`Test failed: ${describeError(testFailure)}`);
        lines.push('Additionally:');
    }
    lines.push(`${records.length} unhandled error(s) detected. In production each one would crash ` +
        'Node-RED (it exits the process on uncaught exceptions and unhandled rejections):');
    const details = [];
    records.forEach((r, i) => {
        lines.push(`  ${i + 1}) ${r.kind} ${r.where}: ${describeError(r.error)}`);
        details.push(`    ${i + 1}) ${describeError(r.error)}`);
        for (const frame of stackFrames(r.error)) details.push(`        ${frame}`);
    });
    const err = new Error(lines.join('\n'));
    err.name = 'CrashTrapError';
    err.stack = `${err.name}: ${err.message}\n${details.join('\n')}`;
    err.records = records;
    if (failed) {
        err.cause = testFailure;
        for (const key of ['actual', 'expected', 'operator', 'showDiff']) {
            if (typeof testFailure === 'object' && key in testFailure) err[key] = testFailure[key];
        }
    }
    return err;
}

function nextTurn() {
    return new Promise((resolve) => setImmediate(resolve));
}

/**
 * Called after a test body finished: give Node one turn to report rejections,
 * then turn anything recorded into a failure of the test.
 *
 * @param {object} test mocha Test
 * @param {*} failure the test's own error, if any
 * @returns {Promise<*>} resolves with the error the test should fail with (or undefined)
 */
async function checkAfterBody(test, failure) {
    // still "during test": rejections created by the body's last steps are reported in this turn
    await nextTurn();
    // Mocha already finished this test (timeout, or an uncaught exception forced
    // it to complete) and ignores this late result; another test or hook may be
    // running now, so leave the phase and the records to it.
    if (test.timedOut || test.state !== undefined) return failure;
    phase = `after test "${test.fullTitle()}" finished (afterEach hook or timer)`;
    if (test.isPending()) return failure; // this.skip(): leave records to afterEach
    const records = takeRecorded();
    if (records.length === 0) return failure;
    return buildTrapError(records, failure);
}

/**
 * Replace `test.fn` with a wrapper that runs the original and then checks for
 * recorded errors. Mocha decided callback-vs-promise style from the original
 * function's arity when the test was created (`test.async`), so the wrapper
 * keeps the same style.
 *
 * @param {object} test mocha Test
 */
function wrapTest(test) {
    const original = test.fn;
    if (typeof original !== 'function' || original[WRAPPED]) return;
    const title = test.fullTitle();
    let wrapped;
    if (test.async) {
        wrapped = function (done) {
            phase = `during test "${title}"`;
            // return the original result so mocha's "overspecified" check still works
            return original.call(this, function (err) {
                checkAfterBody(test, err).then(
                    (finalErr) => done(finalErr),
                    (unexpected) => done(unexpected)
                );
            });
        };
    } else {
        wrapped = async function () {
            phase = `during test "${title}"`;
            let failure;
            try {
                await original.call(this);
            } catch (e) {
                failure = e === undefined || e === null ? new Error(`test threw ${e}`) : e;
            }
            const finalErr = await checkAfterBody(test, failure);
            if (finalErr !== undefined) throw finalErr;
        };
    }
    wrapped[WRAPPED] = true;
    test.fn = wrapped;
}

const mochaHooks = {
    beforeAll() {
        install();
    },
    beforeEach() {
        const test = this.currentTest;
        if (!test) return;
        // errors recorded from now until the body starts (other hooks, timers of
        // earlier tests) are reported by this test, with this "where" text
        phase = `before test "${test.fullTitle()}" (in a hook, or a timer left by an earlier test)`;
        wrapTest(test);
    },
    async afterEach() {
        await nextTurn();
        const records = takeRecorded();
        if (records.length > 0) throw buildTrapError(records);
    },
    async afterAll() {
        phase = 'after the last test';
        try {
            await nextTurn();
            const records = takeRecorded();
            if (records.length > 0) throw buildTrapError(records);
        } finally {
            uninstall();
            cleanupTmpDirs();
        }
    }
};

module.exports = {
    mochaHooks,
    // exposed for test/unit/harness-smoke.spec.js
    _internal: { buildTrapError, wrapTest, takeRecorded, isInstalled: () => installed }
};
