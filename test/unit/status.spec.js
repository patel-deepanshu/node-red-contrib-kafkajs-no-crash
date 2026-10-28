'use strict';

const assert = require('node:assert/strict');
const { StatusReporter } = require('../../lib/status');

/** A fake Node-RED node recording status() calls. */
function fakeNode() {
    const calls = [];
    return {
        calls,
        status(s) {
            calls.push(s);
        }
    };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

describe('lib/status', function () {
    it('set() applies immediately and dedupes identical consecutive statuses', function () {
        const node = fakeNode();
        const s = new StatusReporter(node);
        s.set('green', 'dot', 'connected');
        s.set('green', 'dot', 'connected');
        s.set('yellow', 'ring', 'x');
        s.set('green', 'dot', 'connected');
        assert.deepEqual(node.calls, [
            { fill: 'green', shape: 'dot', text: 'connected' },
            { fill: 'yellow', shape: 'ring', text: 'x' },
            { fill: 'green', shape: 'dot', text: 'connected' }
        ]);
    });

    it('named helpers use consistent colours and shapes', function () {
        const node = fakeNode();
        const s = new StatusReporter(node);
        s.connecting();
        s.connected();
        s.waiting('offline · retry 8s');
        s.error('auth failed · retry 60s');
        s.idle('stopped');
        s.connecting('connecting…');
        s.connected('connected · 12 queued');
        s.clear();
        assert.deepEqual(node.calls, [
            { fill: 'yellow', shape: 'ring', text: 'connecting' },
            { fill: 'green', shape: 'dot', text: 'connected' },
            { fill: 'yellow', shape: 'ring', text: 'offline · retry 8s' },
            { fill: 'red', shape: 'ring', text: 'auth failed · retry 60s' },
            { fill: 'grey', shape: 'ring', text: 'stopped' },
            { fill: 'yellow', shape: 'ring', text: 'connecting…' },
            { fill: 'green', shape: 'dot', text: 'connected · 12 queued' },
            {}
        ]);
    });

    it('truncates texts to 48 characters', function () {
        const node = fakeNode();
        const s = new StatusReporter(node);
        s.error('e'.repeat(100));
        assert.equal(node.calls[0].text.length, 48);
        assert.ok(node.calls[0].text.endsWith('…'));
        s.waiting(undefined);
        assert.equal(node.calls[1].text, '');
        s.idle(12345);
        assert.equal(node.calls[2].text, '12345');
    });

    it('activity() applies the first update at once and throttles the rest (latest wins)', async function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 60 });
        s.activity('green', 'dot', 'flushing · 100 queued');
        s.activity('green', 'dot', 'flushing · 90 queued');
        s.activity('green', 'dot', 'flushing · 80 queued');
        assert.equal(node.calls.length, 1);
        assert.equal(node.calls[0].text, 'flushing · 100 queued');
        await sleep(100);
        assert.equal(node.calls.length, 2);
        assert.equal(node.calls[1].text, 'flushing · 80 queued');
        await sleep(100);
        assert.equal(node.calls.length, 2, 'no further writes without new activity');
        s.dispose();
    });

    it('limits activity() writes to about one per throttle period', async function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 50 });
        const end = Date.now() + 260;
        let i = 0;
        while (Date.now() < end) {
            s.activity('green', 'dot', `n=${i++}`);
            await sleep(2);
        }
        await sleep(80);
        assert.ok(node.calls.length >= 2 && node.calls.length <= 8, `writes: ${node.calls.length}`);
        assert.equal(node.calls[node.calls.length - 1].text, `n=${i - 1}`, 'the latest activity is shown last');
        s.dispose();
    });

    it('set() cancels a pending activity so a stale update cannot overwrite it', async function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 40 });
        s.activity('green', 'dot', 'flushing · 10 queued');
        s.activity('green', 'dot', 'flushing · 9 queued');
        s.waiting('offline · retry 5s');
        await sleep(80);
        assert.deepEqual(node.calls.map((c) => c.text), ['flushing · 10 queued', 'offline · retry 5s']);
        s.dispose();
    });

    it('activity timers are unref()ed', function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 10000 });
        s.activity('green', 'dot', 'a');
        s.activity('green', 'dot', 'b');
        assert.ok(s._timer, 'a timer is pending');
        assert.equal(s._timer.hasRef(), false);
        s.dispose();
        assert.equal(s._timer, null);
    });

    it('dispose() cancels timers and turns every call into a no-op', async function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 30 });
        s.activity('green', 'dot', 'a');
        s.activity('green', 'dot', 'b');
        s.dispose();
        s.dispose();
        assert.equal(s.disposed, true);
        s.set('red', 'ring', 'x');
        s.connected();
        s.error('x');
        s.clear();
        s.activity('green', 'dot', 'c');
        await sleep(60);
        assert.deepEqual(node.calls.map((c) => c.text), ['a']);
    });

    it('never throws when node.status throws or is missing', async function () {
        const throwing = { status() { throw new Error('node gone'); } };
        const s = new StatusReporter(throwing, { throttleMs: 5 });
        assert.doesNotThrow(() => {
            s.set('red', 'ring', 'x');
            s.connected();
            s.activity('green', 'dot', 'a');
            s.activity('green', 'dot', 'b');
            s.clear();
        });
        await sleep(20); // the throttled flush must not throw from its timer either
        const missing = new StatusReporter({});
        assert.doesNotThrow(() => missing.connected());
        const nothing = new StatusReporter(undefined, null);
        assert.doesNotThrow(() => nothing.error('x'));
        s.dispose();
    });

    it('accepts bad options', function () {
        const node = fakeNode();
        const s = new StatusReporter(node, { throttleMs: 'abc' });
        assert.equal(s._throttleMs, 1000);
        s.dispose();
    });
});
