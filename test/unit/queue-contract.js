'use strict';

/**
 * Behaviour shared by DiskQueue and MemoryQueue (lib/disk-queue.js, lib/memory-queue.js).
 * Not a spec file itself: disk-queue.spec.js and memory-queue.spec.js call runQueueContract().
 */

const assert = require('node:assert/strict');
const { QueueFullError } = require('../../lib/errors');

/** Tiny seeded PRNG (mulberry32) so the randomized test is reproducible. */
function prng(seed) {
    let a = seed >>> 0;
    return function next() {
        a = (a + 0x6D2B79F5) >>> 0;
        let t = a;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

/** Logger that records every call per level. */
function captureLogger() {
    const logs = { warn: [], error: [], debug: [] };
    return {
        logs,
        logger: {
            warn: (m) => logs.warn.push(m),
            error: (m) => logs.error.push(m),
            debug: (m) => logs.debug.push(m)
        }
    };
}

function ids(entries) {
    return entries.map((e) => (e.corrupt ? 'corrupt' : e.record.id));
}

function rec(id, extra) {
    return Object.assign({ id, ts: 1700000000000 + id }, extra);
}

function isQueueFull(err) {
    return err instanceof QueueFullError && err.name === 'KafkaNcQueueFullError';
}

/**
 * Register the contract suite.
 * @param {object} p
 * @param {string} p.name suite label
 * @param {(options: object) => Promise<object>} p.create returns an OPENED queue built with options
 * @param {(queue: object, options: object) => Promise<object>} [p.reopen] close + reopen on the same storage
 * @param {(record: object) => number} p.recordBytes how many bytes `queue.bytes` counts for one record
 */
function runQueueContract({ name, create, reopen, recordBytes }) {
    describe(`${name} — queue contract`, function () {
        const live = new Set();

        async function make(options = {}) {
            const q = await create(options);
            live.add(q);
            return q;
        }

        async function again(q, options = {}) {
            live.delete(q);
            const q2 = await reopen(q, options);
            live.add(q2);
            return q2;
        }

        afterEach(async function () {
            const all = [...live];
            live.clear();
            await Promise.all(all.map((q) => q.close()));
        });

        it('starts empty', async function () {
            const q = await make();
            assert.equal(q.size, 0);
            assert.equal(q.bytes, 0);
            assert.deepEqual(await q.peek(), []);
            const s = q.stats();
            assert.equal(s.size, 0);
            assert.equal(s.bytes, 0);
            assert.equal(s.droppedTotal, 0);
            assert.equal(s.deadLettered, 0);
            assert.equal(typeof s.segments, 'number');
            assert.equal(s.oldestTs, undefined);
        });

        it('open() is idempotent', async function () {
            const q = await make();
            await q.open();
            await q.open();
            await q.enqueue(rec(1));
            await q.open();
            assert.deepEqual(ids(await q.peek()), [1]);
        });

        it('keeps FIFO order across enqueue and enqueueMany', async function () {
            const q = await make();
            await q.enqueue(rec(1));
            await q.enqueueMany([rec(2), rec(3), rec(4)]);
            await q.enqueue(rec(5));
            assert.equal(q.size, 5);
            assert.deepEqual(ids(await q.peek()), [1, 2, 3, 4, 5]);
        });

        it('returns records unchanged (deep equality) with positions', async function () {
            const q = await make();
            const r = { id: 1, ts: 5, s: 'héllo ✓ 😀', n: null, a: [1, { b: true }], nested: { x: 1.5 } };
            await q.enqueue(r);
            const [e] = await q.peek();
            assert.deepEqual(e.record, r);
            assert.equal(typeof e.pos.seg, 'number');
            assert.equal(typeof e.pos.end, 'number');
            assert.equal(e.corrupt, undefined);
        });

        it('peek does not remove and honours max (default 100)', async function () {
            const q = await make();
            const many = [];
            for (let i = 1; i <= 150; i++) {
                many.push(rec(i));
            }
            await q.enqueueMany(many);
            assert.deepEqual(ids(await q.peek(3)), [1, 2, 3]);
            assert.deepEqual(ids(await q.peek(3)), [1, 2, 3]);
            assert.equal((await q.peek()).length, 100);
            assert.equal((await q.peek(1000)).length, 150);
            assert.deepEqual(await q.peek(0), []);
            assert.equal(q.size, 150);
        });

        it('counts size synchronously at call time', async function () {
            const q = await make();
            const p1 = q.enqueue(rec(1));
            assert.equal(q.size, 1);
            const p2 = q.enqueueMany([rec(2), rec(3)]);
            assert.equal(q.size, 3);
            assert.equal(q.bytes, recordBytes(rec(1)) + recordBytes(rec(2)) + recordBytes(rec(3)));
            await Promise.all([p1, p2]);
            assert.equal(q.size, 3);
            assert.equal(q.stats().size, 3);
        });

        it('ack removes peeked entries and returns how many were removed', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2), rec(3), rec(4)]);
            const batch = await q.peek(2);
            assert.equal(await q.ack(batch), 2);
            assert.equal(q.size, 2);
            assert.equal(q.bytes, recordBytes(rec(3)) + recordBytes(rec(4)));
            assert.deepEqual(ids(await q.peek()), [3, 4]);
            assert.equal(await q.ack(await q.peek()), 2);
            assert.equal(q.size, 0);
            assert.equal(q.bytes, 0);
            assert.deepEqual(await q.peek(), []);
        });

        it('ack is positional: acking only the last entry removes everything before it', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2), rec(3), rec(4)]);
            const batch = await q.peek(3);
            assert.equal(await q.ack([batch[2]]), 3);
            assert.deepEqual(ids(await q.peek()), [4]);
        });

        it('ack is idempotent and ignores stale or malformed positions', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            const first = await q.peek(2);
            assert.equal(await q.ack(first), 2);
            assert.equal(await q.ack(first), 0);
            assert.equal(await q.ack([first[0]]), 0);
            assert.equal(await q.ack([]), 0);
            assert.equal(await q.ack(null), 0);
            assert.equal(await q.ack(undefined), 0);
            assert.equal(await q.ack('junk'), 0);
            assert.equal(await q.ack([{}, { pos: { seg: 'x', end: 1 } }, { pos: { seg: 1, end: -5 } }, 42]), 0);
            assert.equal(q.size, 1);
            assert.deepEqual(ids(await q.peek()), [3]);
        });

        it('a single entry (not wrapped in an array) can be acked', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2)]);
            const [e] = await q.peek(1);
            assert.equal(await q.ack(e), 1);
            assert.deepEqual(ids(await q.peek()), [2]);
        });

        it('enqueueMany is all-or-nothing when a record is not JSON-serialisable', async function () {
            const q = await make();
            const circular = { id: 2 };
            circular.self = circular;
            await assert.rejects(q.enqueueMany([rec(1), circular, rec(3)]), TypeError);
            assert.equal(q.size, 0);
            assert.deepEqual(await q.peek(), []);
            await assert.rejects(q.enqueue(undefined), TypeError);
            await assert.rejects(q.enqueue({ id: 1n }), TypeError);
            await assert.rejects(q.enqueue(() => 1), TypeError);
            assert.equal(q.size, 0);
            await q.enqueue(rec(4));
            assert.deepEqual(ids(await q.peek()), [4]);
        });

        it('rejects a non-array enqueueMany and resolves an empty one', async function () {
            const q = await make();
            await assert.rejects(q.enqueueMany('nope'), TypeError);
            await assert.rejects(q.enqueueMany(null), TypeError);
            assert.deepEqual(await q.enqueueMany([]), { dropped: 0 });
            assert.equal(q.size, 0);
        });

        it('snapshots records at enqueue time', async function () {
            const q = await make();
            const r = rec(1, { list: [1, 2] });
            const p = q.enqueue(r);
            r.id = 99;
            r.list.push(3);
            await p;
            const [e] = await q.peek();
            assert.equal(e.record.id, 1);
            assert.deepEqual(e.record.list, [1, 2]);
            e.record.id = 1234; // mutating a peeked record must not change the queue
            assert.equal((await q.peek())[0].record.id, 1);
        });

        it('drop-oldest (by count) drops the oldest records and reports them', async function () {
            const { logs, logger } = captureLogger();
            const q = await make({ maxMessages: 3, logger });
            const results = [];
            for (let i = 1; i <= 5; i++) {
                results.push(await q.enqueue(rec(i)));
            }
            assert.deepEqual(results.map((r) => r.dropped), [0, 0, 0, 1, 1]);
            assert.deepEqual(ids(await q.peek()), [3, 4, 5]);
            assert.equal(q.size, 3);
            assert.equal(q.stats().droppedTotal, 2);
            assert.equal(logs.warn.length, 2);
            assert.match(logs.warn[0], /dropped 1 oldest/);
        });

        it('drop-oldest (by count) inside one enqueueMany', async function () {
            const q = await make({ maxMessages: 3 });
            await q.enqueueMany([rec(1), rec(2)]);
            const r = await q.enqueueMany([rec(3), rec(4), rec(5)]);
            assert.equal(r.dropped, 2);
            assert.deepEqual(ids(await q.peek()), [3, 4, 5]);
        });

        it('drop-oldest (by bytes)', async function () {
            const one = recordBytes(rec(1));
            const q = await make({ maxBytes: 3 * one + Math.floor(one / 2) });
            for (let i = 1; i <= 5; i++) {
                await q.enqueue(rec(i));
            }
            assert.deepEqual(ids(await q.peek()), [3, 4, 5]);
            assert.equal(q.bytes, 3 * one);
            assert.equal(q.stats().droppedTotal, 2);
        });

        it('drop-newest rejects with QueueFullError and writes nothing', async function () {
            const q = await make({ maxMessages: 2, overflow: 'drop-newest' });
            await q.enqueueMany([rec(1), rec(2)]);
            const err = await q.enqueue(rec(3)).then(() => null, (e) => e);
            assert.ok(isQueueFull(err), `expected QueueFullError, got ${err}`);
            assert.match(err.message, /full/);
            assert.equal(q.size, 2);
            assert.deepEqual(ids(await q.peek()), [1, 2]);
            assert.equal(q.stats().droppedTotal, 0);
            await q.ack(await q.peek(1));
            await q.enqueue(rec(4));
            assert.deepEqual(ids(await q.peek()), [2, 4]);
        });

        it('drop-newest (by bytes)', async function () {
            const one = recordBytes(rec(1));
            const q = await make({ maxBytes: 2 * one, overflow: 'drop-newest' });
            await q.enqueue(rec(1));
            await q.enqueue(rec(2));
            await assert.rejects(q.enqueue(rec(3)), isQueueFull);
            assert.deepEqual(ids(await q.peek()), [1, 2]);
        });

        it('a batch larger than the whole limit is rejected under both policies', async function () {
            for (const overflow of ['drop-oldest', 'drop-newest']) {
                const q = await make({ maxMessages: 2, overflow });
                await q.enqueue(rec(1));
                await assert.rejects(q.enqueueMany([rec(2), rec(3), rec(4)]), isQueueFull);
                assert.deepEqual(ids(await q.peek()), [1]);
                assert.equal(q.stats().droppedTotal, 0);

                const one = recordBytes(rec(1));
                const qb = await make({ maxBytes: one * 2, overflow });
                await assert.rejects(qb.enqueue(rec(1, { pad: 'x'.repeat(3 * one) })), isQueueFull);
                assert.equal(qb.size, 0);
            }
        });

        it('positional ack after drop-oldest only removes records that are still queued', async function () {
            const q = await make({ maxMessages: 3 });
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            const inFlight = await q.peek();
            await q.enqueueMany([rec(4), rec(5)]); // drops 1 and 2 while they are "being sent"
            assert.equal(q.size, 3);
            assert.equal(await q.ack(inFlight), 1); // only 3 was still queued
            assert.equal(q.size, 2);
            assert.deepEqual(ids(await q.peek()), [4, 5]);
            assert.equal(await q.ack(inFlight), 0);
        });

        it('concurrent enqueues keep call order', async function () {
            const q = await make();
            const ps = [];
            let id = 1;
            for (let i = 0; i < 100; i++) {
                if (i % 10 === 0) {
                    ps.push(q.enqueueMany([rec(id++), rec(id++)]));
                } else {
                    ps.push(q.enqueue(rec(id++)));
                }
            }
            assert.equal(q.size, id - 1);
            await Promise.all(ps);
            const got = ids(await q.peek(1000));
            assert.deepEqual(got, Array.from({ length: id - 1 }, (_, i) => i + 1));
        });

        it('clear() drops what was requested before it, keeps what came after', async function () {
            const q = await make();
            const before = await q.peek();
            assert.deepEqual(before, []);
            const p1 = q.enqueue(rec(1));
            const p2 = q.enqueue(rec(2));
            const c = q.clear();
            const p3 = q.enqueue(rec(3));
            await Promise.all([p1, p2, p3]);
            assert.equal(await c, 2);
            assert.equal(q.size, 1);
            assert.deepEqual(ids(await q.peek()), [3]);
        });

        it('stale entries peeked before clear() cannot ack newer records', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            const stale = await q.peek();
            assert.equal(await q.clear(), 3);
            assert.equal(q.size, 0);
            await q.enqueueMany([rec(4), rec(5)]);
            assert.equal(await q.ack(stale), 0);
            assert.deepEqual(ids(await q.peek()), [4, 5]);
        });

        it('close() is idempotent and every later call rejects with "queue closed"', async function () {
            const q = await make();
            await q.enqueue(rec(1));
            await q.close();
            await q.close();
            const calls = [
                () => q.enqueue(rec(2)),
                () => q.enqueueMany([rec(2)]),
                () => q.peek(),
                () => q.ack([{ pos: { seg: 0, end: 1 } }]),
                () => q.deadLetter({ record: rec(1) }, 'x'),
                () => q.clear(),
                () => q.open()
            ];
            for (const call of calls) {
                await assert.rejects(call(), { message: 'queue closed' });
            }
            assert.equal(q.closed, true);
        });

        it('operations requested before close() still complete', async function () {
            const q = await make();
            const p = q.enqueue(rec(1));
            const closed = q.close();
            await p;
            await closed;
        });

        it('deadLetter() counts entries and does not remove them', async function () {
            const q = await make();
            await q.enqueueMany([rec(1), rec(2)]);
            const entries = await q.peek();
            await q.deadLetter(entries[0], new Error('boom'));
            await q.deadLetter(entries[1], 'expired');
            await q.deadLetter({ corrupt: true, raw: '{bad', pos: entries[1].pos }, new TypeError('x'));
            assert.equal(q.stats().deadLettered, 3);
            assert.equal(q.size, 2);
            await q.ack(entries);
            assert.equal(q.size, 0);
        });

        it('stats().oldestTs reports the head record ts once it has been peeked', async function () {
            const q = await make();
            await q.enqueueMany([{ id: 1, ts: 123 }, { id: 2, ts: 456 }]);
            await q.peek(1);
            assert.equal(q.stats().oldestTs, 123);
            await q.ack(await q.peek(1));
            await q.peek(1);
            assert.equal(q.stats().oldestTs, 456);
            await q.ack(await q.peek(1));
            assert.equal(q.stats().oldestTs, undefined);
            await q.enqueue({ id: 3 }); // no ts: nothing reported
            await q.peek(1);
            assert.equal(q.stats().oldestTs, undefined);
        });

        it('peek honours the byte budget but always returns at least one record', async function () {
            const one = recordBytes(rec(1));
            const q = await make({ peekMaxBytes: 2 * one });
            await q.enqueueMany([rec(1), rec(2), rec(3), rec(4)]);
            assert.deepEqual(ids(await q.peek(10)), [1, 2]);
            const q1 = await make({ peekMaxBytes: 1 });
            await q1.enqueueMany([rec(1), rec(2)]);
            assert.deepEqual(ids(await q1.peek(10)), [1]);
        });

        it('never throws synchronously, whatever the arguments', async function () {
            const q = await make();
            const ps = [];
            assert.doesNotThrow(() => {
                ps.push(q.peek('x'));
                ps.push(q.peek(-1));
                ps.push(q.peek(null));
                ps.push(q.ack(undefined));
                ps.push(q.ack(Symbol('s')));
                ps.push(q.enqueueMany(undefined));
                ps.push(q.enqueue(Symbol('s')));
                ps.push(q.deadLetter(undefined, undefined));
                ps.push(q.deadLetter(null, { toString() { throw new Error('nasty'); } }));
                ps.push(q.peek(Symbol('s')));
                ps.push(q.peek({ valueOf() { throw new Error('nasty valueOf'); } }));
                ps.push(q.ack([{ get pos() { throw new Error('nasty getter'); } }]));
                ps.push(q.enqueueMany(new Proxy([], {
                    get(target, key) {
                        if (key === 'length') {
                            throw new Error('nasty proxy');
                        }
                        return target[key];
                    }
                })));
                q.stats();
            });
            const settled = await Promise.allSettled(ps);
            assert.equal(settled.length, 13);
            assert.equal(settled[11].status, 'rejected');
            assert.equal(settled[12].status, 'rejected');
            assert.equal(q.size, 0);
            assert.equal((await q.peek('x')).length, 0);
        });

        describe('randomized model test', function () {
            this.timeout(60000);
            const variants = [
                { seed: 1, options: {} },
                { seed: 7, options: { maxMessages: 25 } },
                { seed: 42, options: { maxMessages: 25, overflow: 'drop-newest' } },
                { seed: 2024, options: {} }
            ];
            for (const { seed, options } of variants) {
                it(`matches an in-memory model (seed ${seed}, ${JSON.stringify(options)})`, async function () {
                    await modelRun(seed, options, 400);
                });
            }
        });

        /**
         * Random enqueue/peek/ack/clear/close+reopen operations compared, step by step, against a plain
         * array of record ids.
         */
        async function modelRun(seed, options, steps) {
            const rnd = prng(seed);
            const int = (n) => Math.floor(rnd() * n);
            const maxM = options.maxMessages || 0;
            const dropNewest = options.overflow === 'drop-newest';
            let q = await make(options);
            let model = [];
            let nextId = 1;
            let lastPeek = null;
            const trail = [];
            for (let step = 0; step < steps; step++) {
                const r = rnd();
                const where = () => `seed ${seed} step ${step}: ${trail.slice(-6).join(' | ')}`;
                if (r < 0.35) {
                    // a burst of concurrent appends (exercises group commit), applied to the model in call order
                    const calls = 1 + int(3);
                    const promises = [];
                    const expected = [];
                    for (let c = 0; c < calls; c++) {
                        const n = 1 + int(4);
                        const recs = [];
                        for (let i = 0; i < n; i++) {
                            recs.push({ id: nextId++, pad: 'x'.repeat(int(60)) });
                        }
                        let outcome = 0;
                        if (maxM && n > maxM) {
                            outcome = 'full';
                        } else if (maxM && model.length + n > maxM) {
                            if (dropNewest) {
                                outcome = 'full';
                            } else {
                                outcome = model.length + n - maxM;
                                model.splice(0, outcome);
                            }
                        }
                        if (outcome !== 'full') {
                            model.push(...recs.map((x) => x.id));
                        }
                        expected.push(outcome);
                        promises.push(n === 1 && rnd() < 0.5 ? q.enqueue(recs[0]) : q.enqueueMany(recs));
                    }
                    trail.push(`enqueue ${JSON.stringify(expected)}`);
                    const results = await Promise.allSettled(promises);
                    results.forEach((res, i) => {
                        if (expected[i] === 'full') {
                            assert.equal(res.status, 'rejected', where());
                            assert.ok(isQueueFull(res.reason), where());
                        } else {
                            assert.equal(res.status, 'fulfilled', `${where()} ${res.reason}`);
                            assert.equal(res.value.dropped, expected[i], where());
                        }
                    });
                } else if (r < 0.6) {
                    const max = 1 + int(15);
                    trail.push(`peek ${max}`);
                    lastPeek = await q.peek(max);
                    assert.deepEqual(ids(lastPeek), model.slice(0, max), where());
                } else if (r < 0.85) {
                    if (!lastPeek || lastPeek.length === 0) {
                        continue;
                    }
                    // ack a random prefix, or just one random entry (positional: everything before goes too)
                    const chosen = rnd() < 0.5
                        ? lastPeek.slice(0, 1 + int(lastPeek.length))
                        : [lastPeek[int(lastPeek.length)]];
                    const target = Math.max(...chosen.map((e) => e.record.id));
                    const before = model.length;
                    model = model.filter((id) => id > target);
                    trail.push(`ack <=${target}`);
                    assert.equal(await q.ack(chosen), before - model.length, where());
                } else if (r < 0.95) {
                    if (reopen) {
                        trail.push('reopen');
                        q = await again(q, options);
                        lastPeek = null; // positions belong to the old instance
                    }
                } else if (r < 0.97) {
                    trail.push('clear');
                    assert.equal(await q.clear(), model.length, where());
                    model = [];
                } else {
                    trail.push('stats');
                    assert.equal(q.stats().size, model.length, where());
                }
                assert.equal(q.size, model.length, where());
            }
            assert.deepEqual(ids(await q.peek(100000)), model);
            if (reopen) {
                q = await again(q, options);
                assert.equal(q.size, model.length);
                assert.deepEqual(ids(await q.peek(100000)), model);
            }
        }
    });
}

module.exports = { runQueueContract, prng, captureLogger };
