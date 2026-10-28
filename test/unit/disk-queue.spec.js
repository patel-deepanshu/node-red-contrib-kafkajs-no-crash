'use strict';

const assert = require('node:assert/strict');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const { DiskQueue, normalizeQueueOptions } = require('../../lib/disk-queue');
const { tmpDir } = require('../helpers/common');
const { runQueueContract, captureLogger, prng } = require('./queue-contract');

const CURSOR = 'cursor.json';

function lineBytes(record) {
    return Buffer.byteLength(JSON.stringify(record)) + 1;
}

function rec(id, extra) {
    return Object.assign({ id, ts: 1700000000000 + id }, extra);
}

function ids(entries) {
    return entries.map((e) => (e.corrupt ? 'corrupt' : e.record.id));
}

function range(from, to) {
    return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

function segFiles(dir) {
    return fs.readdirSync(dir).filter((n) => /^seg-\d{16}\.jsonl$/.test(n)).sort();
}

function newQueueDir() {
    return path.join(tmpDir('kafkajs-nc-dq-'), 'queue');
}

/** Child process for the SIGKILL test: argv = dir, first id, fsync (1/0), big batches (1/0). */
const CRASH_CHILD = `
const { DiskQueue } = require(${JSON.stringify(require.resolve('../../lib/disk-queue'))});
const [dir, startArg, fsyncArg, bigArg] = process.argv.slice(1);
const big = bigArg === '1';
let id = Number(startArg);
(async () => {
    const q = new DiskQueue({ dir, fsync: fsyncArg === '1', segmentMaxBytes: big ? 8 * 1024 * 1024 : 2048 });
    await q.open();
    process.send({ ready: true });
    (async () => {
        for (;;) {
            if (big) {
                const batch = [];
                for (let i = 0; i < 4000; i++) batch.push({ id: id++, pad: 'y'.repeat(800) });
                await q.enqueueMany(batch);
                process.send({ enqRange: [batch[0].id, batch[batch.length - 1].id] });
                continue;
            }
            const ps = [];
            for (let i = 1 + Math.floor(Math.random() * 20); i > 0; i--) {
                const myId = id++;
                const r = { id: myId, pad: 'x'.repeat(Math.floor(Math.random() * 200)) };
                ps.push((Math.random() < 0.3 ? q.enqueueMany([r]) : q.enqueue(r)).then(() => process.send({ enq: myId })));
            }
            await Promise.all(ps);
        }
    })();
    (async () => {
        for (;;) {
            const entries = await q.peek(1 + Math.floor(Math.random() * 30));
            if (!entries.length) {
                await new Promise((r) => setImmediate(r));
                continue;
            }
            const maxId = entries[entries.length - 1].record.id;
            process.send({ acking: maxId });
            await q.ack(entries);
            process.send({ acked: maxId });
        }
    })();
})();
`;

/** Run the crash child until a random SIGKILL; resolve with what it reported as done. */
function crashRound({ dir, start, fsync, big, killAfterMs }) {
    return new Promise((resolve, reject) => {
        const res = { enq: new Set(), acked: 0, acking: 0, segmentMaxBytes: big ? 8 * 1024 * 1024 : 2048 };
        const child = spawn(process.execPath, ['-e', CRASH_CHILD, dir, String(start), fsync ? '1' : '0', big ? '1' : '0'],
            { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
        const watchdog = setTimeout(() => child.kill('SIGKILL'), 20000);
        child.on('message', (m) => {
            if (m.ready) {
                setTimeout(() => child.kill('SIGKILL'), killAfterMs);
            } else if (m.enq !== undefined) {
                res.enq.add(m.enq);
            } else if (m.enqRange) {
                for (let x = m.enqRange[0]; x <= m.enqRange[1]; x++) {
                    res.enq.add(x);
                }
            } else if (m.acking !== undefined) {
                res.acking = Math.max(res.acking, m.acking);
            } else if (m.acked !== undefined) {
                res.acked = Math.max(res.acked, m.acked);
            }
        });
        child.on('error', (err) => {
            clearTimeout(watchdog);
            reject(err);
        });
        child.on('exit', () => {
            clearTimeout(watchdog);
            resolve(res);
        });
    });
}

/** Drain everything with peek/ack, returning the records in order. */
async function drain(q, batch = 100) {
    const out = [];
    for (;;) {
        const entries = await q.peek(batch);
        if (!entries.length) {
            return out;
        }
        out.push(...entries);
        await q.ack(entries);
    }
}

runQueueContract({
    name: 'DiskQueue',
    create: async (options) => {
        const q = new DiskQueue(Object.assign({ dir: newQueueDir() }, options));
        await q.open();
        return q;
    },
    reopen: async (q, options) => {
        await q.close();
        const q2 = new DiskQueue(Object.assign({}, options, { dir: q.dir }));
        await q2.open();
        return q2;
    },
    recordBytes: lineBytes
});

// same contract with tiny segments and tiny reads: exercises segment roll-over, cross-segment reads and
// records split across read chunks on every operation
runQueueContract({
    name: 'DiskQueue (tiny segments and read chunks, fsync off)',
    create: async (options) => {
        const q = new DiskQueue(Object.assign({ dir: newQueueDir(), segmentMaxBytes: 160, readChunkBytes: 13, fsync: false }, options));
        await q.open();
        return q;
    },
    reopen: async (q, options) => {
        await q.close();
        const q2 = new DiskQueue(Object.assign({ segmentMaxBytes: 160, readChunkBytes: 13, fsync: false }, options, { dir: q.dir }));
        await q2.open();
        return q2;
    },
    recordBytes: lineBytes
});

describe('DiskQueue', function () {
    this.timeout(20000);
    const live = new Set();

    async function openQueue(dir, options = {}) {
        const q = new DiskQueue(Object.assign({}, options, { dir }));
        live.add(q);
        await q.open();
        return q;
    }

    afterEach(async function () {
        const all = [...live];
        live.clear();
        await Promise.all(all.map((q) => q.close()));
    });

    describe('lifecycle and errors', function () {
        it('creates the directory recursively', async function () {
            const dir = path.join(tmpDir('kafkajs-nc-dq-'), 'a', 'b', 'c');
            const q = await openQueue(dir);
            assert.ok(fs.statSync(dir).isDirectory());
            assert.equal(q.dir, path.resolve(dir));
        });

        it('rejects every method before open() with a clear error, and never throws', async function () {
            const q = new DiskQueue({ dir: newQueueDir() });
            live.add(q);
            await assert.rejects(q.enqueue(rec(1)), /not open/);
            await assert.rejects(q.peek(), /not open/);
            await assert.rejects(q.ack([{ pos: { seg: 1, end: 1 } }]), /not open/);
            await assert.rejects(q.clear(), /not open/);
            await assert.rejects(q.deadLetter({ record: 1 }, 'x'), /not open/);
            assert.equal(q.size, 0);
        });

        it('queues operations requested while open() is still running', async function () {
            const q = new DiskQueue({ dir: newQueueDir() });
            live.add(q);
            const opening = q.open();
            const p = q.enqueue(rec(1));
            await opening;
            await p;
            assert.deepEqual(ids(await q.peek()), [1]);
        });

        it('rejects open() without a directory (the constructor never throws)', async function () {
            assert.doesNotThrow(() => new DiskQueue());
            assert.doesNotThrow(() => new DiskQueue(null));
            const q = new DiskQueue({});
            live.add(q);
            await assert.rejects(q.open(), /no queue directory/);
        });

        it('rejects open() with the path in the message when the directory is unusable, and can retry', async function () {
            const base = tmpDir('kafkajs-nc-dq-');
            const file = path.join(base, 'not-a-dir');
            fs.writeFileSync(file, 'x');
            const q = new DiskQueue({ dir: file });
            live.add(q);
            const err = await q.open().then(() => null, (e) => e);
            assert.ok(err, 'open() should fail');
            assert.ok(err.message.includes(file), err.message);
            await assert.rejects(q.enqueue(rec(1)), (e) => e.message.includes('not open') && e.message.includes(file));
            fs.unlinkSync(file);
            await q.open();
            await q.enqueue(rec(1));
            assert.deepEqual(ids(await q.peek()), [1]);
        });

        it('close() waits for pending appends before releasing the files', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            const p = q.enqueueMany([rec(1), rec(2)]);
            await q.close();
            await p;
            const q2 = await openQueue(dir);
            assert.deepEqual(ids(await q2.peek()), [1, 2]);
        });

        it('never throws from the constructor, even with a hostile options object', async function () {
            const q = new DiskQueue({ get dir() { throw new Error('nasty getter'); } });
            live.add(q);
            await assert.rejects(q.open(), /invalid options/);
        });

        it('ignores a seg-* entry that is not a regular file and never reuses its name', async function () {
            const dir = newQueueDir();
            fs.mkdirSync(path.join(dir, 'seg-0000000000000001.jsonl'), { recursive: true });
            const { logs, logger } = captureLogger();
            const q = await openQueue(dir, { logger });
            assert.ok(logs.warn.some((m) => /not a regular file/.test(m)));
            await q.enqueue(rec(1));
            assert.ok(fs.statSync(path.join(dir, 'seg-0000000000000002.jsonl')).isFile());
            assert.deepEqual(ids(await q.peek()), [1]);
        });

        it('parses the fsync option like every node setting: "off", "no", "false" disable it (MAINT-12)', function () {
            for (const off of [false, 'false', 'off', 'OFF', 'no', 0, '0']) {
                assert.equal(normalizeQueueOptions({ fsync: off }).fsync, false, String(off));
            }
            for (const on of [undefined, null, true, 'on', 'yes', 'garbage', 1]) {
                assert.equal(normalizeQueueOptions({ fsync: on }).fsync, true, String(on));
            }
        });

        it('normalises odd option values instead of throwing', async function () {
            const q = await openQueue(newQueueDir(), {
                maxBytes: 'abc', maxMessages: -5, segmentMaxBytes: '0', fsync: 'false', overflow: 'bogus',
                deadLetterMaxBytes: NaN, logger: 'not a logger'
            });
            await q.enqueueMany([rec(1), rec(2)]);
            assert.deepEqual(ids(await q.peek()), [1, 2]);
            assert.equal(q._fsync, false);
            assert.equal(q._overflow, 'drop-oldest');
            assert.equal(q._maxMessages, 0);
        });
    });

    describe('on-disk format', function () {
        it('stores one JSON record per line in seg-<16 digits>.jsonl', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), rec(2, { s: 'ü\n"x"' })]);
            const files = segFiles(dir);
            assert.deepEqual(files, ['seg-0000000000000001.jsonl']);
            const text = fs.readFileSync(path.join(dir, files[0]), 'utf8');
            assert.ok(text.endsWith('\n'));
            const lines = text.split('\n').slice(0, -1).map((l) => JSON.parse(l));
            assert.deepEqual(lines, [rec(1), rec(2, { s: 'ü\n"x"' })]);
            assert.equal(fs.existsSync(path.join(dir, CURSOR)), false, 'no cursor before the first ack');
        });

        it('writes cursor.json on ack and cleans the directory when the queue drains', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            await q.ack(await q.peek(1));
            const cursor = JSON.parse(fs.readFileSync(path.join(dir, CURSOR), 'utf8'));
            assert.deepEqual(cursor, { seg: 1, off: lineBytes(rec(1)) });
            await q.ack(await q.peek());
            assert.deepEqual(fs.readdirSync(dir), []);
            assert.equal(q.stats().segments, 0);
            await q.enqueue(rec(4));
            assert.deepEqual(segFiles(dir), ['seg-0000000000000002.jsonl'], 'sequence numbers keep increasing');
        });

        it('rolls over to new segments and deletes consumed ones', async function () {
            const dir = newQueueDir();
            const one = lineBytes(rec(1));
            const q = await openQueue(dir, { segmentMaxBytes: 3 * one });
            await q.enqueueMany(range(1, 10).map((i) => rec(i)));
            assert.equal(segFiles(dir).length, 4);
            assert.equal(q.stats().segments, 4);
            for (const f of segFiles(dir)) {
                assert.ok(fs.statSync(path.join(dir, f)).size <= 3 * one);
            }
            await q.ack(await q.peek(3));
            assert.deepEqual(segFiles(dir), ['seg-0000000000000002.jsonl', 'seg-0000000000000003.jsonl', 'seg-0000000000000004.jsonl']);
            assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir, CURSOR), 'utf8')), { seg: 2, off: 0 });
            await q.ack(await q.peek(4));
            assert.deepEqual(segFiles(dir), ['seg-0000000000000003.jsonl', 'seg-0000000000000004.jsonl']);
            assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir, CURSOR), 'utf8')), { seg: 3, off: one });
            assert.equal(q.stats().segments, 2);
            assert.deepEqual(ids(await drain(q)), [8, 9, 10]);
            assert.deepEqual(fs.readdirSync(dir), []);
        });

        it('gives a record larger than segmentMaxBytes a segment of its own', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 100 });
            await q.enqueue(rec(1));
            await q.enqueue(rec(2, { pad: 'x'.repeat(500) }));
            await q.enqueue(rec(3));
            const sizes = segFiles(dir).map((f) => fs.statSync(path.join(dir, f)).size);
            assert.deepEqual(sizes, [lineBytes(rec(1)), lineBytes(rec(2, { pad: 'x'.repeat(500) })), lineBytes(rec(3))]);
            assert.deepEqual(ids(await q.peek()), [1, 2, 3]);
        });

        it('fsync=false produces the same files', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { fsync: false });
            await q.enqueueMany([rec(1), rec(2)]);
            await q.ack(await q.peek(1));
            await q.close();
            const q2 = await openQueue(dir, { fsync: false });
            assert.deepEqual(ids(await q2.peek()), [2]);
        });
    });

    describe('restart and crash recovery', function () {
        it('a new instance on the same directory keeps exactly the pending records', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 200 });
            await q.enqueueMany(range(1, 10).map((i) => rec(i)));
            await q.ack(await q.peek(4));
            await q.close();
            const q2 = await openQueue(dir, { segmentMaxBytes: 200 });
            assert.equal(q2.size, 6);
            assert.equal(q2.bytes, range(5, 10).reduce((n, i) => n + lineBytes(rec(i)), 0));
            assert.equal(q2.stats().oldestTs, rec(5).ts, 'head is primed on open');
            assert.deepEqual(ids(await q2.peek()), range(5, 10));
            await q2.enqueue(rec(11));
            await q2.close();
            const q3 = await openQueue(dir, { segmentMaxBytes: 200 });
            assert.deepEqual(ids(await drain(q3)), range(5, 11));
        });

        it('records survive a crash right after enqueue resolved (no close)', async function () {
            const dir = newQueueDir();
            const crashed = await openQueue(dir);
            await crashed.enqueueMany([rec(1), rec(2), rec(3)]);
            // the process "dies": crashed is never closed before the new instance opens the same directory
            const q = new DiskQueue({ dir });
            await q.open();
            try {
                assert.equal(q.size, 3);
                assert.deepEqual(ids(await q.peek()), [1, 2, 3]);
            } finally {
                await q.close();
            }
        });

        it('a crash between ack and cursor write delivers the acked records again (at-least-once)', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany(range(1, 6).map((i) => rec(i)));
            await q.ack(await q.peek(2));
            const cursorAfter2 = fs.readFileSync(path.join(dir, CURSOR));
            await q.ack(await q.peek(2));
            const cursorAfter4 = fs.readFileSync(path.join(dir, CURSOR));
            assert.notDeepEqual(cursorAfter2, cursorAfter4);
            await q.close();
            // the new cursor was written to the tmp file but the rename never happened
            fs.writeFileSync(path.join(dir, CURSOR), cursorAfter2);
            fs.writeFileSync(path.join(dir, CURSOR + '.tmp'), cursorAfter4);
            const q2 = await openQueue(dir);
            assert.equal(fs.existsSync(path.join(dir, CURSOR + '.tmp')), false, 'stale tmp cursor removed');
            assert.deepEqual(ids(await q2.peek()), [3, 4, 5, 6]);
        });

        it('a crash between a drop-oldest append and its cursor write re-delivers the dropped record', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { maxMessages: 3 });
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            assert.equal((await q.enqueue(rec(4))).dropped, 1);
            await q.close();
            fs.unlinkSync(path.join(dir, CURSOR)); // the cursor update never reached the disk
            const q2 = await openQueue(dir, { maxMessages: 3 });
            assert.deepEqual(ids(await q2.peek()), [1, 2, 3, 4], 'nothing lost; temporarily above the limit');
            assert.equal((await q2.enqueue(rec(5))).dropped, 2);
            assert.deepEqual(ids(await q2.peek()), [3, 4, 5]);
        });

        it('truncates a torn write (half a JSON line) at the end of the newest segment', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            await q.close();
            const seg = path.join(dir, segFiles(dir)[0]);
            const goodSize = fs.statSync(seg).size;
            fs.appendFileSync(seg, '{"id":4,"ts":17000000'); // crash in the middle of an append
            const { logs, logger } = captureLogger();
            const q2 = await openQueue(dir, { logger });
            assert.equal(q2.size, 3);
            assert.deepEqual(ids(await q2.peek()), [1, 2, 3]);
            assert.equal(fs.statSync(seg).size, goodSize);
            assert.ok(logs.warn.some((m) => /incomplete record/.test(m) && m.includes(seg)), logs.warn.join('\n'));
            await q2.enqueue(rec(5));
            assert.deepEqual(ids(await q2.peek()), [1, 2, 3, 5]);
            await q2.close();
            const lines = fs.readFileSync(seg, 'utf8').split('\n');
            assert.equal(lines.pop(), '');
            assert.deepEqual(lines.map((l) => JSON.parse(l).id), [1, 2, 3, 5]);
        });

        it('truncates a newest segment that holds only a torn record', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)) });
            await q.enqueueMany(range(1, 4).map((i) => rec(i)));
            await q.close();
            // the crash happened right after a new segment was created
            const torn = path.join(dir, 'seg-0000000000000003.jsonl');
            fs.writeFileSync(torn, '{"id":5,"ts":1');
            const q2 = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)) });
            assert.equal(q2.size, 4);
            assert.equal(fs.statSync(torn).size, 0);
            await q2.enqueue(rec(6));
            assert.deepEqual(ids(await drain(q2)), [1, 2, 3, 4, 6]);
        });

        it('returns a corrupt middle line as {corrupt, raw, pos} and can dead-letter it', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            await q.close();
            const seg = path.join(dir, segFiles(dir)[0]);
            const lines = fs.readFileSync(seg, 'utf8').split('\n');
            lines[1] = 'this is not json';
            fs.writeFileSync(seg, lines.join('\n'));
            const q2 = await openQueue(dir);
            assert.equal(q2.size, 3);
            const entries = await q2.peek();
            assert.deepEqual(ids(entries), [1, 'corrupt', 3]);
            assert.equal(entries[1].raw, 'this is not json');
            assert.equal(entries[1].pos.seg, 1);
            await q2.deadLetter(entries[1], new Error('bad line'));
            assert.equal(await q2.ack(entries), 3);
            const dead = fs.readFileSync(path.join(dir, 'dead-letter.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
            assert.equal(dead.length, 1);
            assert.equal(dead[0].raw, 'this is not json');
            assert.equal(typeof dead[0].ts, 'string');
            assert.equal(typeof dead[0].error, 'object');
            assert.match(dead[0].error.message, /bad line/);
            assert.equal(q2.stats().deadLettered, 1);
        });

        it('returns a blank line as a corrupt entry (it still counts and can be acked)', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueue(rec(1));
            await q.close();
            fs.appendFileSync(path.join(dir, segFiles(dir)[0]), '\n' + JSON.stringify(rec(2)) + '\n');
            const q2 = await openQueue(dir);
            assert.equal(q2.size, 3);
            const entries = await q2.peek();
            assert.deepEqual(ids(entries), [1, 'corrupt', 2]);
            assert.equal(entries[1].raw, '');
            await q2.ack(entries);
            assert.equal(q2.size, 0);
        });

        it('a corrupt cursor.json logs a warning and replays from the oldest segment', async function () {
            for (const garbage of ['{{{ not json', '{"seg":"x","off":1}', '[]', '']) {
                const dir = newQueueDir();
                const q = await openQueue(dir);
                await q.enqueueMany(range(1, 5).map((i) => rec(i)));
                await q.ack(await q.peek(2));
                await q.close();
                fs.writeFileSync(path.join(dir, CURSOR), garbage);
                const { logs, logger } = captureLogger();
                const q2 = await openQueue(dir, { logger });
                assert.deepEqual(ids(await q2.peek()), [1, 2, 3, 4, 5], `cursor ${JSON.stringify(garbage)}`);
                assert.ok(logs.warn.some((m) => /cursor/.test(m)), logs.warn.join('\n'));
            }
        });

        it('a cursor pointing past the newest segment is treated as corrupt (replay, not loss)', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)) });
            await q.enqueueMany(range(1, 5).map((i) => rec(i)));
            await q.close();
            assert.equal(segFiles(dir).length, 3);
            fs.writeFileSync(path.join(dir, CURSOR), JSON.stringify({ seg: 999, off: 0 }));
            const { logs, logger } = captureLogger();
            const q2 = await openQueue(dir, { logger });
            assert.deepEqual(ids(await q2.peek()), [1, 2, 3, 4, 5]);
            assert.equal(logs.warn.length, 1);
            assert.equal(segFiles(dir).length, 3, 'no segment deleted');
        });

        it('a missing cursor.json starts at the oldest segment without a warning', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), rec(2)]);
            await q.close();
            const { logs, logger } = captureLogger();
            const q2 = await openQueue(dir, { logger });
            assert.deepEqual(ids(await q2.peek()), [1, 2]);
            assert.deepEqual(logs.warn, []);
        });

        it('clamps a cursor offset beyond the end of its segment', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)) });
            await q.enqueueMany(range(1, 4).map((i) => rec(i)));
            await q.close();
            fs.writeFileSync(path.join(dir, CURSOR), JSON.stringify({ seg: 1, off: 1e9 }));
            const { logs, logger } = captureLogger();
            const q2 = await openQueue(dir, { logger, segmentMaxBytes: 2 * lineBytes(rec(1)) });
            assert.deepEqual(ids(await q2.peek()), [3, 4]);
            assert.ok(logs.warn.some((m) => /beyond the end/.test(m)));
        });

        it('replays a segment when the cursor offset is not at a record boundary', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueueMany(range(1, 3).map((i) => rec(i)));
            await q.close();
            fs.writeFileSync(path.join(dir, CURSOR), JSON.stringify({ seg: 1, off: lineBytes(rec(1)) + 3 }));
            const { logs, logger } = captureLogger();
            const q2 = await openQueue(dir, { logger });
            assert.deepEqual(ids(await q2.peek()), [1, 2, 3]);
            assert.ok(logs.warn.some((m) => /record boundary/.test(m)));
        });

        it('deletes segments before the cursor that a crash left behind', async function () {
            const dir = newQueueDir();
            const two = 2 * lineBytes(rec(1));
            const q = await openQueue(dir, { segmentMaxBytes: two });
            await q.enqueueMany(range(1, 6).map((i) => rec(i)));
            const first = path.join(dir, 'seg-0000000000000001.jsonl');
            const firstContent = fs.readFileSync(first);
            await q.ack(await q.peek(2));
            assert.equal(fs.existsSync(first), false);
            await q.close();
            fs.writeFileSync(first, firstContent); // deletion "did not happen"
            const q2 = await openQueue(dir, { segmentMaxBytes: two });
            assert.equal(fs.existsSync(first), false);
            assert.deepEqual(ids(await q2.peek()), [3, 4, 5, 6]);
        });

        it('cleans up a drained queue whose cleanup was interrupted', async function () {
            const dir = newQueueDir();
            fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(path.join(dir, CURSOR), JSON.stringify({ seg: 7, off: 120 }));
            const q = await openQueue(dir);
            assert.equal(q.size, 0);
            assert.equal(fs.existsSync(path.join(dir, CURSOR)), false);
            await q.enqueue(rec(1));
            assert.deepEqual(segFiles(dir), ['seg-0000000000000008.jsonl'], 'never reuses a sequence number');
            // an empty leftover segment (crash right after it was created) is removed as well
            const dir2 = newQueueDir();
            fs.mkdirSync(dir2, { recursive: true });
            fs.writeFileSync(path.join(dir2, 'seg-0000000000000003.jsonl'), '');
            const q2 = await openQueue(dir2);
            assert.equal(q2.size, 0);
            assert.deepEqual(fs.readdirSync(dir2), []);
        });

        it('persists the final cursor before deleting a drained queue (no re-delivery if deletion fails)', async function () {
            const dir = newQueueDir();
            const { logs, logger } = captureLogger();
            const q = await openQueue(dir, { logger });
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            await q.ack(await q.peek(1));
            const realUnlink = fs.promises.unlink;
            fs.promises.unlink = async function (p) {
                if (/seg-\d{16}\.jsonl$/.test(p)) {
                    throw Object.assign(new Error('EBUSY: resource busy or locked'), { code: 'EBUSY' });
                }
                return realUnlink.apply(this, arguments);
            };
            try {
                assert.equal(await q.ack(await q.peek()), 2);
            } finally {
                fs.promises.unlink = realUnlink;
            }
            assert.equal(q.size, 0);
            assert.ok(logs.warn.some((m) => /EBUSY/.test(m)), logs.warn.join('\n'));
            assert.equal(segFiles(dir).length, 1, 'segment still there');
            assert.ok(fs.existsSync(path.join(dir, CURSOR)), 'cursor kept to mark it consumed');
            await q.enqueue(rec(4));
            await q.close();
            const q2 = await openQueue(dir);
            assert.deepEqual(ids(await q2.peek()), [4], 'acked records are not delivered again');
            assert.deepEqual(segFiles(dir), ['seg-0000000000000002.jsonl'], 'leftover segment removed on open');
        });

        /** Make deleting the segment files that match `re` fail with `code` until the returned restore() is called. */
        function failUnlink(re, code, times = Infinity) {
            const realUnlink = fs.promises.unlink;
            let left = times;
            fs.promises.unlink = async function (p) {
                if (left > 0 && re.test(p)) {
                    left--;
                    throw Object.assign(new Error(`${code}: resource busy or locked, unlink '${p}'`), { code });
                }
                return realUnlink.apply(this, arguments);
            };
            return () => {
                fs.promises.unlink = realUnlink;
            };
        }

        it('does not send a drained queue again when an older segment could not be deleted (DUP-1)', async function () {
            const dir = newQueueDir();
            const { logger } = captureLogger();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)), logger });
            await q.enqueueMany(range(1, 6).map((i) => rec(i)));
            assert.equal(segFiles(dir).length, 3);
            const restore = failUnlink(/seg-0000000000000001\.jsonl$/, 'EBUSY', 1);
            try {
                assert.deepEqual(ids(await drain(q)), range(1, 6));
            } finally {
                restore();
            }
            assert.deepEqual(segFiles(dir), ['seg-0000000000000001.jsonl'], 'the first segment was left behind');
            await q.close();
            const { logs, logger: logger2 } = captureLogger();
            const q2 = await openQueue(dir, { logger: logger2 });
            assert.equal(q2.size, 0);
            assert.deepEqual(ids(await q2.peek()), [], 'acknowledged records are not sent again');
            assert.deepEqual(fs.readdirSync(dir), [], 'the leftover segment and the cursor are cleaned up');
            assert.deepEqual(logs.warn, []);
            await q2.enqueue(rec(7));
            assert.deepEqual(ids(await q2.peek()), [7]);
        });

        it('a segment left behind is not forgotten when a later drain succeeds (DUP-1)', async function () {
            const dir = newQueueDir();
            const { logger } = captureLogger();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)), logger });
            await q.enqueueMany(range(1, 6).map((i) => rec(i)));
            const restore = failUnlink(/seg-0000000000000001\.jsonl$/, 'EPERM', 1);
            try {
                await drain(q);
            } finally {
                restore();
            }
            await q.enqueueMany([rec(7), rec(8)]);
            assert.deepEqual(ids(await drain(q)), [7, 8]);
            assert.deepEqual(fs.readdirSync(dir), [], 'this drain also deleted the leftover segment');
            await q.close();
            const q2 = await openQueue(dir);
            assert.deepEqual(ids(await q2.peek()), [], 'nothing is sent again after a restart');
        });

        it('keeps marking a leftover segment as consumed while it still cannot be deleted (DUP-1)', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 2 * lineBytes(rec(1)), logger: captureLogger().logger });
            await q.enqueueMany(range(1, 4).map((i) => rec(i)));
            const restore = failUnlink(/seg-0000000000000001\.jsonl$/, 'EBUSY');
            try {
                await drain(q);
                await q.close();
                const q2 = await openQueue(dir, { logger: captureLogger().logger });
                assert.deepEqual(ids(await q2.peek()), []);
                await q2.enqueue(rec(5));
                assert.deepEqual(ids(await drain(q2)), [5]);
                await q2.close();
            } finally {
                restore();
            }
            assert.deepEqual(segFiles(dir), ['seg-0000000000000001.jsonl']);
            const q3 = await openQueue(dir);
            assert.deepEqual(ids(await q3.peek()), [], 'still not sent again');
            assert.deepEqual(fs.readdirSync(dir), [], 'deleted once that is possible');
        });

        it('survives real SIGKILL crashes during concurrent enqueue/ack traffic', async function () {
            // A child process enqueues (small concurrent bursts, or multi-MB batches that a SIGKILL can
            // tear mid-write) while a consumer peeks and acks; it reports every resolved enqueue and every
            // ack (before and after) over IPC and is killed at a random moment. After each crash:
            // no confirmed record lost, no hole, no duplicate/reordering, no confirmed ack undone, no
            // corrupt entry (a torn tail must be truncated).
            this.timeout(60000);
            const rnd = prng(99);
            const base = tmpDir('kafkajs-nc-dq-kill-');
            const plan = [
                { big: false, fsync: true }, { big: false, fsync: false }, { big: false, fsync: true },
                { big: true, fsync: false }, { big: true, fsync: true }
            ];
            for (let r = 0; r < plan.length; r++) {
                const { big, fsync } = plan[r];
                const dir = path.join(base, big ? 'big' : 'small'); // reused: later rounds start from leftovers
                const start = (r + 1) * 1e6;
                const res = await crashRound({ dir, start, fsync, big, killAfterMs: 40 + Math.floor(rnd() * 200) });
                const label = `round ${r} ${JSON.stringify({ big, fsync, enq: res.enq.size, acked: res.acked, acking: res.acking })}`;
                const q = await openQueue(dir, { peekMaxBytes: 1e12, segmentMaxBytes: res.segmentMaxBytes });
                const after = await q.peek(1e7);
                await q.close();
                assert.ok(after.every((e) => !e.corrupt), `${label}: corrupt entry after recovery`);
                const got = after.map((e) => e.record.id);
                for (let i = 1; i < got.length; i++) {
                    assert.ok(got[i] > got[i - 1], `${label}: order/duplicate at ${i}: ${got[i - 1]} -> ${got[i]}`);
                }
                const present = new Set(got);
                const head = got.length ? got[0] : Infinity;
                const lost = [...res.enq].filter((x) => (x > res.acking || x >= head) && !present.has(x));
                assert.deepEqual(lost.slice(0, 5), [], `${label}: confirmed records lost or holes`);
                const undone = got.filter((x) => x >= start && x <= res.acked);
                assert.deepEqual(undone.slice(0, 5), [], `${label}: confirmed acks undone`);
            }
        });

        it('drop-oldest state survives a restart', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { maxMessages: 5 });
            await q.enqueueMany(range(1, 5).map((i) => rec(i)));
            await q.close();
            const q2 = await openQueue(dir, { maxMessages: 5 });
            assert.equal((await q2.enqueueMany([rec(6), rec(7)])).dropped, 2);
            await q2.close();
            const q3 = await openQueue(dir, { maxMessages: 5 });
            assert.deepEqual(ids(await q3.peek()), [3, 4, 5, 6, 7]);
        });
    });

    describe('reading', function () {
        it('reads records split across read chunks, including multi-byte UTF-8', async function () {
            const dir = newQueueDir();
            const records = range(1, 30).map((i) => rec(i, { s: 'äöü€😀'.repeat(i % 5), n: i }));
            const q = await openQueue(dir, { readChunkBytes: 7, segmentMaxBytes: 300 });
            await q.enqueueMany(records);
            assert.deepEqual((await q.peek(1000)).map((e) => e.record), records);
            await q.close();
            const q2 = await openQueue(dir, { readChunkBytes: 5, segmentMaxBytes: 300 });
            assert.equal(q2.size, 30, 'count on open with tiny chunks');
            assert.deepEqual((await drain(q2, 7)).map((e) => e.record), records);
        });

        it('handles a record much larger than the read chunk', async function () {
            const dir = newQueueDir();
            const big = rec(2, { blob: 'z'.repeat(300 * 1024) });
            const q = await openQueue(dir);
            await q.enqueueMany([rec(1), big, rec(3)]);
            const entries = await q.peek();
            assert.deepEqual(entries.map((e) => e.record), [rec(1), big, rec(3)]);
            await q.close();
            const q2 = await openQueue(dir);
            assert.equal(q2.size, 3);
            assert.deepEqual((await drain(q2, 1)).map((e) => e.record.id), [1, 2, 3]);
        });

        it('sees records appended after a previous peek', async function () {
            const q = await openQueue(newQueueDir(), { segmentMaxBytes: 100 });
            await q.enqueue(rec(1));
            assert.deepEqual(ids(await q.peek()), [1]);
            await q.enqueueMany([rec(2), rec(3)]);
            assert.deepEqual(ids(await q.peek()), [1, 2, 3]);
            await q.ack(await q.peek(2));
            await q.enqueue(rec(4));
            assert.deepEqual(ids(await q.peek()), [3, 4]);
        });

        it('does not re-read the segment from the start for every peek', async function () {
            const q = await openQueue(newQueueDir(), { fsync: false });
            await q.enqueueMany(range(1, 5000).map((i) => rec(i, { pad: 'p'.repeat(40) })));
            const total = q.bytes;
            const before = q._readOps;
            const got = await drain(q, 10);
            assert.equal(got.length, 5000);
            const reads = q._readOps - before;
            assert.ok(reads <= Math.ceil(total / (64 * 1024)) + 2, `${reads} reads for ${total} bytes`);
        });

        it('an ack with a position beyond the data consumes everything without error', async function () {
            const q = await openQueue(newQueueDir(), { segmentMaxBytes: 100 });
            await q.enqueueMany(range(1, 5).map((i) => rec(i)));
            assert.equal(await q.ack([{ pos: { seg: 999, end: 1 } }]), 5);
            assert.equal(q.size, 0);
            assert.deepEqual(await q.peek(), []);
        });
    });

    describe('group commit and performance', function () {
        it('coalesces 100 concurrent enqueues into very few writes and keeps their order', async function () {
            const q = await openQueue(newQueueDir(), { fsync: true });
            await q.enqueue(rec(0));
            const before = q._writeOps;
            await Promise.all(range(1, 100).map((i) => q.enqueue(rec(i))));
            const writes = q._writeOps - before;
            assert.ok(writes >= 1 && writes <= 3, `${writes} writes`);
            assert.deepEqual(ids(await q.peek(1000)), range(0, 100));
        });

        it('coalesces appends requested while a write is in progress into the next write', async function () {
            const q = await openQueue(newQueueDir(), { fsync: true });
            const first = q.enqueue(rec(1));
            await new Promise((resolve) => setImmediate(resolve)); // the first write is now in flight
            const rest = range(2, 51).map((i) => q.enqueue(rec(i)));
            await Promise.all([first, ...rest]);
            assert.ok(q._writeOps <= 3, `${q._writeOps} writes`);
            assert.deepEqual(ids(await q.peek(1000)), range(1, 51));
        });

        it('enqueueMany of 10k small records takes < 2 s with fsync off (and is fast with fsync on)', async function () {
            const records = range(1, 10000).map((i) => ({ v: 1, t: 'topic', tf: 0, ts: Date.now(), m: { v: { s: `payload ${i}` } } }));
            for (const fsync of [false, true]) {
                const q = await openQueue(newQueueDir(), { fsync });
                const t0 = Date.now();
                await q.enqueueMany(records);
                const ms = Date.now() - t0;
                assert.ok(ms < 2000, `fsync=${fsync}: ${ms} ms`);
                assert.equal(q.size, 10000);
                const t1 = Date.now();
                await Promise.all(records.map((r) => q.enqueue(r)));
                const ms2 = Date.now() - t1;
                assert.ok(ms2 < 4000, `fsync=${fsync}: 10k concurrent enqueue() took ${ms2} ms`);
                assert.equal(q.size, 20000);
            }
        });

        it('a 50k-record backlog survives a restart and drains in order in < 10 s', async function () {
            this.timeout(30000);
            const dir = newQueueDir();
            const t0 = Date.now();
            const q = await openQueue(dir, { segmentMaxBytes: 512 * 1024 });
            for (let b = 0; b < 10; b++) {
                await q.enqueueMany(range(b * 5000 + 1, (b + 1) * 5000).map((i) => rec(i, { m: { v: { s: 'x'.repeat(50) } } })));
            }
            await q.close();
            const q2 = await openQueue(dir, { segmentMaxBytes: 512 * 1024 });
            assert.equal(q2.size, 50000);
            let expected = 1;
            for (;;) {
                const entries = await q2.peek(100);
                if (!entries.length) {
                    break;
                }
                for (const e of entries) {
                    assert.equal(e.record.id, expected++);
                }
                await q2.ack(entries);
            }
            assert.equal(expected, 50001);
            assert.equal(q2.size, 0);
            assert.deepEqual(fs.readdirSync(dir), []);
            const ms = Date.now() - t0;
            assert.ok(ms < 10000, `${ms} ms`);
        });
    });

    describe('failures', function () {
        it('a failed (partial) write rejects with the file path and leaves no partial record', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueue(rec(1));
            const fh = q._writer.fh;
            const realWrite = fh.write;
            fh.write = async function (buf, off, len, pos) {
                await realWrite.call(this, buf, off, Math.floor(len / 2), pos);
                const e = new Error('ENOSPC: no space left on device');
                e.code = 'ENOSPC';
                throw e;
            };
            const err = await q.enqueueMany([rec(2), rec(3)]).then(() => null, (e) => e);
            delete fh.write;
            assert.ok(err, 'enqueue should fail');
            assert.equal(err.code, 'ENOSPC');
            assert.ok(err.message.includes(path.join(dir, 'seg-0000000000000001.jsonl')), err.message);
            assert.equal(q.size, 1);
            await q.enqueue(rec(4));
            assert.deepEqual(ids(await q.peek()), [1, 4]);
            await q.close();
            const q2 = await openQueue(dir);
            assert.deepEqual(ids(await q2.peek()), [1, 4]);
        });

        it('when the rollback also fails, later appends go to a new segment and nothing valid is lost', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir);
            await q.enqueue(rec(1));
            const fh = q._writer.fh;
            const realWrite = fh.write;
            fh.write = async function (buf, off, len, pos) {
                await realWrite.call(this, buf, off, Math.floor(len / 2), pos);
                throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' });
            };
            fh.truncate = async function () {
                throw Object.assign(new Error('EIO: i/o error'), { code: 'EIO' });
            };
            await assert.rejects(q.enqueue(rec(2)), { code: 'EIO' });
            assert.equal(q.size, 1);
            await q.enqueue(rec(3));
            assert.deepEqual(segFiles(dir), ['seg-0000000000000001.jsonl', 'seg-0000000000000002.jsonl']);
            assert.deepEqual(ids(await q.peek()), [1, 3], 'reads stop at the committed size');
            await q.close();
            // after a restart the half-written bytes surface as one corrupt entry (to be dead-lettered)
            const q2 = await openQueue(dir);
            assert.equal(q2.size, 3, 'the torn bytes count as one entry');
            const entries = await q2.peek();
            assert.deepEqual(ids(entries), [1, 'corrupt', 3]);
            assert.equal(await q2.ack(entries), 3);
            assert.equal(q2.size, 0);
        });

        it('ignores directory fsync errors of platforms that do not support it, but not real I/O errors', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { fsync: true, segmentMaxBytes: 50 });
            const realOpen = fs.promises.open;
            let code = 'EISDIR';
            fs.promises.open = async function (p, ...rest) {
                if (p === q.dir) {
                    throw Object.assign(new Error(`${code}: directory sync not possible`), { code });
                }
                return realOpen.call(this, p, ...rest);
            };
            try {
                await q.enqueue(rec(1));
                await q.enqueue(rec(2));
                assert.equal(await q.ack(await q.peek(1)), 1);
                code = 'EIO';
                const err = await q.enqueue(rec(3)).then(() => null, (e) => e);
                assert.ok(err && err.code === 'EIO', String(err));
                assert.ok(err.message.includes(q.dir), err.message);
                assert.deepEqual(segFiles(dir), ['seg-0000000000000002.jsonl'], 'the new segment was rolled back');
            } finally {
                fs.promises.open = realOpen;
            }
            await q.enqueue(rec(4));
            assert.deepEqual(ids(await q.peek()), [2, 4]);
        });

        it('carries on without fsync (warning once) on file systems that cannot sync, but fails on real I/O errors', async function () {
            const dir = newQueueDir();
            const probe = await fs.promises.open(path.join(tmpDir('kafkajs-nc-dq-'), 'probe'), 'w');
            const FileHandle = probe.constructor;
            await probe.close();
            const realSync = FileHandle.prototype.datasync;
            let code = 'EINVAL';
            FileHandle.prototype.datasync = async function () {
                throw Object.assign(new Error(`${code}: sync not possible`), { code });
            };
            const { logs, logger } = captureLogger();
            try {
                const q = await openQueue(dir, { logger, fsync: true });
                await q.enqueue(rec(1));
                await q.enqueue(rec(2));
                await q.ack(await q.peek(1));
                assert.equal(logs.warn.filter((m) => /does not support fsync/.test(m)).length, 1);
                const q2 = await openQueue(newQueueDir(), { fsync: true });
                code = 'EIO';
                const err = await q2.enqueue(rec(1)).then(() => null, (e) => e);
                assert.ok(err && err.code === 'EIO', String(err));
                assert.ok(err.message.includes(q2.dir), err.message);
                assert.equal(q2.size, 0);
            } finally {
                FileHandle.prototype.datasync = realSync;
            }
        });

        it('a failed cursor write is logged and the ack still takes effect', async function () {
            const { logs, logger } = captureLogger();
            const q = await openQueue(newQueueDir(), { logger });
            await q.enqueueMany([rec(1), rec(2), rec(3)]);
            q._persistCursor = async () => {
                throw new Error('disk on fire');
            };
            assert.equal(await q.ack(await q.peek(1)), 1);
            delete q._persistCursor;
            assert.ok(logs.error.some((m) => /disk on fire/.test(m)));
            assert.deepEqual(ids(await q.peek()), [2, 3]);
        });

        it('a read error rejects peek with the path and the next peek recovers', async function () {
            const q = await openQueue(newQueueDir());
            await q.enqueueMany([rec(1), rec(2)]);
            const realRead = q._readMore;
            q._readMore = async function () {
                q._readMore = realRead;
                throw new Error('Offline queue: cannot read /some/file: EIO');
            };
            await assert.rejects(q.peek(), /cannot read/);
            assert.deepEqual(ids(await q.peek()), [1, 2]);
        });
    });

    describe('clear and dead letters', function () {
        it('clear() removes the segment files and the cursor', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { segmentMaxBytes: 60 });
            await q.enqueueMany(range(1, 5).map((i) => rec(i)));
            await q.ack(await q.peek(1));
            assert.equal(await q.clear(), 4);
            assert.deepEqual(fs.readdirSync(dir), []);
            await q.close();
            const q2 = await openQueue(dir);
            assert.equal(q2.size, 0);
        });

        it('writes dead letters as JSON lines and rotates the file', async function () {
            const dir = newQueueDir();
            const q = await openQueue(dir, { deadLetterMaxBytes: 600 });
            await q.enqueueMany(range(1, 10).map((i) => rec(i, { pad: 'd'.repeat(60) })));
            const entries = await q.peek();
            for (const e of entries) {
                await q.deadLetter(e, e.record.id % 2 ? new Error(`boom ${e.record.id}`) : 'expired');
            }
            assert.equal(q.stats().deadLettered, 10);
            const current = path.join(dir, 'dead-letter.jsonl');
            const rotated = path.join(dir, 'dead-letter.1.jsonl');
            assert.ok(fs.existsSync(rotated), 'rotated file exists');
            assert.ok(fs.statSync(current).size <= 600);
            assert.ok(fs.statSync(rotated).size <= 600);
            const lines = [rotated, current]
                .flatMap((f) => fs.readFileSync(f, 'utf8').trim().split('\n'))
                .map((l) => JSON.parse(l));
            const last = lines[lines.length - 1];
            assert.equal(last.record.id, 10);
            assert.equal(last.error, 'expired');
            const odd = lines.find((l) => l.record.id % 2 === 1);
            assert.equal(typeof odd.error, 'object');
            assert.match(odd.error.message, /boom/);
            assert.ok(!Number.isNaN(Date.parse(odd.ts)));
            assert.equal(q.size, 10, 'dead-lettering does not remove');
        });

        it('logs how much it deletes when a rotation replaces an older dead-letter.1.jsonl (DLQ-1)', async function () {
            const dir = newQueueDir();
            const { logs, logger } = captureLogger();
            const q = await openQueue(dir, { deadLetterMaxBytes: 600, logger });
            await q.enqueueMany(range(1, 30).map((i) => rec(i, { pad: 'y'.repeat(100) })));
            const entries = await q.peek();
            let firstRotation = true;
            for (const e of entries) {
                const rotated = path.join(dir, 'dead-letter.1.jsonl');
                const before = fs.existsSync(rotated) ? fs.statSync(rotated).size : 0;
                await q.deadLetter(e, 'expired');
                if (before > 0 && fs.statSync(rotated).size !== before) {
                    // the previous dead-letter.1.jsonl was just replaced: its bytes must be reported
                    const line = logs.warn.find((m) => m.includes(`${before} bytes of older dead letters were deleted`));
                    assert.ok(line, `rotation that deleted ${before} bytes is logged:\n${logs.warn.join('\n')}`);
                    assert.match(line, /dead-letter\.jsonl reached 600 bytes and replaced dead-letter\.1\.jsonl/);
                    firstRotation = false;
                }
            }
            assert.equal(firstRotation, false, 'the test rotated more than once');
            assert.ok(logs.warn.length >= 2);
        });
    });
});
