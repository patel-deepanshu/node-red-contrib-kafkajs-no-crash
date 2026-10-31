'use strict';

const assert = require('node:assert/strict');

const { MemoryQueue, DEAD_LETTER_KEEP } = require('../../lib/memory-queue');
const { runQueueContract, captureLogger } = require('./queue-contract');

runQueueContract({
    name: 'MemoryQueue',
    create: async (options) => {
        const q = new MemoryQueue(options);
        await q.open();
        return q;
    },
    // no reopen: a memory queue does not survive a restart by design
    recordBytes: (record) => Buffer.byteLength(JSON.stringify(record))
});

describe('MemoryQueue', function () {
    it('needs no directory and reports no segments', async function () {
        const q = new MemoryQueue({ dir: '/ignored', fsync: true, segmentMaxBytes: 1 });
        assert.equal(q.dir, null);
        await q.enqueue({ id: 1 });
        assert.equal(q.stats().segments, 0);
        await q.close();
    });

    it('works without calling open() (open is optional)', async function () {
        const q = new MemoryQueue();
        await q.enqueue({ id: 1 });
        assert.deepEqual((await q.peek()).map((e) => e.record.id), [1]);
        await q.close();
    });

    it('never throws from the constructor', function () {
        assert.doesNotThrow(() => new MemoryQueue());
        assert.doesNotThrow(() => new MemoryQueue(null));
        assert.doesNotThrow(() => new MemoryQueue({ maxBytes: 'x', maxMessages: {}, overflow: 7, logger: 5 }));
        assert.doesNotThrow(() => new MemoryQueue({ get maxBytes() { throw new Error('nasty getter'); } }));
    });

    it('counts bytes as the JSON length of each record', async function () {
        const q = new MemoryQueue();
        const r = { id: 1, s: 'ü€' };
        await q.enqueue(r);
        assert.equal(q.bytes, Buffer.byteLength(JSON.stringify(r)));
        await q.close();
    });

    it('uses positions { seg: 0, end: <sequence> } and ignores positions of other queues', async function () {
        const q = new MemoryQueue();
        await q.enqueueMany([{ id: 1 }, { id: 2 }]);
        const entries = await q.peek();
        assert.deepEqual(entries.map((e) => e.pos), [{ seg: 0, end: 1 }, { seg: 0, end: 2 }]);
        assert.equal(await q.ack([{ pos: { seg: 3, end: 100 } }]), 0);
        assert.equal(q.size, 2);
        await q.close();
    });

    it('keeps the most recent dead letters for inspection', async function () {
        const q = new MemoryQueue();
        for (let i = 0; i < 120; i++) {
            await q.deadLetter({ record: { id: i }, pos: { seg: 0, end: i + 1 } }, i % 2 ? 'expired' : new Error(`e${i}`));
        }
        const dl = q.getDeadLetters();
        assert.equal(dl.length, 100);
        assert.equal(dl[0].record.id, 20);
        assert.equal(dl[99].record.id, 119);
        assert.equal(dl[99].error, 'expired');
        assert.match(dl[98].error.message, /e118/);
        assert.equal(q.stats().deadLettered, 120);
        assert.equal(DEAD_LETTER_KEEP, 100);
        await q.close();
    });

    it('getDeadLetters() returns copies, also after close() (MAINT-7: the only place their content is kept)', async function () {
        const q = new MemoryQueue();
        await q.deadLetter({ record: { id: 1, value: 'payload' }, pos: { seg: 0, end: 1 } }, 'expired');
        const first = q.getDeadLetters();
        first[0].record.value = 'changed by a caller';
        first.push({ bogus: true });
        await q.close();
        const again = q.getDeadLetters();
        assert.equal(again.length, 1);
        assert.deepEqual(again[0].record, { id: 1, value: 'payload' });
        assert.equal(again[0].error, 'expired');
        assert.ok(!Number.isNaN(Date.parse(again[0].ts)));
    });

    it('warns when it is closed with unsent records', async function () {
        const { logs, logger } = captureLogger();
        const q = new MemoryQueue({ logger });
        await q.enqueueMany([{ id: 1 }, { id: 2 }]);
        await q.close();
        assert.equal(logs.warn.length, 1);
        assert.match(logs.warn[0], /2 unsent/);
        assert.equal(q.size, 0);
    });

    it('stays fast with a large backlog (compacts consumed slots)', async function () {
        const q = new MemoryQueue();
        const t0 = Date.now();
        await q.enqueueMany(Array.from({ length: 50000 }, (_, i) => ({ id: i })));
        let n = 0;
        for (;;) {
            const entries = await q.peek(100);
            if (!entries.length) {
                break;
            }
            for (const e of entries) {
                assert.equal(e.record.id, n++);
            }
            await q.ack(entries);
        }
        assert.equal(n, 50000);
        assert.ok(q._items.length < 2048, `backing array not compacted: ${q._items.length}`);
        assert.ok(Date.now() - t0 < 5000);
        await q.close();
    });

    it('a logger that throws cannot break the queue', async function () {
        const q = new MemoryQueue({ maxMessages: 1, logger: { warn() { throw new Error('bad logger'); } } });
        await q.enqueue({ id: 1 });
        assert.equal((await q.enqueue({ id: 2 })).dropped, 1);
        await q.close();
    });
});
