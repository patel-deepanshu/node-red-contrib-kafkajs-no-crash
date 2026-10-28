'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const registry = require('../../lib/queue-registry');

describe('lib/queue-registry', function () {
    const dir = path.resolve('/tmp/kafkajs-nc-registry-spec/queue-a');

    afterEach(function () {
        registry._reset();
    });

    it('lets one owner acquire a directory', function () {
        assert.equal(registry.isOwned(dir), false);
        const release = registry.acquire(dir, 'node1');
        assert.equal(typeof release, 'function');
        assert.equal(registry.isOwned(dir), true);
        assert.equal(registry.ownerOf(dir), 'node1');
        release();
        assert.equal(registry.isOwned(dir), false);
        assert.equal(registry.ownerOf(dir), null);
    });

    it('rejects a different owner with a clear message', function () {
        const release = registry.acquire(dir, 'node1');
        assert.throws(() => registry.acquire(dir, 'node2'),
            (e) => e instanceof Error && e.message === `Queue directory ${dir} is already used by node node1` &&
                e.code === 'QUEUE_DIR_IN_USE');
        release();
        const r2 = registry.acquire(dir, 'node2');
        assert.equal(registry.ownerOf(dir), 'node2');
        r2();
    });

    it('treats equivalent paths as the same directory', function () {
        const release = registry.acquire(dir, 'node1');
        assert.throws(() => registry.acquire(dir + '/../queue-a/', 'node2'), /already used by node node1/);
        assert.throws(() => registry.acquire(path.join(dir, '.'), 'node2'), /already used/);
        assert.equal(registry.isOwned(path.join(dir, 'sub', '..')), true);
        release();
    });

    it('resolves relative paths against the working directory', function () {
        const release = registry.acquire('relative-queue', 'n');
        assert.equal(registry.isOwned(path.resolve('relative-queue')), true);
        release();
    });

    it('never lets two instances hold a directory, also with the same owner id (LOSS-2)', function () {
        // Before: the same owner (a redeployed node) got a second, reference-counted claim,
        // so two DiskQueue objects could write the same files.
        const oldInstance = registry.acquire(dir, 'node1');
        assert.throws(() => registry.acquire(dir, 'node1'),
            (e) => e.code === 'QUEUE_DIR_IN_USE' && /already used by node node1/.test(e.message));
        oldInstance.closing();
        assert.throws(() => registry.acquire(dir, 'node1'),
            (e) => e.code === 'QUEUE_DIR_CLOSING' && e.message === `Queue directory ${dir} is still being closed by node node1`);
        assert.equal(registry.isOwned(dir), true, 'a closing holder still owns the directory');
        oldInstance();
        const newInstance = registry.acquire(dir, 'node1');
        assert.equal(registry.ownerOf(dir), 'node1');
        newInstance();
        assert.equal(registry.isOwned(dir), false);
    });

    it('release is idempotent and a late release never drops a newer claim of the same owner', function () {
        const r1 = registry.acquire(dir, 'node1');
        r1();
        r1();
        assert.equal(registry.isOwned(dir), false);
        const r2 = registry.acquire(dir, 'node1');
        r1();
        r1.closing();
        assert.equal(registry.ownerOf(dir), 'node1', 'stale release of an old instance does not affect the new one');
        assert.throws(() => registry.acquire(dir, 'node3'), (e) => e.code === 'QUEUE_DIR_IN_USE', 'r1.closing() did not mark r2');
        r2();
        const r3 = registry.acquire(dir, 'node3');
        r2();
        assert.equal(registry.ownerOf(dir), 'node3');
        r3();
    });

    describe('whenReleased()', function () {
        it('resolves true at once for a free directory', async function () {
            assert.equal(await registry.whenReleased(dir, 1000), true);
            assert.equal(await registry.whenReleased(dir), true);
        });

        it('does not wait for a holder that is not closing (two nodes configured with one queue)', async function () {
            const release = registry.acquire(dir, 'node1');
            const t0 = Date.now();
            assert.equal(await registry.whenReleased(dir, 5000), false);
            assert.ok(Date.now() - t0 < 100);
            release();
        });

        it('waits for a closing holder to release the directory', async function () {
            const release = registry.acquire(dir, 'node1');
            release.closing();
            const waiting = registry.whenReleased(dir, 5000);
            let settled = false;
            waiting.then(() => {
                settled = true;
            });
            await new Promise((resolve) => setTimeout(resolve, 30));
            assert.equal(settled, false, 'still waiting while the holder closes');
            release();
            assert.equal(await waiting, true);
            assert.doesNotThrow(() => registry.acquire(dir, 'node1')());
        });

        it('gives up after the timeout, and waits without limit when none is given', async function () {
            const release = registry.acquire(dir, 'node1');
            release.closing();
            assert.equal(await registry.whenReleased(dir, 20), false);
            const unbounded = registry.whenReleased(dir);
            setTimeout(release, 30);
            assert.equal(await unbounded, true);
        });

        it('resolves false when someone else took the directory meanwhile', async function () {
            const release = registry.acquire(dir, 'node1');
            release.closing();
            const waiting = registry.whenReleased(dir, 1000);
            release();
            registry.acquire(dir, 'node2');
            assert.equal(await waiting, false);
        });
    });

    it('keeps different directories independent', function () {
        const a = registry.acquire(dir, 'node1');
        const b = registry.acquire(dir + '-other', 'node2');
        assert.equal(registry.ownerOf(dir), 'node1');
        assert.equal(registry.ownerOf(dir + '-other'), 'node2');
        a();
        b();
    });

    it('stringifies owner ids', function () {
        const release = registry.acquire(dir, 42);
        assert.equal(registry.ownerOf(dir), '42');
        release();
    });
});
