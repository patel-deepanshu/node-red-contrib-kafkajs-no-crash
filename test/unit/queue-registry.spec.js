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
            (e) => e instanceof Error && e.message === `Queue directory ${dir} is already used by node node1`);
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

    it('lets the same owner acquire again (redeploy) with reference counting', function () {
        const oldInstance = registry.acquire(dir, 'node1');
        const newInstance = registry.acquire(dir, 'node1');
        oldInstance(); // the old node finishes closing after the new one started
        assert.equal(registry.isOwned(dir), true, 'still held by the new instance');
        assert.throws(() => registry.acquire(dir, 'node2'), /already used/);
        newInstance();
        assert.equal(registry.isOwned(dir), false);
    });

    it('release is idempotent and cannot release someone else\'s claim', function () {
        const r1 = registry.acquire(dir, 'node1');
        const r2 = registry.acquire(dir, 'node1');
        r1();
        r1();
        r1();
        assert.equal(registry.isOwned(dir), true, 'repeated release of r1 must not drop r2');
        r2();
        assert.equal(registry.isOwned(dir), false);
        const r3 = registry.acquire(dir, 'node3');
        r1();
        r2();
        assert.equal(registry.ownerOf(dir), 'node3', 'stale releases do not affect the new owner');
        r3();
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
        assert.doesNotThrow(() => registry.acquire(dir, '42')());
        release();
    });
});
