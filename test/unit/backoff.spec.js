'use strict';

const assert = require('node:assert/strict');
const { Backoff } = require('../../lib/backoff');

describe('lib/backoff', function () {
    it('grows exponentially from min and caps at max (no jitter)', function () {
        const b = new Backoff({ min: 1000, max: 10000, factor: 2, jitter: 0 });
        assert.deepEqual([b.next(), b.next(), b.next(), b.next(), b.next(), b.next()],
            [1000, 2000, 4000, 8000, 10000, 10000]);
        assert.equal(b.attempt, 6);
    });

    it('uses sensible defaults (1 s → 60 s, factor 2, ±20 %)', function () {
        const b = new Backoff();
        const first = b.next();
        assert.ok(first >= 800 && first <= 1200, String(first));
        for (let i = 0; i < 20; i++) b.next();
        const late = b.next();
        assert.ok(late >= 48000 && late <= 60000, String(late));
    });

    it('applies jitter within ±jitter and never exceeds max', function () {
        const low = new Backoff({ min: 1000, max: 60000, jitter: 0.2, random: () => 0 });
        const high = new Backoff({ min: 1000, max: 60000, jitter: 0.2, random: () => 0.999999 });
        assert.equal(low.next(), 800);
        assert.equal(high.next(), 1200);
        const capped = new Backoff({ min: 60000, max: 60000, jitter: 0.5, random: () => 1 });
        assert.equal(capped.next(), 60000);
        const capLow = new Backoff({ min: 60000, max: 60000, jitter: 0.5, random: () => 0 });
        assert.equal(capLow.next(), 30000);
    });

    it('stays within [base*(1-j), min(max, base*(1+j))] for random values', function () {
        const b = new Backoff({ min: 100, max: 5000, factor: 3, jitter: 0.3 });
        for (let i = 0; i < 200; i++) {
            const attempt = b.attempt;
            const base = Math.min(5000, 100 * Math.pow(3, attempt));
            const d = b.next();
            assert.ok(d >= Math.floor(base * 0.7) && d <= Math.min(5000, Math.ceil(base * 1.3)), `${attempt}: ${d}`);
        }
    });

    it('reset() starts again from min', function () {
        const b = new Backoff({ min: 100, max: 1000, jitter: 0 });
        b.next();
        b.next();
        b.next();
        assert.equal(b.attempt, 3);
        b.reset();
        assert.equal(b.attempt, 0);
        assert.equal(b.next(), 100);
    });

    it('never produces Infinity/NaN after very many attempts', function () {
        const b = new Backoff({ min: 1000, max: 30000, jitter: 0.2 });
        for (let i = 0; i < 5000; i++) {
            const d = b.next();
            assert.ok(Number.isFinite(d) && d >= 0 && d <= 30000);
        }
    });

    it('sanitises bad options and never throws', function () {
        for (const opts of [undefined, null, 'x', {}, { min: 'abc', max: null, factor: 0, jitter: 5 },
            { min: -5, max: -10 }, { min: '500', max: '2000', factor: '2', jitter: '0' }]) {
            const b = new Backoff(opts);
            const d = b.next();
            assert.ok(Number.isFinite(d) && d >= 0, `${JSON.stringify(opts)} → ${d}`);
        }
        const strings = new Backoff({ min: '500', max: '2000', factor: '2', jitter: '0' });
        assert.deepEqual([strings.next(), strings.next(), strings.next(), strings.next()], [500, 1000, 2000, 2000]);
        const inverted = new Backoff({ min: 5000, max: 1000, jitter: 0 });
        assert.equal(inverted.next(), 5000, 'max is raised to min');
        const brokenRandom = new Backoff({ min: 1000, jitter: 0.5, random: () => { throw new Error('x'); } });
        assert.equal(brokenRandom.next(), 1000);
        const nanRandom = new Backoff({ min: 1000, jitter: 0.5, random: () => NaN });
        assert.equal(nanRandom.next(), 1000);
    });
});
