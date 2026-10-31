'use strict';

/**
 * Exponential backoff with jitter for the reconnect supervisors.
 *
 * kafkajs' own retries cannot be cancelled, so the nodes keep kafkajs retries
 * short and reconnect forever themselves, waiting `backoff.next()` ms between
 * attempts. Jitter spreads reconnects of many nodes/instances after an outage.
 */

const { toNumber } = require('./util');

class Backoff {
    /**
     * @param {object} [options]
     * @param {number} [options.min=1000] first delay (ms)
     * @param {number} [options.max=60000] largest delay (ms); jitter never exceeds it
     * @param {number} [options.factor=2] growth per attempt (>= 1)
     * @param {number} [options.jitter=0.2] +/- fraction of randomness (0..1)
     * @param {() => number} [options.random=Math.random] injectable for tests
     */
    constructor(options) {
        const o = options && typeof options === 'object' ? options : {};
        this._min = toNumber(o.min, 1000, { min: 0 });
        this._max = Math.max(this._min, toNumber(o.max, 60000, { min: 0 }));
        this._factor = toNumber(o.factor, 2, { min: 1 });
        this._jitter = toNumber(o.jitter, 0.2, { min: 0, max: 1 });
        this._random = typeof o.random === 'function' ? o.random : Math.random;
        this._attempt = 0;
    }

    /**
     * The delay before the next attempt, in ms: `min * factor^attempt` capped
     * at `max`, then +/- `jitter` randomness, clamped to [0, max]. Increments
     * the attempt counter.
     *
     * @returns {number}
     */
    next() {
        let base = this._min * Math.pow(this._factor, this._attempt);
        if (!Number.isFinite(base) || base > this._max) base = this._max;
        let r = 0.5;
        try {
            r = Number(this._random());
        } catch (_) {
            // keep r = 0.5 (no jitter) if an injected random function fails
        }
        if (!Number.isFinite(r)) r = 0.5;
        const delta = (r * 2 - 1) * this._jitter; // in [-jitter, +jitter]
        let delay = Math.round(base * (1 + delta));
        if (!Number.isFinite(delay) || delay < 0) delay = 0;
        if (delay > this._max) delay = this._max;
        if (this._attempt < Number.MAX_SAFE_INTEGER) this._attempt++;
        return delay;
    }

    /** Start again from `min` (call after a successful connection). */
    reset() {
        this._attempt = 0;
    }

    /** Number of `next()` calls since construction or the last `reset()`. */
    get attempt() {
        return this._attempt;
    }
}

module.exports = { Backoff };
