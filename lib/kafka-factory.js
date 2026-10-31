'use strict';

/**
 * The single place that loads kafkajs.
 *
 * - Tests inject a fake implementation with `setKafkajs(fake)`.
 * - The first time the real module is used, the Snappy/LZ4 codecs are
 *   registered into it (once per process).
 * - Keeping the import in one module would also allow another client library
 *   to be plugged in later.
 */

const codecs = require('./codecs');

let override = null;
let real = null;
let codecSupport = { snappy: false, lz4: false };

/**
 * The kafkajs module to use: the test override if set, else the real kafkajs.
 * Throws only if kafkajs itself cannot be loaded (callers already wrap client
 * creation in try/catch).
 *
 * @returns {object}
 */
function getKafkajs() {
    if (override) return override;
    if (!real) {
        const mod = require('kafkajs');
        try {
            codecSupport = codecs.register(mod);
        } catch (_) {
            // compression codecs are optional
        }
        real = mod;
    }
    return real;
}

/**
 * Test hook: use `impl` instead of kafkajs; `null` restores the real module.
 *
 * @param {object|null} impl
 */
function setKafkajs(impl) {
    override = impl || null;
}

/**
 * Which optional codecs were registered into the real kafkajs.
 *
 * @returns {{snappy: boolean, lz4: boolean}}
 */
function getCodecSupport() {
    return { ...codecSupport };
}

module.exports = { getKafkajs, setKafkajs, getCodecSupport };
