'use strict';

const assert = require('node:assert/strict');
const kafkajs = require('kafkajs');
const { LEVELS, toKafkaLogLevel, createLogCreator, formatEntry, _resetDedupe } = require('../../lib/logger');

/** A fake RED with a recording RED.log. */
function fakeRED() {
    const lines = { warn: [], info: [], debug: [], error: [] };
    return {
        lines,
        log: {
            warn: (t) => lines.warn.push(t),
            info: (t) => lines.info.push(t),
            debug: (t) => lines.debug.push(t),
            error: (t) => lines.error.push(t)
        }
    };
}

function entry(level, message, extra = {}, namespace = 'Connection') {
    const label = { 1: 'ERROR', 2: 'WARN', 4: 'INFO', 5: 'DEBUG' }[level];
    return { namespace, level, label, log: { timestamp: new Date().toISOString(), logger: 'kafkajs', message, ...extra } };
}

describe('lib/logger', function () {
    beforeEach(function () {
        _resetDedupe();
    });

    describe('toKafkaLogLevel', function () {
        it('maps names to kafkajs numeric levels', function () {
            assert.equal(toKafkaLogLevel('off', kafkajs), kafkajs.logLevel.NOTHING);
            assert.equal(toKafkaLogLevel('nothing', kafkajs), kafkajs.logLevel.NOTHING);
            assert.equal(toKafkaLogLevel('error', kafkajs), kafkajs.logLevel.ERROR);
            assert.equal(toKafkaLogLevel('warn', kafkajs), kafkajs.logLevel.WARN);
            assert.equal(toKafkaLogLevel('info', kafkajs), kafkajs.logLevel.INFO);
            assert.equal(toKafkaLogLevel('debug', kafkajs), kafkajs.logLevel.DEBUG);
            assert.equal(toKafkaLogLevel(' DEBUG ', kafkajs), 5);
        });
        it('defaults to WARN and always returns a number', function () {
            for (const v of [undefined, null, '', 'verbose', {}, 3, NaN]) {
                const l = toKafkaLogLevel(v, kafkajs);
                assert.equal(typeof l, 'number');
                assert.equal(l, LEVELS.WARN, String(v));
            }
            assert.equal(toKafkaLogLevel(4), 4, 'numeric levels pass through');
            assert.equal(toKafkaLogLevel('info'), 4, 'works without the kafkajs module');
            assert.equal(toKafkaLogLevel('info', { logLevel: null }), 4);
        });
    });

    describe('formatEntry', function () {
        it('follows the documented format', function () {
            assert.equal(formatEntry(entry(1, 'Connection error: connect ECONNREFUSED 127.0.0.1:9092',
                { broker: '127.0.0.1:9092', error: 'connect ECONNREFUSED 127.0.0.1:9092' }), '[kafkajs-nc-producer:p1]'),
            '[kafkajs:Connection] [kafkajs-nc-producer:p1] Connection error: connect ECONNREFUSED 127.0.0.1:9092'
                + ' (broker 127.0.0.1:9092): connect ECONNREFUSED 127.0.0.1:9092');
        });
        it('omits the namespace, broker and a duplicate error', function () {
            assert.equal(formatEntry(entry(2, 'KafkaJS v2.0.0 switched default partitioner', {}, null), 'L'),
                '[kafkajs] L KafkaJS v2.0.0 switched default partitioner');
            assert.equal(formatEntry(entry(1, 'same', { error: 'same' }), 'L'), '[kafkajs:Connection] L same');
            assert.equal(formatEntry(entry(1, 'm'), ''), '[kafkajs:Connection] m');
        });
        it('renders error objects and circular extras', function () {
            const circ = { a: 1 };
            circ.self = circ;
            assert.equal(formatEntry(entry(1, 'Crash', { error: new Error('boom') }), 'L'), '[kafkajs:Connection] L Crash: boom');
            assert.match(formatEntry(entry(1, 'Crash', { error: circ }), 'L'), /\[Circular\]/);
        });
    });

    describe('createLogCreator', function () {
        it('routes kafkajs levels to RED.log (ERROR and WARN → warn)', function () {
            const RED = fakeRED();
            const log = createLogCreator(RED, '[t:1]')(kafkajs.logLevel.DEBUG);
            log(entry(1, 'an error'));
            log(entry(2, 'a warning'));
            log(entry(4, 'some info'));
            log(entry(5, 'debug detail'));
            log(entry(0, 'nothing'));
            assert.deepEqual(RED.lines.warn, ['[kafkajs:Connection] [t:1] an error', '[kafkajs:Connection] [t:1] a warning']);
            assert.deepEqual(RED.lines.info, ['[kafkajs:Connection] [t:1] some info']);
            assert.deepEqual(RED.lines.debug, ['[kafkajs:Connection] [t:1] debug detail']);
            assert.deepEqual(RED.lines.error, []);
        });

        it('suppresses identical lines within the window and reports the repeat count afterwards', function () {
            const RED = fakeRED();
            const log = createLogCreator(RED, 'L', { dedupeMs: 60000 })(2);
            const realNow = Date.now;
            let now = 1000000;
            Date.now = () => now;
            try {
                for (let i = 0; i < 5; i++) log(entry(1, 'Connection error: ECONNREFUSED'));
                log(entry(1, 'another line'));
                assert.deepEqual(RED.lines.warn, ['[kafkajs:Connection] L Connection error: ECONNREFUSED',
                    '[kafkajs:Connection] L another line']);
                now += 60001;
                log(entry(1, 'Connection error: ECONNREFUSED'));
                assert.equal(RED.lines.warn[2], '[kafkajs:Connection] L Connection error: ECONNREFUSED (repeated 4 times)');
                log(entry(1, 'Connection error: ECONNREFUSED'));
                assert.equal(RED.lines.warn.length, 3);
                now += 60001;
                log(entry(1, 'Connection error: ECONNREFUSED'));
                assert.equal(RED.lines.warn[3], '[kafkajs:Connection] L Connection error: ECONNREFUSED (repeated 1 times)');
            } finally {
                Date.now = realNow;
            }
        });

        it('dedupes per level+text: same text at another level is printed', function () {
            const RED = fakeRED();
            const log = createLogCreator(RED, 'L')(5);
            log(entry(4, 'x'));
            log(entry(5, 'x'));
            assert.equal(RED.lines.info.length, 1);
            assert.equal(RED.lines.debug.length, 1);
        });

        it('shares de-duplication across creators (a new kafkajs client per reconnect attempt)', function () {
            const RED = fakeRED();
            for (let attempt = 0; attempt < 10; attempt++) {
                const log = createLogCreator(RED, '[p:1]')(2);
                log(entry(1, 'Connection error: connect ECONNREFUSED'));
                log(entry(1, 'Failed to connect to seed broker, trying another broker from the list', {}, 'BrokerPool'));
            }
            assert.equal(RED.lines.warn.length, 2);
            // a different node label is a different line
            createLogCreator(RED, '[p:2]')(2)(entry(1, 'Connection error: connect ECONNREFUSED'));
            assert.equal(RED.lines.warn.length, 3);
        });

        it('keeps the de-duplication map bounded (500 keys)', function () {
            const RED = fakeRED();
            const log = createLogCreator(RED, 'L')(2);
            for (let i = 0; i < 2000; i++) log(entry(1, `line ${i}`));
            assert.equal(RED.lines.warn.length, 2000);
            // the oldest keys were evicted, so line 0 prints again; a recent one is still suppressed
            log(entry(1, 'line 0'));
            log(entry(1, 'line 1999'));
            assert.equal(RED.lines.warn.length, 2001);
        });

        it('dedupeMs 0 disables suppression', function () {
            const RED = fakeRED();
            const log = createLogCreator(RED, 'L', { dedupeMs: 0 })(2);
            log(entry(1, 'x'));
            log(entry(1, 'x'));
            assert.equal(RED.lines.warn.length, 2);
        });

        it('never throws: throwing RED.log, missing RED, hostile entries', function () {
            const throwing = { log: { warn() { throw new Error('log broken'); }, info() { throw new Error('x'); } } };
            const hostile = new Proxy({}, { get() { throw new Error('trap'); } });
            const circ = {};
            circ.c = circ;
            const creators = [createLogCreator(throwing, 'L'), createLogCreator(undefined, 'L'), createLogCreator({}, 'L'),
                createLogCreator({ log: {} }, 'L'), createLogCreator(fakeRED(), hostile)];
            for (const create of creators) {
                const log = create(2);
                assert.doesNotThrow(() => {
                    log(entry(1, 'm'));
                    log(entry(4, 'm', { error: circ, broker: circ }));
                    log(hostile);
                    log(undefined);
                    log(null);
                    log({ level: 1, log: hostile });
                    log({ level: 1, namespace: hostile, log: { message: hostile } });
                    log(entry(1, { toString() { throw new Error('x'); } }));
                    log(entry(1, 'bigint', { error: { n: 1n } }));
                });
            }
        });

        it('works as a real kafkajs logCreator', function () {
            const RED = fakeRED();
            const { Kafka } = kafkajs;
            const kafka = new Kafka({
                clientId: 't',
                brokers: ['127.0.0.1:1'],
                logLevel: kafkajs.logLevel.INFO,
                logCreator: createLogCreator(RED, '[test]')
            });
            kafka.logger().info('hello from kafkajs', { broker: 'b:1' });
            kafka.logger().debug('filtered by kafkajs');
            kafka.logger().error('circular extra', { error: (() => { const o = {}; o.o = o; return o; })() });
            assert.deepEqual(RED.lines.info, ['[kafkajs] [test] hello from kafkajs (broker b:1)']);
            assert.equal(RED.lines.debug.length, 0);
            assert.equal(RED.lines.warn.length, 1);
        });
    });
});
