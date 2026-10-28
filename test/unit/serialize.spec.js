'use strict';

const assert = require('node:assert/strict');
const s = require('../../lib/serialize');

const isMessageError = (e) => e && e.name === 'KafkaNcMessageError';

describe('lib/serialize', function () {
    describe('toKafkaValue', function () {
        it('passes Buffers and strings through', function () {
            const b = Buffer.from([0, 1, 255]);
            assert.equal(s.toKafkaValue(b), b);
            assert.equal(s.toKafkaValue('text'), 'text');
            assert.equal(s.toKafkaValue(''), '', 'an empty string is a valid value');
        });

        it('keeps null as a tombstone', function () {
            assert.equal(s.toKafkaValue(null), null);
        });

        it('rejects undefined with a MessageError', function () {
            assert.throws(() => s.toKafkaValue(undefined), (e) => isMessageError(e) && e.message === 'msg.payload is undefined');
        });

        it('stringifies scalars (falsy values are NOT dropped)', function () {
            assert.equal(s.toKafkaValue(0), '0');
            assert.equal(s.toKafkaValue(-1.5), '-1.5');
            assert.equal(s.toKafkaValue(false), 'false');
            assert.equal(s.toKafkaValue(true), 'true');
            assert.equal(s.toKafkaValue(12345678901234567890n), '12345678901234567890');
        });

        it('converts Dates to ISO strings and rejects invalid Dates', function () {
            assert.equal(s.toKafkaValue(new Date(0)), '1970-01-01T00:00:00.000Z');
            assert.throws(() => s.toKafkaValue(new Date('nope')), isMessageError);
        });

        it('converts typed arrays and ArrayBuffers to Buffers', function () {
            const u8 = new Uint8Array([1, 2, 3]);
            const out = s.toKafkaValue(u8);
            assert.ok(Buffer.isBuffer(out));
            assert.deepEqual([...out], [1, 2, 3]);
            const ab = new Uint8Array([4, 5]).buffer;
            assert.deepEqual([...s.toKafkaValue(ab)], [4, 5]);
            const sub = new Uint8Array([9, 8, 7, 6]).subarray(1, 3);
            assert.deepEqual([...s.toKafkaValue(sub)], [8, 7]);
            const f32 = new Float32Array([1]);
            assert.equal(s.toKafkaValue(f32).length, 4);
        });

        it('JSON-encodes objects and arrays', function () {
            assert.equal(s.toKafkaValue({ a: 1, b: [true, null] }), '{"a":1,"b":[true,null]}');
            assert.equal(s.toKafkaValue([1, 'x']), '[1,"x"]');
            assert.equal(s.toKafkaValue({}), '{}');
        });

        it('rejects circular objects, BigInt inside JSON, functions and symbols', function () {
            const circ = { a: 1 };
            circ.self = circ;
            assert.throws(() => s.toKafkaValue(circ), (e) => isMessageError(e) && /JSON/.test(e.message));
            assert.throws(() => s.toKafkaValue({ n: 1n }), isMessageError);
            assert.throws(() => s.toKafkaValue(() => 1), isMessageError);
            assert.throws(() => s.toKafkaValue(Symbol('x')), isMessageError);
            assert.throws(() => s.toKafkaValue({ toJSON() { throw new Error('nope'); } }), isMessageError);
            assert.throws(() => s.toKafkaValue({ toJSON() { return undefined; } }), isMessageError);
        });
    });

    describe('toKafkaKey', function () {
        it('maps empty keys to null', function () {
            assert.equal(s.toKafkaKey(undefined), null);
            assert.equal(s.toKafkaKey(null), null);
            assert.equal(s.toKafkaKey(''), null);
        });
        it('converts like values', function () {
            const b = Buffer.from('k');
            assert.equal(s.toKafkaKey(b), b);
            assert.equal(s.toKafkaKey('k'), 'k');
            assert.equal(s.toKafkaKey(0), '0');
            assert.equal(s.toKafkaKey(false), 'false');
            assert.equal(s.toKafkaKey(7n), '7');
            assert.equal(s.toKafkaKey({ id: 1 }), '{"id":1}');
            assert.equal(s.toKafkaKey(new Date(0)), '1970-01-01T00:00:00.000Z');
            const circ = {};
            circ.c = circ;
            assert.throws(() => s.toKafkaKey(circ), (e) => isMessageError(e) && /msg\.key/.test(e.message));
        });
    });

    describe('toKafkaHeaders', function () {
        it('returns undefined for undefined/null', function () {
            assert.equal(s.toKafkaHeaders(undefined), undefined);
            assert.equal(s.toKafkaHeaders(null), undefined);
        });

        it('rejects non-objects and arrays', function () {
            for (const bad of ['x', 5, true, [], [['a', 'b']], Buffer.from('x')]) {
                assert.throws(() => s.toKafkaHeaders(bad), isMessageError, String(bad));
            }
        });

        it('converts every value type', function () {
            const buf = Buffer.from('raw');
            const out = s.toKafkaHeaders({
                str: 'v',
                buf,
                num: 0,
                bool: false,
                big: 5n,
                date: new Date(0),
                obj: { a: 1 },
                gone: null,
                missing: undefined,
                arr: ['a', null, 1, Buffer.from('b'), undefined, { x: 1 }],
                emptyArr: [null, undefined]
            });
            assert.deepEqual(out, {
                str: 'v',
                buf,
                num: '0',
                bool: 'false',
                big: '5',
                date: '1970-01-01T00:00:00.000Z',
                obj: '{"a":1}',
                arr: ['a', '1', Buffer.from('b'), '{"x":1}']
            });
            assert.equal(out.buf, buf, 'Buffers are passed by reference');
        });

        it('accepts Maps', function () {
            assert.deepEqual(s.toKafkaHeaders(new Map([['a', 1], ['b', null]])), { a: '1' });
        });

        it('does not mutate the prototype for a "__proto__" header', function () {
            const h = JSON.parse('{"__proto__": "evil", "ok": "1"}');
            const out = s.toKafkaHeaders(h);
            assert.equal(Object.getPrototypeOf(out), Object.prototype);
            assert.equal(Object.getOwnPropertyDescriptor(out, '__proto__').value, 'evil');
            assert.equal(out.ok, '1');
        });

        it('turns unexpected failures into MessageErrors', function () {
            const circ = {};
            circ.c = circ;
            assert.throws(() => s.toKafkaHeaders({ c: circ }), (e) => isMessageError(e) && /msg\.headers\["c"\]/.test(e.message));
            const getterBomb = Object.defineProperty({}, 'x', { enumerable: true, get() { throw new Error('getter'); } });
            assert.throws(() => s.toKafkaHeaders(getterBomb), isMessageError);
            assert.throws(() => s.toKafkaHeaders({ f: () => 1 }), isMessageError);
        });
    });

    describe('toKafkaPartition', function () {
        it('maps empty values to undefined', function () {
            assert.equal(s.toKafkaPartition(undefined), undefined);
            assert.equal(s.toKafkaPartition(null), undefined);
            assert.equal(s.toKafkaPartition(''), undefined);
        });
        it('accepts non-negative integers as numbers or numeric strings', function () {
            assert.equal(s.toKafkaPartition(0), 0, 'partition 0 must not be dropped');
            assert.equal(s.toKafkaPartition(3), 3);
            assert.equal(s.toKafkaPartition('0'), 0);
            assert.equal(s.toKafkaPartition(' 12 '), 12);
            assert.equal(s.toKafkaPartition(2n), 2);
            assert.equal(s.toKafkaPartition(2147483647), 2147483647);
            assert.ok(Object.is(s.toKafkaPartition(-0), 0));
        });
        it('rejects everything else', function () {
            for (const bad of [-1, 1.5, '1.5', 'a', '-1', NaN, Infinity, 2147483648, true, {}, [], '0x1', -1n]) {
                assert.throws(() => s.toKafkaPartition(bad), isMessageError, String(bad));
            }
        });
    });

    describe('toKafkaTimestamp', function () {
        it('maps empty values to undefined', function () {
            assert.equal(s.toKafkaTimestamp(undefined), undefined);
            assert.equal(s.toKafkaTimestamp(null), undefined);
            assert.equal(s.toKafkaTimestamp(''), undefined);
        });
        it('converts Dates, numbers and digit strings', function () {
            assert.equal(s.toKafkaTimestamp(new Date(1700000000123)), '1700000000123');
            assert.equal(s.toKafkaTimestamp(1700000000123.9), '1700000000123');
            assert.equal(s.toKafkaTimestamp(0), '0');
            assert.equal(s.toKafkaTimestamp('1700000000123'), '1700000000123');
            assert.equal(s.toKafkaTimestamp(' 42 '), '42');
            assert.equal(s.toKafkaTimestamp(1700000000123n), '1700000000123');
        });
        it('rejects invalid timestamps (kafkajs would throw a BigInt SyntaxError)', function () {
            for (const bad of ['abc', '1.5', '-1', -1, NaN, Infinity, new Date('x'), {}, true, '99999999999999999999',
                2 ** 60]) {
                assert.throws(() => s.toKafkaTimestamp(bad), isMessageError, String(bad));
            }
        });
    });

    describe('fromKafkaHeaders', function () {
        it('decodes Buffers to strings, keeps null, maps arrays', function () {
            const out = s.fromKafkaHeaders({ a: Buffer.from('x'), n: null, arr: [Buffer.from('1'), Buffer.from('2')], str: 's' });
            assert.deepEqual(out, { a: 'x', n: null, arr: ['1', '2'], str: 's' });
        });
        it('keeps Buffers with asBuffer', function () {
            const out = s.fromKafkaHeaders({ a: Buffer.from('x'), str: 's', arr: [Buffer.from('1'), null] }, { asBuffer: true });
            assert.ok(Buffer.isBuffer(out.a));
            assert.ok(Buffer.isBuffer(out.str));
            assert.deepEqual(out.arr, [Buffer.from('1'), null]);
        });
        it('returns {} for undefined and invalid input', function () {
            assert.deepEqual(s.fromKafkaHeaders(undefined), {});
            assert.deepEqual(s.fromKafkaHeaders(null), {});
            assert.deepEqual(s.fromKafkaHeaders('x'), {});
            assert.deepEqual(s.fromKafkaHeaders([]), {});
        });
        it('is safe against a "__proto__" header', function () {
            const out = s.fromKafkaHeaders(JSON.parse('{"__proto__": {"polluted": true}}'));
            assert.equal(out.polluted, undefined);
            assert.equal(Object.getPrototypeOf(out), Object.prototype);
            assert.equal(({}).polluted, undefined);
        });
    });

    describe('decodeValue', function () {
        const buf = Buffer.from('{"a":1}');
        it('decodes as string (default), buffer or json', function () {
            assert.equal(s.decodeValue(buf, 'string'), '{"a":1}');
            assert.equal(s.decodeValue(buf), '{"a":1}');
            assert.equal(s.decodeValue(buf, 'unknown-format'), '{"a":1}');
            assert.equal(s.decodeValue(buf, 'buffer'), buf);
            assert.deepEqual(s.decodeValue(buf, 'json'), { a: 1 });
        });
        it('returns null for tombstones', function () {
            for (const f of ['string', 'buffer', 'json']) {
                assert.equal(s.decodeValue(null, f), null);
                assert.equal(s.decodeValue(undefined, f), null);
            }
        });
        it('throws SyntaxError for invalid JSON (the caller reports it)', function () {
            assert.throws(() => s.decodeValue(Buffer.from('not json'), 'json'), SyntaxError);
        });
        it('tolerates string input', function () {
            assert.deepEqual(s.decodeValue('[1]', 'json'), [1]);
            assert.ok(Buffer.isBuffer(s.decodeValue('x', 'buffer')));
        });
    });

    describe('encodeBytes / decodeBytes', function () {
        it('round-trips null, strings and Buffers', function () {
            for (const v of [null, '', 'héllo', Buffer.from([0, 255, 10, 13]), Buffer.alloc(0)]) {
                const e = s.encodeBytes(v);
                const json = JSON.parse(JSON.stringify(e));
                assert.deepEqual(s.decodeBytes(json), v);
            }
        });
        it('uses the documented shapes', function () {
            assert.equal(s.encodeBytes(null), null);
            assert.equal(s.encodeBytes(undefined), undefined);
            assert.deepEqual(s.encodeBytes('a'), { s: 'a' });
            assert.deepEqual(s.encodeBytes(Buffer.from('a')), { b: 'YQ==' });
            assert.deepEqual(s.encodeBytes(new Uint8Array([97])), { b: 'YQ==' });
        });
        it('tolerates raw strings and Buffer JSON', function () {
            assert.equal(s.decodeBytes('raw'), 'raw');
            assert.equal(s.decodeBytes(undefined), undefined);
            assert.deepEqual(s.decodeBytes(JSON.parse(JSON.stringify(Buffer.from('xy')))), Buffer.from('xy'));
        });
        it('throws MessageError for garbage', function () {
            assert.throws(() => s.decodeBytes(5), isMessageError);
            assert.throws(() => s.decodeBytes({ z: 1 }), isMessageError);
        });
    });

    describe('encodeMessage / decodeMessage', function () {
        function roundTrip(m) {
            return s.decodeMessage(JSON.parse(JSON.stringify(s.encodeMessage(m))));
        }

        it('round-trips a full message with Buffers and header arrays', function () {
            const m = {
                key: Buffer.from([1, 2, 3]),
                value: Buffer.from('payload'),
                headers: { a: 'x', b: Buffer.from([0xff]), c: ['1', Buffer.from('2')] },
                partition: 0,
                timestamp: '1700000000123'
            };
            assert.deepEqual(roundTrip(m), m);
        });

        it('round-trips string values, null tombstones and null keys', function () {
            assert.deepEqual(roundTrip({ key: null, value: null }), { key: null, value: null });
            assert.deepEqual(roundTrip({ value: 'v' }), { value: 'v' });
            assert.deepEqual(roundTrip({ key: 'k', value: '' }), { key: 'k', value: '' });
        });

        it('omits undefined fields and uses the compact shape', function () {
            assert.deepEqual(s.encodeMessage({ value: 'v', key: undefined, partition: undefined, timestamp: undefined }),
                { v: { s: 'v' } });
            assert.deepEqual(s.encodeMessage({ key: 'k', value: null, partition: 2, timestamp: 5, headers: { h: 'x' } }),
                { k: { s: 'k' }, v: null, p: 2, ts: '5', h: { h: { s: 'x' } } });
        });

        it('keeps a missing value missing (kafkajs then rejects it as a MESSAGE error)', function () {
            const m = roundTrip({ key: 'k' });
            assert.ok(!('value' in m));
        });

        it('survives the output of the to* converters', function () {
            const m = {
                key: s.toKafkaKey(42),
                value: s.toKafkaValue({ temp: 21.5 }),
                headers: s.toKafkaHeaders({ unit: 'C', raw: Buffer.from([1]), list: ['a', 'b'] }),
                partition: s.toKafkaPartition('1'),
                timestamp: s.toKafkaTimestamp(new Date(1000))
            };
            assert.deepEqual(roundTrip(m), m);
        });

        it('is safe against "__proto__" header keys', function () {
            const m = { value: 'v', headers: JSON.parse('{"__proto__": "x"}') };
            const out = roundTrip(m);
            assert.equal(Object.getPrototypeOf(out.headers), Object.prototype);
            assert.equal(Object.getOwnPropertyDescriptor(out.headers, '__proto__').value, 'x');
        });

        it('rejects corrupt input with a MessageError', function () {
            for (const bad of [null, undefined, 'x', 5, []]) {
                assert.throws(() => s.decodeMessage(bad), isMessageError, String(bad));
            }
            assert.throws(() => s.decodeMessage({ v: 5 }), isMessageError);
            assert.throws(() => s.encodeMessage(null), isMessageError);
        });
    });
});
