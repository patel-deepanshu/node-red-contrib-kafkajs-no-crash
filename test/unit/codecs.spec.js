'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const kafkajs = require('kafkajs');
const Encoder = require('kafkajs/src/protocol/encoder');
const Decoder = require('kafkajs/src/protocol/decoder');
const { RecordBatch } = require('kafkajs/src/protocol/recordBatch/v0');
const Record = require('kafkajs/src/protocol/recordBatch/record/v0');
const decodeRecordBatch = require('kafkajs/src/protocol/recordBatch/v0/decoder');
const { KafkaJSNotImplemented } = require('kafkajs/src/errors');
const xxh32 = require('lz4js/xxh32.js');
const codecs = require('../../lib/codecs');
const kafkaFactory = require('../../lib/kafka-factory');

const { CompressionTypes, CompressionCodecs } = kafkajs;

// "hello kafka hello kafka hello kafka hello kafka hello kafka!" compressed by
// Kafka's own Java client (org.apache.kafka.common.compress.Lz4BlockOutputStream, kafka-clients 3.9.1).
const JAVA_LZ4_FRAME = 'BCJNGGBAghYAAADPaGVsbG8ga2Fma2EgDAAYUGFma2EhAAAAAA==';
const JAVA_LZ4_TEXT = 'hello kafka hello kafka hello kafka hello kafka hello kafka!';

/** The buffers every codec must round-trip. */
function samples() {
    return {
        empty: Buffer.alloc(0),
        tiny: Buffer.from('x'),
        text: Buffer.from('hello kafka '.repeat(1000)),
        json: Buffer.from(JSON.stringify(Array.from({ length: 2000 }, (_, i) => ({ id: i, name: `sensor-${i % 13}` })))),
        random: crypto.randomBytes(150000),
        zeros: Buffer.alloc(300000)
    };
}

/** Compress through a kafkajs Encoder, exactly as kafkajs calls codecs. */
async function roundTrip(type, buf) {
    const codec = CompressionCodecs[type]();
    const encoder = new Encoder();
    encoder.writeBuffer(buf);
    const compressed = await codec.compress(encoder);
    assert.ok(Buffer.isBuffer(compressed), 'compress resolves a Buffer');
    const out = await codec.decompress(compressed);
    assert.ok(Buffer.isBuffer(out), 'decompress resolves a Buffer');
    return { compressed, out };
}

describe('lib/codecs', function () {
    before(function () {
        codecs.register(kafkajs);
    });

    describe('register()', function () {
        it('registers Snappy and LZ4 into the real kafkajs', function () {
            assert.deepEqual(codecs.register(kafkajs), { snappy: true, lz4: true });
            for (const type of [CompressionTypes.Snappy, CompressionTypes.LZ4]) {
                const codec = CompressionCodecs[type]();
                assert.equal(typeof codec.compress, 'function');
                assert.equal(typeof codec.decompress, 'function');
            }
            assert.equal(CompressionCodecs[CompressionTypes.Snappy], require('kafkajs-snappy'));
            assert.equal(CompressionCodecs[CompressionTypes.LZ4], codecs.lz4Codec);
        });

        it('replaces the throwing kafkajs placeholders but leaves GZIP/ZSTD alone', function () {
            const notImplemented = (name) => () => {
                throw new KafkaJSNotImplemented(`${name} compression not implemented`);
            };
            const gzip = () => ({ compress: async () => Buffer.alloc(0), decompress: async () => Buffer.alloc(0) });
            const fake = {
                CompressionTypes,
                CompressionCodecs: { 1: gzip, 2: notImplemented('Snappy'), 3: notImplemented('LZ4'), 4: notImplemented('ZSTD') }
            };
            assert.deepEqual(codecs.register(fake), { snappy: true, lz4: true });
            assert.equal(fake.CompressionCodecs[1], gzip);
            assert.throws(() => fake.CompressionCodecs[4](), /ZSTD compression not implemented/);
        });

        it('keeps an already working codec (e.g. registered by the user)', function () {
            const custom = () => ({ compress: async () => Buffer.from('c'), decompress: async () => Buffer.from('d') });
            const fake = { CompressionTypes, CompressionCodecs: { 2: custom, 3: custom } };
            assert.deepEqual(codecs.register(fake), { snappy: true, lz4: true });
            assert.equal(fake.CompressionCodecs[2], custom);
            assert.equal(fake.CompressionCodecs[3], custom);
        });

        it('never throws on bad input', function () {
            for (const bad of [undefined, null, {}, { CompressionTypes }, { CompressionCodecs: {} }, 'x',
                new Proxy({}, { get() { throw new Error('trap'); } })]) {
                assert.deepEqual(codecs.register(bad), { snappy: false, lz4: false });
            }
            const frozen = { CompressionTypes, CompressionCodecs: Object.freeze({}) };
            assert.deepEqual(codecs.register(frozen), { snappy: false, lz4: false });
        });

        it('is applied by kafka-factory on first use of the real module', function () {
            kafkaFactory.setKafkajs(null);
            assert.equal(kafkaFactory.getKafkajs(), kafkajs);
            assert.deepEqual(kafkaFactory.getCodecSupport(), { snappy: true, lz4: true });
        });
    });

    describe('round trips through a kafkajs Encoder', function () {
        for (const [name, type] of [['Snappy', CompressionTypes.Snappy], ['LZ4', CompressionTypes.LZ4]]) {
            for (const [sample, buf] of Object.entries(samples())) {
                it(`${name}: ${sample} (${buf.length} bytes)`, async function () {
                    const { compressed, out } = await roundTrip(type, buf);
                    assert.ok(out.equals(buf));
                    if (sample === 'text' || sample === 'zeros') {
                        assert.ok(compressed.length < buf.length / 5, `compressible data shrinks (${compressed.length})`);
                    }
                });
            }
        }
    });

    describe('full kafkajs RecordBatch encode/decode', function () {
        for (const [name, type] of [['Snappy', CompressionTypes.Snappy], ['LZ4', CompressionTypes.LZ4]]) {
            it(`${name}-compressed batches decode back to the same records`, async function () {
                const records = [
                    Record({ offsetDelta: 0, timestampDelta: 0, key: Buffer.from('k1'), value: Buffer.from('v'.repeat(5000)),
                        headers: { h: Buffer.from('x') } }),
                    Record({ offsetDelta: 1, timestampDelta: 1, key: null, value: Buffer.from('second') })
                ];
                const now = Date.now();
                const encoded = await RecordBatch({ compression: type, records, firstOffset: '0', firstTimestamp: now,
                    maxTimestamp: now + 1, lastOffsetDelta: 1 });
                const batch = await decodeRecordBatch(new Decoder(encoded.buffer));
                assert.equal(batch.records.length, 2);
                assert.equal(batch.records[0].key.toString(), 'k1');
                assert.equal(batch.records[0].value.toString(), 'v'.repeat(5000));
                assert.equal(batch.records[0].headers.h.toString(), 'x');
                assert.equal(batch.records[1].value.toString(), 'second');
            });
        }
    });

    describe('LZ4 frame format (Kafka compatibility)', function () {
        it('writes a frame Kafka accepts: block independence flag, 64 KB blocks, valid header checksum', function () {
            const frame = codecs.lz4CompressFrame(Buffer.from('abc'.repeat(100)));
            assert.equal(frame.readUInt32LE(0), 0x184d2204, 'magic');
            assert.equal(frame[4], 0x60, 'FLG: version 01 + block independence (Kafka rejects dependent blocks)');
            assert.equal(frame[5], 0x40, 'BD: 64 KB maximum block size');
            assert.equal(frame[6], (xxh32.hash(0, frame, 4, 2) >>> 8) & 0xff, 'header checksum');
            assert.equal(frame.readUInt32LE(frame.length - 4), 0, 'end mark');
        });

        it('never writes a block larger than 64 KB', function () {
            const input = crypto.randomBytes(300000); // incompressible: stored blocks show their sizes exactly
            const frame = codecs.lz4CompressFrame(input);
            let offset = 7;
            let blocks = 0;
            for (;;) {
                const word = frame.readUInt32LE(offset);
                offset += 4;
                if (word === 0) break;
                const size = word & 0x7fffffff;
                assert.ok(size <= 65536, `block size ${size}`);
                offset += size;
                blocks++;
            }
            assert.equal(offset, frame.length);
            assert.ok(blocks >= 5);
        });

        it('decodes a frame produced by the Kafka Java client', function () {
            const out = codecs.lz4DecompressFrame(Buffer.from(JAVA_LZ4_FRAME, 'base64'));
            assert.equal(out.toString('utf8'), JAVA_LZ4_TEXT);
        });

        it('round-trips sizes around the block boundary', function () {
            for (const size of [65471, 65472, 65473, 65535, 65536, 65537, 131072]) {
                const buf = Buffer.alloc(size);
                for (let i = 0; i < size; i++) buf[i] = (i * 31 + (i >> 7)) & 0x3f;
                assert.ok(codecs.lz4DecompressFrame(codecs.lz4CompressFrame(buf)).equals(buf), String(size));
            }
        });

        it('accepts plain Uint8Arrays', function () {
            const u8 = new Uint8Array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5]);
            const out = codecs.lz4DecompressFrame(codecs.lz4CompressFrame(u8));
            assert.deepEqual([...out], [...u8]);
        });

        it('rejects garbage with a rejected promise (never a sync throw into kafkajs)', async function () {
            const codec = codecs.lz4Codec();
            await assert.rejects(codec.decompress(Buffer.from('not lz4 at all')));
        });
    });
});
