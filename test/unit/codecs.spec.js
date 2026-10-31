'use strict';

const assert = require('node:assert/strict');
const { execFile } = require('node:child_process');
const crypto = require('node:crypto');
const kafkajs = require('kafkajs');
const lz4js = require('lz4js');
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

// 'hello kafka ' x 12500 as a frame WITH block checksums (FLG 0x70), decoded byte-identically by an independent
// decoder (lz4-java 1.8.0 LZ4FrameInputStream, the library Kafka ships).
const BLOCK_CHECKSUM_FRAME =
    'BCJNGHBArRYBAADPaGVsbG8ga2Fma2EgDAD' +
    '/'.repeat(341) +
    'nFBhZmthIBwCcZoWAQAAz2hlbGxvIGthZmthIAwA' +
    '/'.repeat(341) +
    '5xQYWZrYSAcAnGaYAAAAM9oZWxsbyBrYWZrYSAMAP' +
    '/'.repeat(98) +
    'llBhZmthIAAlDTAAAAAA';
const BLOCK_CHECKSUM_TEXT = 'hello kafka '.repeat(12500);

/** A frame header (magic, FLG, BD, optional content size, header checksum). */
function lz4Header(flg, bd, contentSize) {
    const h = Buffer.alloc(contentSize === undefined ? 7 : 15);
    h.writeUInt32LE(0x184d2204, 0);
    h[4] = flg;
    h[5] = bd;
    if (contentSize !== undefined) h.writeUInt32LE(contentSize, 6);
    h[h.length - 1] = (xxh32.hash(0, h, 4, h.length - 5) >>> 8) & 0xff;
    return h;
}

/** One block: 4-byte size word (bit 31: stored uncompressed), the data, optionally its xxh32 checksum after it. */
function lz4Block(data, { stored = false, checksum = false } = {}) {
    const word = Buffer.alloc(4);
    word.writeUInt32LE(((stored ? 0x80000000 : 0) | data.length) >>> 0, 0);
    const parts = [word, data];
    if (checksum) {
        const sum = Buffer.alloc(4);
        sum.writeUInt32LE(xxh32.hash(0, data, 0, data.length) >>> 0, 0);
        parts.push(sum);
    }
    return Buffer.concat(parts);
}

/** Re-encode one of our frames with FLG.BChecksum set and a checksum after every block (as the spec says). */
function withBlockChecksums(frame) {
    const parts = [lz4Header(frame[4] | 0x10, frame[5])];
    for (let i = 7; ;) {
        const word = frame.readUInt32LE(i);
        if (word === 0) break;
        const size = word & 0x7fffffff;
        parts.push(lz4Block(frame.subarray(i + 4, i + 4 + size), { stored: word !== size, checksum: true }));
        i += 4 + size;
    }
    parts.push(Buffer.alloc(4));
    return Buffer.concat(parts);
}

const END = Buffer.alloc(4);
// "1 literal: 'A'" as a compressed block
const TINY_BLOCK = Buffer.from([0x10, 0x41]);

/** Run a script in a child Node process under a virtual memory limit (POSIX sh); resolves { code, out }. */
function runLimited(kb, script) {
    return new Promise((resolve) => {
        const cmd = `ulimit -v ${kb} && exec "${process.execPath}" -e "$0"`;
        execFile('/bin/sh', ['-c', cmd, script], { timeout: 60000 }, (err, stdout, stderr) => {
            resolve({ code: err ? err.code : 0, out: `${stdout}${stderr}` });
        });
    });
}

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

        it('decodes frames with dependent blocks (matches into the previous block, lz4js\' own writer)', function () {
            const input = Buffer.alloc(4.5 * 1024 * 1024);
            for (let i = 0; i < input.length; i++) input[i] = (i * 7 + (i >> 12)) & 0xff;
            const frame = Buffer.from(lz4js.compress(input));
            assert.ok(codecs.lz4DecompressFrame(frame).equals(input));
            // the second block on its own refers to data before it: it really is dependent
            const first = frame.readUInt32LE(7);
            const second = frame.subarray(7 + 4 + first);
            assert.throws(() => codecs.lz4DecompressFrame(Buffer.concat([frame.subarray(0, 7), second])),
                /Invalid LZ4 data: match offset out of range/);
        });
    });

    describe('LZ4 block checksums (CODEC-1)', function () {
        it('decodes a frame with block checksums like Kafka\'s Java decoder does', function () {
            const out = codecs.lz4DecompressFrame(Buffer.from(BLOCK_CHECKSUM_FRAME, 'base64'));
            assert.equal(out.length, BLOCK_CHECKSUM_TEXT.length);
            assert.ok(out.equals(Buffer.from(BLOCK_CHECKSUM_TEXT)));
        });

        it('round-trips text, random and small data through frames with block checksums', function () {
            for (const input of [Buffer.from('hello kafka '.repeat(12500)), crypto.randomBytes(100000),
                Buffer.from('{"a":1,"b":"some json payload"}')]) {
                const frame = withBlockChecksums(codecs.lz4CompressFrame(input));
                assert.ok(codecs.lz4DecompressFrame(frame).equals(input));
            }
        });

        it('rejects a block whose checksum does not match (corrupt data is never returned)', async function () {
            const frame = withBlockChecksums(codecs.lz4CompressFrame(Buffer.from('hello kafka '.repeat(100))));
            frame[12] ^= 0x01; // inside the first block's data
            await assert.rejects(codecs.lz4Codec().decompress(frame), /Invalid LZ4 data: block checksum mismatch/);
        });
    });

    describe('LZ4 decompression bombs (CRASH-1)', function () {
        it('allocates for the real blocks, not for the content size a frame declares (2 GiB here)', function () {
            const frame = Buffer.concat([lz4Header(0x68, 0x40, 0x7fffffff), lz4Block(Buffer.from('A'), { stored: true }), END]);
            assert.equal(codecs.lz4DecompressFrame(frame).toString(), 'A', 'like Kafka, the content size is not used');
        });

        it('allocates for the real blocks, not for the declared maximum block size (4 MiB x 1000 here)', function () {
            const frame = Buffer.concat([lz4Header(0x60, 0x70), ...Array(1000).fill(lz4Block(TINY_BLOCK)), END]);
            const out = codecs.lz4DecompressFrame(frame);
            assert.equal(out.toString(), 'A'.repeat(1000));
            assert.ok(out.buffer.byteLength < 1024 * 1024, `kept ${out.buffer.byteLength} bytes`);
        });

        it(`rejects a batch that could decompress to more than ${codecs.LZ4_MAX_OUTPUT / 1048576} MiB, before allocating`,
            async function () {
                assert.equal(codecs.LZ4_MAX_OUTPUT, 256 * 1024 * 1024);
                // 70 compressed 4 MiB blocks of 16.5 KB each: up to 280 MiB of output from 1.2 MB of input
                const frame = Buffer.concat([lz4Header(0x60, 0x70), ...Array(70).fill(lz4Block(Buffer.alloc(16500, 0x11))), END]);
                const start = Date.now();
                await assert.rejects(codecs.lz4Codec().decompress(frame),
                    /Invalid LZ4 data: the batch would decompress to more than 256 MiB/);
                assert.ok(Date.now() - start < 1000, 'nothing was decoded or allocated');
                assert.throws(() => codecs.lz4DecompressFrame(codecs.lz4CompressFrame(Buffer.alloc(5000)), { maxOutput: 4096 }),
                    /more than 0 MiB/);
            });

        it('does not abort Node with a fatal out-of-memory error where 2 GiB cannot be allocated', async function () {
            if (process.platform === 'win32') this.skip();
            this.timeout(120000);
            const limit = 2000000; // KiB of virtual memory: enough for Node, not for a 2 GiB buffer
            const alive = await runLimited(limit, 'console.log("alive")');
            if (alive.code !== 0 || !alive.out.includes('alive')) this.skip(); // this system cannot run Node so limited
            const script = `
                const codecs = require(${JSON.stringify(require.resolve('../../lib/codecs'))});
                const xxh32 = require(${JSON.stringify(require.resolve('lz4js/xxh32.js'))});
                ${lz4Header.toString()}
                ${lz4Block.toString()}
                const frames = [
                    Buffer.concat([lz4Header(0x68, 0x40, 0x7fffffff), lz4Block(Buffer.from('A'), { stored: true }), Buffer.alloc(4)]),
                    Buffer.concat([lz4Header(0x60, 0x70), ...Array(1000).fill(lz4Block(Buffer.from([0x10, 0x41]))), Buffer.alloc(4)])
                ];
                (async () => {
                    for (const f of frames) {
                        const out = await codecs.lz4Codec().decompress(f).then((o) => 'decoded ' + o.length, (e) => 'rejected');
                        console.log(out);
                    }
                })();`;
            const r = await runLimited(limit, script);
            assert.equal(r.code, 0, r.out.slice(0, 2000));
            assert.match(r.out, /decoded 1\ndecoded 1000/);
        });

        it('rejects malformed frames with a catchable error', async function () {
            const good = codecs.lz4CompressFrame(Buffer.from('abcdefgh'.repeat(50)));
            const cases = {
                'bad magic': Buffer.from('not lz4 at all'),
                'frame version 2': Buffer.concat([lz4Header(0xa0, 0x40), END]),
                'dictionary id': Buffer.concat([lz4Header(0x61, 0x40), END]),
                'block size index 3': Buffer.concat([lz4Header(0x60, 0x30), END]),
                'no end mark': good.subarray(0, good.length - 4),
                'block longer than the input': good.subarray(0, 20),
                'block larger than the maximum': Buffer.concat([lz4Header(0x60, 0x40),
                    lz4Block(Buffer.alloc(65537), { stored: true }), END]),
                'match offset 0': Buffer.concat([lz4Header(0x60, 0x40), lz4Block(Buffer.from([0x10, 0x41, 0, 0, 0x00])), END]),
                'match before the start': Buffer.concat([lz4Header(0x60, 0x40), lz4Block(Buffer.from([0x10, 0x41, 2, 0, 0x00])), END]),
                'literals past the block': Buffer.concat([lz4Header(0x60, 0x40), lz4Block(Buffer.from([0x50, 0x41])), END]),
                'truncated match length': Buffer.concat([lz4Header(0x60, 0x40), lz4Block(Buffer.from([0x1f, 0x41, 1, 0, 0xff])), END]),
                'more than the maximum block size': Buffer.concat([lz4Header(0x60, 0x40),
                    lz4Block(Buffer.from([0x1f, 0x41, 1, 0, ...Array(300).fill(0xff), 0])), END])
            };
            for (const [name, frame] of Object.entries(cases)) {
                assert.throws(() => codecs.lz4DecompressFrame(frame), /^Error: Invalid LZ4 data: /, name);
                const err = await codecs.lz4Codec().decompress(frame).then(() => null, (e) => e);
                assert.ok(err && err.retriable === false, `${name}: non-retriable rejection (the same batch fails the same way)`);
            }
        });

        it('never throws anything else and never hangs on damaged frames (fuzz)', function () {
            const base = codecs.lz4CompressFrame(Buffer.from(JSON.stringify(Array.from({ length: 2000 }, (_, i) => ({ i })))));
            const rnd = (n) => crypto.randomInt(n);
            const start = Date.now();
            for (let k = 0; k < 3000; k++) {
                const frame = Buffer.from(base);
                for (let j = 1 + (k % 8); j > 0; j--) frame[7 + rnd(frame.length - 7)] = rnd(256);
                try {
                    codecs.lz4DecompressFrame(frame);
                } catch (e) {
                    assert.match(e.message, /^Invalid LZ4 data: /);
                }
            }
            assert.ok(Date.now() - start < 5000, `${Date.now() - start} ms`);
        });
    });
});
