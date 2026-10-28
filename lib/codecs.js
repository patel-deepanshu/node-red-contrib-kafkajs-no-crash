'use strict';

/**
 * Register Snappy and LZ4 compression codecs into kafkajs.
 *
 * kafkajs only ships GZIP; a consumer that receives a Snappy/LZ4 batch dies
 * with KafkaJSNotImplemented (src/protocol/message/compression/index.js). Both codecs used
 * here are pure JavaScript, so they install everywhere Node-RED runs.
 *
 * kafkajs codec contract: `CompressionCodecs[type] = () => ({ compress, decompress })`
 * where `compress(encoder)` gets a kafkajs Encoder (bytes in `encoder.buffer`)
 * and both functions return a Promise<Buffer>.
 *
 * LZ4: lz4js' own frame writer is not usable with Kafka. It omits the
 * "block independence" flag (Kafka's Lz4BlockInputStream rejects such frames
 * with "Dependent block stream is unsupported"), lets matches reach into
 * previous blocks, and can start a match closer than 12 bytes to the end of a
 * full block (liblz4's safe decoder rejects that). We therefore build the
 * frame ourselves from lz4js' block compressor, the same way the Java client
 * does: independent blocks, 64 KB maximum block size, no checksums.
 *
 * Its frame reader is not usable either: it sizes the output from numbers in
 * the frame (up to 4 GB for a few bytes of input) and, when that allocation
 * fails, falls back to a JS Array filled in a loop, which aborts Node with a
 * fatal, uncatchable out-of-memory error. It also reads block checksums from
 * the wrong place (silently corrupt output). We therefore decode frames
 * ourselves too, with every size checked against the input and a total cap.
 */

const lz4js = require('lz4js');
const xxh32 = require('lz4js/xxh32.js');

const LZ4_MAGIC = 0x184d2204;
const LZ4_FLG = 0x60; // version 01 + block independence
const LZ4_BD = 0x40; // maximum block size 64 KB (same as the Java client)
const LZ4_MAX_BLOCK = 64 * 1024;
// Keep every block well below the declared maximum so the decoder's output
// buffer always has room past the last match (liblz4 MFLIMIT = 12 bytes).
const LZ4_CHUNK = LZ4_MAX_BLOCK - 64;
const LZ4_UNCOMPRESSED_FLAG = 0x80000000;
const LZ4_FLG_VERSION_MASK = 0xc0;
const LZ4_FLG_VERSION = 0x40;
const LZ4_FLG_BLOCK_CHECKSUM = 0x10;
const LZ4_FLG_CONTENT_SIZE = 0x08;
const LZ4_FLG_DICT_ID = 0x01;
/** Maximum block size by the BD byte's 3-bit index (LZ4 frame format). */
const LZ4_BLOCK_SIZES = { 4: 64 * 1024, 5: 256 * 1024, 6: 1024 * 1024, 7: 4 * 1024 * 1024 };
/** One LZ4 batch never decompresses to more than this: refuse it (catchably) instead of allocating. */
const LZ4_MAX_OUTPUT = 256 * 1024 * 1024;
const LZ4_MIN_MATCH = 4;
/** A compressed byte expands to at most 255 bytes (a match length byte of 255), so this bounds any block. */
const LZ4_MAX_RATIO = 255;

/** The 7-byte frame header: magic, FLG, BD, header checksum. */
function lz4FrameHeader() {
    const header = Buffer.alloc(7);
    header.writeUInt32LE(LZ4_MAGIC, 0);
    header[4] = LZ4_FLG;
    header[5] = LZ4_BD;
    header[6] = (xxh32.hash(0, header, 4, 2) >>> 8) & 0xff;
    return header;
}

/**
 * Compress bytes into a Kafka-compatible LZ4 frame.
 *
 * @param {Uint8Array} input
 * @returns {Buffer}
 */
function lz4CompressFrame(input) {
    const src = Buffer.isBuffer(input) ? input : Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    const parts = [lz4FrameHeader()];
    const hashTable = new Uint32Array(1 << 16);
    const scratch = Buffer.alloc(lz4js.compressBound(LZ4_CHUNK));
    for (let offset = 0; offset < src.length; offset += LZ4_CHUNK) {
        // A block-local view plus a cleared hash table keeps blocks independent.
        const chunk = src.subarray(offset, Math.min(offset + LZ4_CHUNK, src.length));
        hashTable.fill(0);
        const size = lz4js.compressBlock(chunk, scratch, 0, chunk.length, hashTable);
        const blockHeader = Buffer.alloc(4);
        if (size > 0 && size < chunk.length) {
            blockHeader.writeUInt32LE(size, 0);
            parts.push(blockHeader, Buffer.from(scratch.subarray(0, size)));
        } else {
            // incompressible (or tiny) data is stored as-is
            blockHeader.writeUInt32LE((LZ4_UNCOMPRESSED_FLAG | chunk.length) >>> 0, 0);
            parts.push(blockHeader, chunk);
        }
    }
    parts.push(Buffer.alloc(4)); // end mark
    return Buffer.concat(parts);
}

/**
 * The error for data that cannot be decoded. kafkajs would re-fetch the same
 * batch and fail the same way, so it is marked non-retriable: the consumer
 * reports it at once (and its supervisor retries with the normal backoff).
 */
function lz4Error(detail) {
    const err = new Error(`Invalid LZ4 data: ${detail}`);
    err.retriable = false;
    return err;
}

/** Read an LZ4 length extension (a run of 255 bytes ended by a smaller one). */
function lz4Length(src, pos, end, length) {
    let b;
    do {
        if (pos >= end) throw lz4Error('truncated block');
        b = src[pos++];
        length += b;
    } while (b === 255);
    return { length, pos };
}

/**
 * Decode one compressed LZ4 block, src[sIndex, sEnd), into dst at dIndex, never writing at or past dEnd.
 * A match may reach back into earlier blocks of the same frame (frames with dependent blocks), never
 * before the start of the output. Throws on malformed input instead of writing garbage.
 *
 * @returns {number} the new dIndex
 */
function lz4DecodeBlock(src, sIndex, sEnd, dst, dIndex, dEnd) {
    let s = sIndex;
    let d = dIndex;
    while (s < sEnd) {
        const token = src[s++];
        let literals = token >>> 4;
        if (literals === 15) ({ length: literals, pos: s } = lz4Length(src, s, sEnd, literals));
        if (literals > sEnd - s) throw lz4Error('literals run past the end of their block');
        if (literals > dEnd - d) throw lz4Error('a block decompresses to more than its maximum size');
        for (const end = s + literals; s < end;) dst[d++] = src[s++];
        if (s === sEnd) break; // the last sequence of a block has literals only
        if (sEnd - s < 2) throw lz4Error('truncated block');
        const offset = src[s] | (src[s + 1] << 8);
        s += 2;
        if (offset === 0 || offset > d) throw lz4Error('match offset out of range');
        let matchLength = token & 15;
        if (matchLength === 15) ({ length: matchLength, pos: s } = lz4Length(src, s, sEnd, matchLength));
        matchLength += LZ4_MIN_MATCH;
        if (matchLength > dEnd - d) throw lz4Error('a block decompresses to more than its maximum size');
        // byte by byte: a match may overlap the bytes it produces (offset < length repeats a pattern)
        for (let from = d - offset, end = d + matchLength; d < end;) dst[d++] = dst[from++];
    }
    return d;
}

/**
 * Decompress an LZ4 frame (any producer: Java, librdkafka, kafkajs-lz4, ours).
 *
 * Checks what Kafka's own Java decoder (Lz4BlockInputStream) checks: magic,
 * version, block sizes within the frame's maximum and the input, and block
 * checksums. Like it, the optional content size and content checksum are not
 * used, and the header checksum is not verified (Kafka < 0.10 wrote a wrong
 * one, KAFKA-3160). The output is allocated from the actual blocks, never
 * from a size the frame merely declares, and capped at `maxOutput`.
 *
 * @param {Uint8Array} input
 * @param {{maxOutput?: number}} [options] maxOutput: largest accepted output (default 256 MiB)
 * @returns {Buffer}
 * @throws {Error} 'Invalid LZ4 data: ...' (a catchable error, also for a frame that is too large)
 */
function lz4DecompressFrame(input, options) {
    const src = Buffer.isBuffer(input) ? input : Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    const maxOutput = options && Number.isSafeInteger(options.maxOutput) ? options.maxOutput : LZ4_MAX_OUTPUT;
    if (src.length < 7 || src.readUInt32LE(0) !== LZ4_MAGIC) throw lz4Error('not an LZ4 frame (bad magic number)');
    const flg = src[4];
    if ((flg & LZ4_FLG_VERSION_MASK) !== LZ4_FLG_VERSION) throw lz4Error(`unsupported frame version ${flg >>> 6}`);
    if (flg & LZ4_FLG_DICT_ID) throw lz4Error('frames that need a dictionary are not supported');
    const maxBlock = LZ4_BLOCK_SIZES[(src[5] >>> 4) & 7];
    if (!maxBlock) throw lz4Error('invalid maximum block size');
    const checksumBytes = flg & LZ4_FLG_BLOCK_CHECKSUM ? 4 : 0;

    // 1. Walk the block headers: check every size against the input and the frame's maximum, verify the
    //    block checksums, and add up the largest possible output before allocating anything.
    const blocks = [];
    let pos = 7 + (flg & LZ4_FLG_CONTENT_SIZE ? 8 : 0);
    let bound = 0;
    for (;;) {
        if (pos + 4 > src.length) throw lz4Error('truncated frame');
        const word = src.readUInt32LE(pos);
        pos += 4;
        if (word === 0) break; // end mark (an optional content checksum may follow; Kafka ignores it too)
        const stored = word >= LZ4_UNCOMPRESSED_FLAG;
        const size = stored ? word - LZ4_UNCOMPRESSED_FLAG : word;
        if (size > maxBlock) throw lz4Error(`a block of ${size} bytes exceeds the frame's maximum of ${maxBlock}`);
        if (size + checksumBytes > src.length - pos) throw lz4Error('truncated frame');
        if (checksumBytes && (xxh32.hash(0, src, pos, size) >>> 0) !== src.readUInt32LE(pos + size)) {
            throw lz4Error('block checksum mismatch');
        }
        const room = stored ? size : Math.min(maxBlock, size * LZ4_MAX_RATIO);
        blocks.push({ start: pos, size, stored, room });
        bound += room;
        if (bound > maxOutput) {
            throw lz4Error(`the batch would decompress to more than ${Math.round(maxOutput / 1048576)} MiB`);
        }
        pos += size + checksumBytes;
    }

    // 2. Decode into one buffer (a failed allocation throws a catchable RangeError).
    let dst;
    try {
        dst = Buffer.alloc(bound);
    } catch (err) {
        throw lz4Error(`cannot allocate ${bound} bytes (${err && err.message})`);
    }
    let d = 0;
    for (const b of blocks) {
        if (b.stored) {
            src.copy(dst, d, b.start, b.start + b.size);
            d += b.size;
        } else {
            d = lz4DecodeBlock(src, b.start, b.start + b.size, dst, d, d + b.room);
        }
    }
    // Records reference the decompressed buffer: copy when most of it is unused slack.
    return d < bound / 2 ? Buffer.from(dst.subarray(0, d)) : dst.subarray(0, d);
}

/** kafkajs codec factory for LZ4. */
const lz4Codec = () => ({
    async compress(encoder) {
        return lz4CompressFrame(encoder.buffer);
    },
    async decompress(buffer) {
        return lz4DecompressFrame(buffer);
    }
});

/**
 * Whether `codecs[type]` is a working codec. kafkajs pre-populates Snappy/LZ4
 * with factories that THROW KafkaJSNotImplemented, so presence is not enough.
 */
function isUsable(codecs, type) {
    try {
        const factory = codecs[type];
        if (typeof factory !== 'function') return false;
        const codec = factory();
        return !!codec && typeof codec.compress === 'function' && typeof codec.decompress === 'function';
    } catch (_) {
        return false;
    }
}

/**
 * Register Snappy (kafkajs-snappy) and LZ4 (lz4js) into a kafkajs module's
 * CompressionCodecs, unless a working codec is already registered (e.g. by the
 * user). Each registration is independent; never throws.
 *
 * @param {{CompressionCodecs: object, CompressionTypes: object}} kafkajs
 * @returns {{snappy: boolean, lz4: boolean}} availability after registration
 */
function register(kafkajs) {
    const result = { snappy: false, lz4: false };
    let codecs;
    let types;
    try {
        codecs = kafkajs && kafkajs.CompressionCodecs;
        types = kafkajs && kafkajs.CompressionTypes;
    } catch (_) {
        return result;
    }
    if (!codecs || !types) return result;
    try {
        if (!isUsable(codecs, types.Snappy)) codecs[types.Snappy] = require('kafkajs-snappy');
    } catch (_) {
        // kafkajs-snappy missing or broken: Snappy stays unavailable
    }
    try {
        if (!isUsable(codecs, types.LZ4)) codecs[types.LZ4] = lz4Codec;
    } catch (_) {
        // LZ4 stays unavailable
    }
    result.snappy = isUsable(codecs, types.Snappy);
    result.lz4 = isUsable(codecs, types.LZ4);
    return result;
}

module.exports = {
    register,
    lz4Codec,
    lz4CompressFrame,
    lz4DecompressFrame,
    LZ4_MAX_OUTPUT
};
