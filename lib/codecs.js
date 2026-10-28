'use strict';

/**
 * Register Snappy and LZ4 compression codecs into kafkajs.
 *
 * kafkajs only ships GZIP; a consumer that receives a Snappy/LZ4 batch dies
 * with KafkaJSNotImplemented (5-kafkajs-internals.md §0.8). Both codecs used
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
 * Decompress an LZ4 frame (any producer: Java, librdkafka, kafkajs-lz4, ours).
 *
 * @param {Uint8Array} input
 * @returns {Buffer}
 */
function lz4DecompressFrame(input) {
    const out = lz4js.decompress(input);
    return Buffer.from(out.buffer, out.byteOffset, out.byteLength);
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
    lz4DecompressFrame
};
