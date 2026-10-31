'use strict';

/**
 * In-memory queue with the same interface and overflow semantics as DiskQueue (producer queue mode
 * 'memory'). Records are snapshotted as JSON at enqueue time, exactly like on disk, so both queues behave
 * identically for callers (and are tested with one shared contract suite). Nothing survives a restart.
 *
 * Dead letters cannot go to a file here: the most recent DEAD_LETTER_KEEP of them stay in memory (until
 * Node-RED restarts) and getDeadLetters() returns them, in the same shape as the lines of DiskQueue's
 * dead-letter.jsonl.
 *
 * Positions are { seg: 0, end: <sequence number of the record> }.
 */

const {
    intOption,
    normalizeQueueOptions,
    serializeRecord,
    maxPosition,
    limitText,
    queueFullError,
    buildDeadLetter,
    handled
} = require('./disk-queue');

/** How many dead-lettered entries are kept for inspection (getDeadLetters()). */
const DEAD_LETTER_KEEP = 100;
/** Compact the backing array once this many consumed slots accumulated at its front. */
const COMPACT_AT = 1024;

class MemoryQueue {
    /**
     * @param {object} [options] same as DiskQueue; dir, fsync, segmentMaxBytes and deadLetterMaxBytes are ignored
     * @param {number} [options.maxBytes=0] limit of pending bytes (JSON length of the records; 0 = unlimited)
     * @param {number} [options.maxMessages=0] limit of pending records (0 = unlimited)
     * @param {'drop-oldest'|'drop-newest'} [options.overflow='drop-oldest']
     * @param {number} [options.peekMaxBytes=8388608] byte budget of one peek() (at least one record is returned)
     * @param {{warn: Function, error: Function, debug: Function}} [options.logger]
     */
    constructor(options) {
        let o;
        try {
            o = normalizeQueueOptions(options);
        } catch (_) {
            o = normalizeQueueOptions({}); // hostile options object: fall back to defaults, never throw
        }
        this._maxBytes = o.maxBytes;
        this._maxMessages = o.maxMessages;
        this._overflow = o.overflow;
        this._peekMaxBytes = o.peekMaxBytes;
        this._logger = o.logger;
        this._items = [];      // { json, bytes, seq, ts? }; consumed ones before this._head
        this._head = 0;
        this._nextSeq = 1;
        this._bytes = 0;
        this._closed = false;
        this._droppedTotal = 0;
        this._deadLettered = 0;
        this._deadLetters = [];
    }

    /** Always null: nothing is stored on disk. */
    get dir() {
        return null;
    }

    /** Pending records. */
    get size() {
        return this._items.length - this._head;
    }

    /** Pending bytes (sum of Buffer.byteLength(JSON.stringify(record))). */
    get bytes() {
        return this._bytes;
    }

    /** True once close() was called. */
    get closed() {
        return this._closed;
    }

    /**
     * Snapshot of the queue state. Never throws.
     * @returns {{size:number, bytes:number, segments:number, droppedTotal:number, deadLettered:number, oldestTs?:number}}
     */
    stats() {
        const s = {
            size: this.size,
            bytes: this._bytes,
            segments: 0,
            droppedTotal: this._droppedTotal,
            deadLettered: this._deadLettered
        };
        const head = this._items[this._head];
        if (head) {
            if (head.ts === undefined) {
                try {
                    const rec = JSON.parse(head.json);
                    head.ts = rec !== null && typeof rec === 'object' && Number.isFinite(rec.ts) ? rec.ts : null;
                } catch (_) {
                    head.ts = null;
                }
            }
            if (head.ts !== null) {
                s.oldestTs = head.ts;
            }
        }
        return s;
    }

    /** Nothing to do (kept for interface parity; calling it is optional). */
    open() {
        return this._closed ? handled(Promise.reject(new Error('queue closed'))) : Promise.resolve();
    }

    /**
     * @param {*} record JSON-serialisable value (snapshotted at call time)
     * @returns {Promise<{dropped:number}>}
     */
    enqueue(record) {
        return this.enqueueMany([record]);
    }

    /**
     * Append records atomically (all or nothing) and in order.
     * @param {Array<*>} records
     * @returns {Promise<{dropped:number}>}
     */
    enqueueMany(records) {
        try {
            return Promise.resolve(this._enqueueMany(records));
        } catch (err) {
            return handled(Promise.reject(err));
        }
    }

    _enqueueMany(records) {
        if (this._closed) {
            throw new Error('queue closed');
        }
        if (!Array.isArray(records)) {
            throw new TypeError('enqueueMany() expects an array of records');
        }
        const items = new Array(records.length);
        let b = 0;
        for (let i = 0; i < records.length; i++) {
            const json = serializeRecord(records[i], i);
            items[i] = { json, bytes: Buffer.byteLength(json, 'utf8'), seq: 0 };
            b += items[i].bytes;
        }
        const n = items.length;
        if (n === 0) {
            return { dropped: 0 };
        }
        const maxM = this._maxMessages;
        const maxB = this._maxBytes;
        if ((maxM && n > maxM) || (maxB && b > maxB)) {
            throw queueFullError(`Memory queue: cannot queue ${n} message(s) of ${b} bytes: larger than the ` +
                `whole queue limit (${limitText(maxM, maxB)})`);
        }
        const fits = () => (!maxM || this.size + n <= maxM) && (!maxB || this._bytes + b <= maxB);
        let dropped = 0;
        if (!fits()) {
            if (this._overflow === 'drop-newest') {
                throw queueFullError(`Memory queue is full (${this.size} messages queued, limit ` +
                    `${limitText(maxM, maxB)}); message rejected (overflow policy drop-newest)`);
            }
            while (!fits() && this.size > 0) {
                this._bytes -= this._items[this._head].bytes;
                this._items[this._head] = undefined;
                this._head++;
                dropped++;
            }
            this._compact();
            this._droppedTotal += dropped;
            this._log('warn', `Memory queue full (limit ${limitText(maxM, maxB)}): dropped ${dropped} oldest ` +
                'message(s) to make room');
        }
        for (const item of items) {
            item.seq = this._nextSeq++;
            this._items.push(item);
        }
        this._bytes += b;
        return { dropped };
    }

    /**
     * @param {number} [max=100]
     * @returns {Promise<Array<{record:*, pos:{seg:number,end:number}}>>}
     */
    peek(max = 100) {
        if (this._closed) {
            return handled(Promise.reject(new Error('queue closed')));
        }
        const out = [];
        let bytes = 0;
        try {
            const limit = intOption(max, 100, 0, 1000000);
            for (let i = this._head; i < this._items.length && out.length < limit; i++) {
                const item = this._items[i];
                if (out.length > 0 && bytes + item.bytes > this._peekMaxBytes) {
                    break;
                }
                bytes += item.bytes;
                out.push({ record: JSON.parse(item.json), pos: { seg: 0, end: item.seq } });
            }
        } catch (err) {
            return handled(Promise.reject(err));
        }
        return Promise.resolve(out);
    }

    /**
     * Remove every record up to the highest position given. Idempotent.
     * @returns {Promise<number>} number of records removed
     */
    ack(entries) {
        if (this._closed) {
            return handled(Promise.reject(new Error('queue closed')));
        }
        let target;
        try {
            target = maxPosition(entries);
        } catch (err) {
            return handled(Promise.reject(err)); // e.g. a getter that throws
        }
        let removed = 0;
        if (target && target.seg === 0) {
            while (this._head < this._items.length && this._items[this._head].seq <= target.end) {
                this._bytes -= this._items[this._head].bytes;
                this._items[this._head] = undefined;
                this._head++;
                removed++;
            }
            this._compact();
        }
        return Promise.resolve(removed);
    }

    /**
     * Count a dead-lettered entry and keep the most recent DEAD_LETTER_KEEP in memory (see getDeadLetters()).
     * @returns {Promise<void>}
     */
    deadLetter(entry, err) {
        if (this._closed) {
            return handled(Promise.reject(new Error('queue closed')));
        }
        try {
            this._deadLetters.push(JSON.parse(buildDeadLetter(entry, err).json));
            if (this._deadLetters.length > DEAD_LETTER_KEEP) {
                this._deadLetters.shift();
            }
            this._deadLettered++;
        } catch (e) {
            return handled(Promise.reject(e));
        }
        return Promise.resolve();
    }

    /**
     * The most recent dead-lettered entries (at most DEAD_LETTER_KEEP, oldest first), each in the shape of a
     * dead-letter.jsonl line: { ts: ISO string, error: toPlainError(err) | string, record | raw }. Returns
     * copies (callers may change them), also after close(). Never throws.
     *
     * @returns {Array<{ts: string, error: (object|string), record?: *, raw?: string}>}
     */
    getDeadLetters() {
        try {
            return JSON.parse(JSON.stringify(this._deadLetters));
        } catch (_) {
            return []; // cannot happen: the entries were parsed from JSON
        }
    }

    /** @returns {Promise<number>} number of records dropped */
    clear() {
        if (this._closed) {
            return handled(Promise.reject(new Error('queue closed')));
        }
        const n = this.size;
        this._items = [];
        this._head = 0;
        this._bytes = 0;
        return Promise.resolve(n);
    }

    /** Discard everything (memory only). Idempotent, never rejects. */
    close() {
        if (!this._closed) {
            this._closed = true;
            if (this.size > 0) {
                this._log('warn', `Memory queue closed with ${this.size} unsent message(s); they are discarded`);
            }
            this._items = [];
            this._head = 0;
            this._bytes = 0;
        }
        return Promise.resolve();
    }

    _compact() {
        if (this._head === this._items.length) {
            this._items = [];
            this._head = 0;
        } else if (this._head >= COMPACT_AT && this._head * 2 >= this._items.length) {
            this._items = this._items.slice(this._head);
            this._head = 0;
        }
    }

    _log(level, message) {
        try {
            const logger = this._logger;
            if (logger && typeof logger[level] === 'function') {
                logger[level](message);
            }
        } catch (_) {
            // a logger must never break the queue
        }
    }
}

module.exports = { MemoryQueue, DEAD_LETTER_KEEP };
