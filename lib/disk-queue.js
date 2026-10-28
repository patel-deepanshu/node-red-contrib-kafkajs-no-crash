'use strict';

/**
 * Durable, segmented FIFO used by the producer's store-and-forward ("offline queue").
 *
 * On-disk layout of `dir`:
 *   seg-<16-digit seq>.jsonl   append-only segments, one JSON record per line ('\n' terminated)
 *   cursor.json                {"seg":<seq>,"off":<bytes>} = position of the oldest pending record,
 *                              replaced atomically (write tmp, fsync, rename, fsync dir)
 *   dead-letter.jsonl          records that could not be delivered (rotated to dead-letter.1.jsonl)
 *
 * Durability rules (why the code looks the way it does):
 * - A record is only acknowledged to the caller (enqueue resolves) after its bytes are written, and
 *   fsynced when `fsync` is true. Concurrent appends are coalesced into one write + one fsync
 *   ("group commit"), so many small enqueues stay cheap.
 * - The cursor is only ever moved forward, and only after the caller acked records (or drop-oldest
 *   overflow dropped them). If the process dies before the new cursor reaches the disk, the records
 *   are delivered again after a restart: at-least-once, never lost.
 * - A crash in the middle of an append leaves a torn line (no trailing '\n') at the end of the newest
 *   segment. open() truncates it: that append was never acknowledged to its caller.
 * - Positions are byte offsets, so acks are positional: ack() moves the cursor to the highest position
 *   given and is idempotent; positions at or before the cursor are ignored.
 * - Every public method returns a promise and never throws synchronously. One internal operation chain
 *   serialises all file access; it swallows failures after forwarding them to the caller, so a failed
 *   operation can never become an unhandled rejection (which would kill Node-RED).
 *
 * Positions handed out by one DiskQueue instance are only meaningful to that instance.
 */

const fs = require('fs');
const path = require('path');

const fsp = fs.promises;

const SEGMENT_RE = /^seg-(\d{16})\.jsonl$/;
const CURSOR_FILE = 'cursor.json';
const CURSOR_TMP_FILE = 'cursor.json.tmp';
const DEAD_LETTER_FILE = 'dead-letter.jsonl';
const DEAD_LETTER_ROTATED_FILE = 'dead-letter.1.jsonl';
const NEWLINE = 0x0a;
const KIB = 1024;
const MIB = 1024 * 1024;
const NEW_SEGMENT = -1;
/** Error codes of platforms (Windows, some network file systems) that cannot fsync a directory. */
const DIR_SYNC_UNSUPPORTED = new Set(['EISDIR', 'EPERM', 'EINVAL', 'EACCES', 'ENOTSUP', 'EBADF']);
/** fdatasync failures meaning "this file system cannot sync" (some FUSE/9p/SMB mounts) rather than an I/O error. */
const FILE_SYNC_UNSUPPORTED = new Set(['EINVAL', 'ENOTSUP', 'ENOSYS']);
/** Windows (anti-virus, indexers) can briefly lock a file that is being renamed. */
const RENAME_RETRY_CODES = new Set(['EPERM', 'EACCES', 'EBUSY']);
const WRAPPED = Symbol('kafkajsNcQueueError');

function noop() {}

/** Mark a promise as handled (callers still see the rejection) so it can never crash the process. */
function handled(promise) {
    promise.catch(noop);
    return promise;
}

/**
 * Parse an integer option without ever throwing: invalid → def, otherwise truncated and clamped.
 */
function intOption(value, def, min, max) {
    if (value === null || value === undefined || value === '' || typeof value === 'boolean') {
        return def;
    }
    let n;
    try {
        n = Number(value);
    } catch (_) {
        return def; // Symbol, or an object whose valueOf throws
    }
    if (!Number.isFinite(n)) {
        return def;
    }
    return Math.min(max, Math.max(min, Math.trunc(n)));
}

/**
 * Normalise the constructor options shared by DiskQueue and MemoryQueue. Never throws.
 * @param {object} [options]
 * @returns {object}
 */
function normalizeQueueOptions(options) {
    const o = options !== null && typeof options === 'object' ? options : {};
    const fsyncOpt = o.fsync;
    return {
        dir: typeof o.dir === 'string' ? o.dir : '',
        maxBytes: intOption(o.maxBytes, 0, 0, Number.MAX_SAFE_INTEGER),
        maxMessages: intOption(o.maxMessages, 0, 0, Number.MAX_SAFE_INTEGER),
        overflow: o.overflow === 'drop-newest' ? 'drop-newest' : 'drop-oldest',
        segmentMaxBytes: intOption(o.segmentMaxBytes, 4 * MIB, 1, 1024 * MIB),
        fsync: fsyncOpt === undefined || fsyncOpt === null
            ? true
            : !(fsyncOpt === false || fsyncOpt === 'false' || fsyncOpt === 0 || fsyncOpt === '0'),
        deadLetterMaxBytes: intOption(o.deadLetterMaxBytes, 50 * MIB, 0, Number.MAX_SAFE_INTEGER),
        // upper bound for the bytes returned by one peek() (always at least one record), so a backlog of
        // large records cannot make one flush batch use unbounded memory
        peekMaxBytes: intOption(o.peekMaxBytes, 8 * MIB, 1, Number.MAX_SAFE_INTEGER),
        // read granularity for peeking and counting; small values are only useful in tests
        readChunkBytes: intOption(o.readChunkBytes, 64 * KIB, 1, 64 * MIB),
        logger: o.logger !== null && typeof o.logger === 'object' ? o.logger : null
    };
}

/**
 * JSON-encode one queue record, snapshotting it at call time.
 * @throws {TypeError} when the record cannot be represented as JSON
 */
function serializeRecord(record, index) {
    let json;
    try {
        json = JSON.stringify(record);
    } catch (err) {
        throw new TypeError(`Queue record ${index} is not JSON-serialisable: ${err && err.message}`);
    }
    if (typeof json !== 'string') {
        throw new TypeError(`Queue record ${index} is not JSON-serialisable (${typeof record})`);
    }
    return json;
}

/** Compare two positions: negative when a is before b. */
function comparePos(a, b) {
    return a.seg !== b.seg ? a.seg - b.seg : a.end - b.end;
}

function isPos(p) {
    return p !== null && typeof p === 'object' &&
        Number.isSafeInteger(p.seg) && p.seg >= 0 && Number.isSafeInteger(p.end) && p.end > 0;
}

/**
 * Highest valid position among peeked entries (or bare positions); null when there is none.
 * Accepts an array or a single entry and ignores anything malformed.
 */
function maxPosition(entries) {
    const list = Array.isArray(entries) ? entries : [entries];
    let max = null;
    for (const e of list) {
        const p = e !== null && typeof e === 'object' && isPos(e.pos) ? e.pos : e;
        if (isPos(p) && (max === null || comparePos(p, max) > 0)) {
            max = { seg: p.seg, end: p.end };
        }
    }
    return max;
}

/** Text describing the configured limits, for error messages. */
function limitText(maxMessages, maxBytes) {
    const parts = [];
    if (maxMessages) {
        parts.push(`${maxMessages} messages`);
    }
    if (maxBytes) {
        parts.push(`${maxBytes} bytes`);
    }
    return parts.join(', ');
}

/**
 * Create the QueueFullError of lib/errors.js (name 'KafkaNcQueueFullError').
 * @param {string} message
 * @returns {Error}
 */
function queueFullError(message) {
    // required lazily: lib/errors.js is a sibling module and this keeps the queue loadable on its own
    const { QueueFullError } = require('./errors');
    return new QueueFullError(message);
}

/** JSON-safe description of a dead-letter reason: toPlainError(err) for objects, String(err) otherwise. */
function deadLetterError(err) {
    if (err !== null && typeof err === 'object') {
        try {
            return require('./errors').toPlainError(err);
        } catch (_) {
            // toPlainError never throws by contract; this only guards against a broken install
        }
    }
    try {
        return String(err);
    } catch (_) {
        return 'unknown error';
    }
}

/**
 * Build the JSON line stored for a dead-lettered entry (shared with MemoryQueue). Never throws.
 * @param {object} entry a peeked entry ({record,pos} or {corrupt,raw,pos}) or a bare record
 * @param {*} err the reason (Error or string such as 'expired')
 * @returns {{ object: object, json: string }}
 */
function buildDeadLetter(entry, err) {
    const object = { ts: new Date().toISOString(), error: deadLetterError(err) };
    if (entry !== null && typeof entry === 'object' && entry.corrupt) {
        object.raw = typeof entry.raw === 'string' ? entry.raw : String(entry.raw);
    } else if (entry !== null && typeof entry === 'object' && Object.prototype.hasOwnProperty.call(entry, 'record')) {
        object.record = entry.record;
    } else {
        object.record = entry;
    }
    let json;
    try {
        json = JSON.stringify(object);
    } catch (_) {
        json = undefined;
    }
    if (typeof json !== 'string') {
        delete object.record;
        object.raw = '[record is not JSON-serialisable]';
        json = JSON.stringify(object);
    }
    return { object, json };
}

/** Wrap a file-system error so its message names the file involved (and keep code/cause). */
function fsError(err, action, file) {
    if (err && err[WRAPPED]) {
        return err;
    }
    const detail = err && err.message ? err.message : String(err);
    const wrapped = new Error(`Offline queue: cannot ${action} ${file}: ${detail}`, { cause: err });
    if (err && err.code) {
        wrapped.code = err.code;
    }
    wrapped.path = file;
    Object.defineProperty(wrapped, WRAPPED, { value: true });
    return wrapped;
}

async function closeQuiet(fh) {
    if (fh) {
        try {
            await fh.close();
        } catch (_) {
            // nothing useful to do: the descriptor is gone either way
        }
    }
}

/** Write all of buf at position (null = current position, i.e. append mode). Handles short writes. */
async function writeFully(fh, buf, position) {
    let done = 0;
    while (done < buf.length) {
        const pos = position === null ? null : position + done;
        const { bytesWritten } = await fh.write(buf, done, buf.length - done, pos);
        if (!(bytesWritten > 0)) {
            throw new Error(`short write (${done} of ${buf.length} bytes)`);
        }
        done += bytesWritten;
    }
}

/** Read up to len bytes at position into buf; returns the bytes read (less than len only at end of file). */
async function readFully(fh, buf, len, position) {
    let done = 0;
    while (done < len) {
        const { bytesRead } = await fh.read(buf, done, len - done, position + done);
        if (bytesRead === 0) {
            break;
        }
        done += bytesRead;
    }
    return done;
}

async function unlinkIfExists(file) {
    try {
        await fsp.unlink(file);
    } catch (err) {
        if (!err || err.code !== 'ENOENT') {
            throw fsError(err, 'delete', file);
        }
    }
}

async function renameWithRetry(from, to) {
    for (let attempt = 0; ; attempt++) {
        try {
            await fsp.rename(from, to);
            return;
        } catch (err) {
            if (attempt >= 3 || !err || !RENAME_RETRY_CODES.has(err.code)) {
                throw fsError(err, 'rename', `${from} -> ${to}`);
            }
            await new Promise((resolve) => setTimeout(resolve, 20 * (attempt + 1)));
        }
    }
}

/**
 * fsync a directory so a created/renamed/deleted entry survives a power loss. Best effort on platforms
 * that do not allow opening or syncing directories (Windows reports EISDIR/EPERM).
 */
async function syncDir(dir) {
    let fh;
    try {
        fh = await fsp.open(dir, 'r');
        await fh.sync();
    } catch (err) {
        if (!err || !DIR_SYNC_UNSUPPORTED.has(err.code)) {
            throw fsError(err, 'fsync directory', dir);
        }
    } finally {
        await closeQuiet(fh);
    }
}

/**
 * Durable segmented FIFO of JSON records (store-and-forward). See the file header for the format.
 */
class DiskQueue {
    /**
     * @param {object} options
     * @param {string} options.dir directory holding the queue files (created by open())
     * @param {number} [options.maxBytes=0] limit of pending bytes (0 = unlimited)
     * @param {number} [options.maxMessages=0] limit of pending records (0 = unlimited)
     * @param {'drop-oldest'|'drop-newest'} [options.overflow='drop-oldest'] what to do when a limit is hit
     * @param {number} [options.segmentMaxBytes=4194304] roll to a new segment file beyond this size
     * @param {boolean} [options.fsync=true] fsync appends and cursor updates before resolving
     * @param {number} [options.deadLetterMaxBytes=52428800] rotate dead-letter.jsonl beyond this size (0 = never)
     * @param {number} [options.peekMaxBytes=8388608] byte budget of one peek() (at least one record is returned)
     * @param {{warn: Function, error: Function, debug: Function}} [options.logger]
     */
    constructor(options) {
        let o;
        let optionsError = null;
        try {
            o = normalizeQueueOptions(options);
        } catch (err) {
            o = normalizeQueueOptions({});
            optionsError = new Error(`Offline queue: invalid options: ${err && err.message}`);
        }
        this._dir = o.dir ? path.resolve(o.dir) : '';
        this._configError = optionsError ||
            (o.dir ? null : new Error('Offline queue: no queue directory configured'));
        this._maxBytes = o.maxBytes;
        this._maxMessages = o.maxMessages;
        this._overflow = o.overflow;
        this._segmentMaxBytes = o.segmentMaxBytes;
        this._fsync = o.fsync;
        this._fileSyncUnsupported = false;
        this._deadLetterMaxBytes = o.deadLetterMaxBytes;
        this._peekMaxBytes = o.peekMaxBytes;
        this._readChunk = o.readChunkBytes;
        this._logger = o.logger;

        // lifecycle
        this._openPromise = null;
        this._openError = null;
        this._ready = false;
        this._closed = false;
        this._closePromise = null;

        // single operation chain; never rejects (see _run)
        this._chain = Promise.resolve();
        // append batch that new enqueues may still join (group commit); null once another op is queued
        this._openBatch = null;

        // what is on disk
        this._segments = [];            // ascending sequence numbers of the segment files
        this._segSizes = new Map();     // seq -> committed size in bytes (complete lines only)
        this._nextSeq = 1;
        this._cursor = null;            // { seg, off } of the oldest pending record; null without segments
        this._count = 0;                // pending records on disk
        this._bytes = 0;                // pending bytes on disk
        this._unwrittenCount = 0;       // records accepted by enqueue*() but not yet written
        this._unwrittenBytes = 0;
        this._writer = null;            // { seg, fh } open handle on the newest segment
        this._forceRoll = false;        // newest segment must not be appended to (failed rollback)

        // read side: one open handle + offset, plus lines already read but not yet acked, so peeks do
        // not re-read the segment from the start
        this._reader = null;            // { seg, fh, off, parts: Buffer[], partsLen }
        this._ahead = [];               // [{ raw, pos: {seg,end}, len, torn? }] from the cursor onwards
        this._aheadBytes = 0;

        // statistics
        this._droppedTotal = 0;
        this._deadLettered = 0;
        this._deadLetterSize = null;
        this._writeOps = 0;             // physical segment writes (lets tests observe group commit)
        this._readOps = 0;              // physical segment reads by the peek reader
    }

    /** Absolute queue directory. */
    get dir() {
        return this._dir;
    }

    /** Pending records, including enqueues requested but not yet written. */
    get size() {
        return this._count + this._unwrittenCount;
    }

    /** Pending bytes (JSON line sizes), including enqueues requested but not yet written. */
    get bytes() {
        return this._bytes + this._unwrittenBytes;
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
            bytes: this.bytes,
            segments: this._segments.length,
            droppedTotal: this._droppedTotal,
            deadLettered: this._deadLettered
        };
        const ts = this._headTs();
        if (ts !== undefined) {
            s.oldestTs = ts;
        }
        return s;
    }

    /**
     * Create the directory and recover the queue state (torn writes, stale/corrupt cursor). Idempotent;
     * after a failure it may be called again.
     * @returns {Promise<void>}
     */
    open() {
        if (this._closed) {
            return handled(Promise.reject(new Error('queue closed')));
        }
        if (!this._openPromise) {
            const p = this._run(() => this._doOpen(), false);
            this._openPromise = p;
            p.then(() => {
                this._openError = null;
            }, (err) => {
                this._openError = err;
                if (this._openPromise === p) {
                    this._openPromise = null;   // allow another attempt
                }
            });
        }
        return this._openPromise;
    }

    /**
     * Append one record.
     * @param {*} record JSON-serialisable value (snapshotted at call time)
     * @returns {Promise<{dropped:number}>} dropped = older records removed by the drop-oldest policy
     */
    enqueue(record) {
        return this.enqueueMany([record]);
    }

    /**
     * Append records atomically (all or nothing) and in order. Resolves once they are on disk.
     * @param {Array<*>} records
     * @returns {Promise<{dropped:number}>}
     */
    enqueueMany(records) {
        try {
            return this._enqueueMany(records);
        } catch (err) {
            return handled(Promise.reject(err)); // e.g. a hostile array-like whose getters throw
        }
    }

    _enqueueMany(records) {
        const notUsable = this._usableError();
        if (notUsable) {
            return handled(Promise.reject(notUsable));
        }
        if (!Array.isArray(records)) {
            return handled(Promise.reject(new TypeError('enqueueMany() expects an array of records')));
        }
        if (records.length === 0) {
            return Promise.resolve({ dropped: 0 });
        }
        const lines = new Array(records.length);
        let bytes = 0;
        try {
            for (let i = 0; i < records.length; i++) {
                lines[i] = Buffer.from(serializeRecord(records[i], i) + '\n', 'utf8');
                bytes += lines[i].length;
            }
        } catch (err) {
            return handled(Promise.reject(err));
        }
        // counted synchronously so callers (status, live/backlog decisions) see the record immediately
        this._unwrittenCount += lines.length;
        this._unwrittenBytes += bytes;
        return handled(new Promise((resolve, reject) => {
            let batch = this._openBatch;
            if (!batch) {
                batch = { items: [] };
                this._run(() => this._appendOp(batch), true).catch(noop);
                this._openBatch = batch;
            }
            batch.items.push({ lines, bytes, dropped: 0, resolve, reject });
        }));
    }

    /**
     * Read up to `max` records from the head without removing them.
     * @param {number} [max=100]
     * @returns {Promise<Array<{record:*, pos:{seg:number,end:number}} | {corrupt:true, raw:string, pos:{seg:number,end:number}}>>}
     */
    peek(max = 100) {
        const notUsable = this._usableError();
        if (notUsable) {
            return handled(Promise.reject(notUsable));
        }
        const limit = intOption(max, 100, 0, 1000000); // never throws
        return handled(this._run(() => this._peekOp(limit), false));
    }

    /**
     * Remove peeked entries: the cursor moves to the highest position given (records before it are
     * removed too). Idempotent; positions at or before the cursor are ignored.
     * @param {Array<{pos:{seg:number,end:number}}>} entries
     * @returns {Promise<number>} number of records removed
     */
    ack(entries) {
        const notUsable = this._usableError();
        if (notUsable) {
            return handled(Promise.reject(notUsable));
        }
        let target;
        try {
            target = maxPosition(entries); // snapshot the positions now: callers may reuse the array
        } catch (err) {
            return handled(Promise.reject(err)); // e.g. a getter that throws
        }
        if (!target) {
            return Promise.resolve(0);
        }
        return handled(this._run(() => this._ackOp(target), false));
    }

    /**
     * Append an undeliverable entry to dead-letter.jsonl (it is not removed from the queue: ack it).
     * @param {object} entry peeked entry
     * @param {Error|string} err reason
     * @returns {Promise<void>}
     */
    deadLetter(entry, err) {
        const notUsable = this._usableError();
        if (notUsable) {
            return handled(Promise.reject(notUsable));
        }
        let line;
        try {
            line = Buffer.from(buildDeadLetter(entry, err).json + '\n', 'utf8');
        } catch (e) {
            return handled(Promise.reject(e));
        }
        return handled(this._run(() => this._deadLetterOp(line), false));
    }

    /**
     * Drop every pending record (enqueues requested after this call are kept).
     * @returns {Promise<number>} number of records dropped
     */
    clear() {
        const notUsable = this._usableError();
        if (notUsable) {
            return handled(Promise.reject(notUsable));
        }
        return handled(this._run(async () => {
            this._assertReady();
            const n = this._count;
            await this._resetEmpty(true);
            return n;
        }, false));
    }

    /**
     * Finish every operation requested so far, then release file handles. Idempotent, never rejects.
     * Afterwards every method rejects with Error('queue closed').
     * @returns {Promise<void>}
     */
    close() {
        if (!this._closePromise) {
            this._closed = true;
            this._openBatch = null;
            this._closePromise = this._chain
                .then(async () => {
                    this._ready = false;
                    await this._resetReader();
                    await this._closeWriter();
                })
                .then(noop, (err) => this._log('debug', `error while closing: ${err && err.message}`));
        }
        return this._closePromise;
    }

    // ---------------------------------------------------------------- internals: lifecycle and chain

    _usableError() {
        if (this._closed) {
            return new Error('queue closed');
        }
        if (!this._ready && !this._openPromise) {
            return this._notOpenError();
        }
        return null;
    }

    _notOpenError() {
        const why = this._openError ? `: ${this._openError.message}` : ' (call open() first)';
        return new Error(`Offline queue ${this._dir} is not open${why}`);
    }

    _assertReady() {
        if (!this._ready) {
            throw this._notOpenError();
        }
    }

    /** Queue fn on the operation chain. The chain itself swallows failures; the caller gets them. */
    _run(fn, isAppend) {
        if (!isAppend) {
            // later enqueues must not jump ahead of this operation
            this._openBatch = null;
        }
        const p = this._chain.then(fn);
        this._chain = p.then(noop, noop);
        return p;
    }

    _log(level, message) {
        try {
            const logger = this._logger;
            if (logger && typeof logger[level] === 'function') {
                logger[level](`Offline queue ${this._dir}: ${message}`);
            }
        } catch (_) {
            // a logger must never break the queue
        }
    }

    _segPath(seg) {
        return path.join(this._dir, `seg-${String(seg).padStart(16, '0')}.jsonl`);
    }

    _cursorPath() {
        return path.join(this._dir, CURSOR_FILE);
    }

    _newestSeg() {
        return this._segments.length ? this._segments[this._segments.length - 1] : null;
    }

    _headTs() {
        try {
            const head = this._ahead[0];
            if (!head || head.torn) {
                return undefined;
            }
            if (head.ts === undefined) {
                const rec = JSON.parse(head.raw);
                head.ts = rec !== null && typeof rec === 'object' && Number.isFinite(rec.ts) ? rec.ts : null;
            }
            return head.ts === null ? undefined : head.ts;
        } catch (_) {
            if (this._ahead[0]) {
                this._ahead[0].ts = null;
            }
            return undefined;
        }
    }

    // ---------------------------------------------------------------- internals: open / recovery

    async _doOpen() {
        if (this._configError) {
            throw this._configError;
        }
        try {
            await this._recover();
        } catch (err) {
            await this._resetReader();
            await this._closeWriter();
            this._segments = [];
            this._segSizes = new Map();
            this._cursor = null;
            this._count = 0;
            this._bytes = 0;
            throw err;
        }
        this._ready = true;
        if (this._count === 0) {
            // nothing pending: normalise to a clean directory (deletes leftover empty/consumed files)
            if (this._segments.length) {
                await this._resetEmpty(false);
            }
            return;
        }
        try {
            await this._readMore(); // prime the reader so stats().oldestTs is known
        } catch (err) {
            this._log('warn', err.message);
        }
    }

    async _recover() {
        const dir = this._dir;
        try {
            await fsp.mkdir(dir, { recursive: true });
            await fsp.access(dir, fs.constants.R_OK | fs.constants.W_OK);
        } catch (err) {
            throw fsError(err, 'use directory', dir);
        }
        // a cursor update that never got renamed into place: the old cursor is still valid
        await unlinkIfExists(path.join(dir, CURSOR_TMP_FILE)).catch((err) => this._log('warn', err.message));

        let names;
        try {
            names = await fsp.readdir(dir);
        } catch (err) {
            throw fsError(err, 'list directory', dir);
        }
        const segs = [];
        for (const name of names) {
            const m = SEGMENT_RE.exec(name);
            if (m && Number.isSafeInteger(Number(m[1]))) {
                segs.push(Number(m[1]));
            }
        }
        segs.sort((a, b) => a - b);
        const sizes = new Map();
        for (const seg of segs.slice()) {
            const file = this._segPath(seg);
            let st;
            try {
                st = await fsp.stat(file);
            } catch (err) {
                throw fsError(err, 'stat', file);
            }
            if (!st.isFile()) {
                this._log('warn', `ignoring ${file}: not a regular file`);
                segs.splice(segs.indexOf(seg), 1);
                continue;
            }
            sizes.set(seg, st.size);
        }

        const stored = await this._readCursorFile();
        let cursor = null;
        let maxSeq = segs.length ? segs[segs.length - 1] : 0;
        if (stored.cursor) {
            maxSeq = Math.max(maxSeq, stored.cursor.seg);
        }
        if (segs.length) {
            const newest = segs[segs.length - 1];
            if (stored.cursor && stored.cursor.seg > newest) {
                // we never write a cursor beyond the newest segment: treat as corrupt (at-least-once)
                this._log('warn', `cursor ${JSON.stringify(stored.cursor)} points past the newest segment; ` +
                    'replaying the queue from the oldest segment (messages may be sent twice)');
                cursor = { seg: segs[0], off: 0 };
            } else if (stored.cursor) {
                cursor = { seg: stored.cursor.seg, off: stored.cursor.off };
                if (!sizes.has(cursor.seg)) {
                    // its segment was fully consumed and deleted: continue with the next one
                    cursor = { seg: segs.find((s) => s > cursor.seg), off: 0 };
                }
            } else {
                if (stored.corrupt) {
                    this._log('warn', `cursor file ${this._cursorPath()} is unreadable (${stored.corrupt}); ` +
                        'replaying the queue from the oldest segment (messages may be sent twice)');
                }
                cursor = { seg: segs[0], off: 0 };
            }
            // segments before the cursor were fully consumed (a crash interrupted their deletion)
            while (segs.length > 1 && segs[0] < cursor.seg) {
                const old = segs.shift();
                sizes.delete(old);
                await unlinkIfExists(this._segPath(old)).catch((err) => this._log('warn', err.message));
            }
        } else if (stored.cursor || stored.corrupt) {
            // a drained queue whose cleanup was interrupted
            await unlinkIfExists(this._cursorPath()).catch((err) => this._log('warn', err.message));
        }

        this._segments = segs;
        this._segSizes = sizes;
        this._nextSeq = maxSeq + 1;
        this._cursor = null;
        this._count = 0;
        this._bytes = 0;
        if (!segs.length) {
            return;
        }
        await this._truncateTornTail(segs[segs.length - 1]);
        const size = sizes.get(cursor.seg);
        if (cursor.off > size) {
            this._log('warn', `cursor offset ${cursor.off} is beyond the end of ${this._segPath(cursor.seg)} ` +
                `(${size} bytes); continuing from its end`);
            cursor.off = size;
        }
        if (cursor.off > 0 && !(await this._isLineStart(cursor.seg, cursor.off))) {
            this._log('warn', `cursor offset ${cursor.off} is not at a record boundary in ` +
                `${this._segPath(cursor.seg)}; replaying that segment (messages may be sent twice)`);
            cursor.off = 0;
        }
        // a cursor at the very end of an older segment (its deletion was interrupted): move to the next
        // segment and delete the consumed ones. Persisting is not needed: a cursor naming a missing segment
        // already means "start of the next one".
        let i = segs.indexOf(cursor.seg);
        while (i < segs.length - 1 && cursor.off >= sizes.get(segs[i])) {
            i++;
            cursor = { seg: segs[i], off: 0 };
        }
        while (segs.length > 1 && segs[0] < cursor.seg) {
            const old = segs.shift();
            sizes.delete(old);
            await unlinkIfExists(this._segPath(old)).catch((err) => this._log('warn', err.message));
        }
        this._cursor = cursor;
        const counted = await this._countFrom(cursor);
        this._count = counted.count;
        this._bytes = counted.bytes;
    }

    /** @returns {Promise<{cursor?:{seg:number,off:number}, missing?:true, corrupt?:string}>} */
    async _readCursorFile() {
        const file = this._cursorPath();
        let text;
        try {
            text = await fsp.readFile(file, 'utf8');
        } catch (err) {
            if (err && err.code === 'ENOENT') {
                return { missing: true };
            }
            throw fsError(err, 'read', file);
        }
        try {
            const c = JSON.parse(text);
            if (c !== null && typeof c === 'object' && Number.isSafeInteger(c.seg) && c.seg >= 0 &&
                Number.isSafeInteger(c.off) && c.off >= 0) {
                return { cursor: { seg: c.seg, off: c.off } };
            }
            return { corrupt: 'invalid content' };
        } catch (err) {
            return { corrupt: err.message };
        }
    }

    /** Remove an incomplete last line (a crash during an append) from the newest segment. */
    async _truncateTornTail(seg) {
        const size = this._segSizes.get(seg);
        if (!size) {
            return;
        }
        const file = this._segPath(seg);
        let fh;
        try {
            fh = await fsp.open(file, 'r+');
            let end = size;
            let lastNewline = -1;
            const buf = Buffer.allocUnsafe(Math.min(this._readChunk, size));
            while (end > 0 && lastNewline < 0) {
                const start = Math.max(0, end - buf.length);
                const n = await readFully(fh, buf, end - start, start);
                const i = buf.subarray(0, n).lastIndexOf(NEWLINE);
                if (i >= 0) {
                    lastNewline = start + i;
                }
                end = start;
            }
            const keep = lastNewline + 1;
            if (keep < size) {
                this._log('warn', `removed ${size - keep} bytes of an incomplete record at the end of ${file} ` +
                    '(interrupted write before a crash; that message was never acknowledged)');
                await fh.truncate(keep);
                await this._syncData(fh, file);
                this._segSizes.set(seg, keep);
            }
        } catch (err) {
            throw fsError(err, 'repair', file);
        } finally {
            await closeQuiet(fh);
        }
    }

    async _isLineStart(seg, off) {
        const file = this._segPath(seg);
        let fh;
        try {
            fh = await fsp.open(file, 'r');
            const buf = Buffer.alloc(1);
            const n = await readFully(fh, buf, 1, off - 1);
            return n === 1 && buf[0] === NEWLINE;
        } catch (err) {
            throw fsError(err, 'read', file);
        } finally {
            await closeQuiet(fh);
        }
    }

    /** Count pending records from the cursor, streaming (never loads a whole segment). */
    async _countFrom(cursor) {
        let count = 0;
        let bytes = 0;
        const newest = this._newestSeg();
        const buf = Buffer.allocUnsafe(this._readChunk);
        for (const seg of this._segments) {
            if (seg < cursor.seg) {
                continue;
            }
            const start = seg === cursor.seg ? cursor.off : 0;
            const size = this._segSizes.get(seg);
            if (size <= start) {
                continue;
            }
            const file = this._segPath(seg);
            let fh;
            try {
                fh = await fsp.open(file, 'r');
                let pos = start;
                let last = NEWLINE;
                while (pos < size) {
                    const n = await readFully(fh, buf, Math.min(buf.length, size - pos), pos);
                    if (n === 0) {
                        throw new Error(`unexpected end of file at byte ${pos} (expected ${size})`);
                    }
                    const view = buf.subarray(0, n);
                    for (let i = view.indexOf(NEWLINE); i !== -1; i = view.indexOf(NEWLINE, i + 1)) {
                        count++;
                    }
                    last = view[n - 1];
                    pos += n;
                }
                if (last !== NEWLINE && seg !== newest) {
                    count++; // incomplete line in an older segment: surfaced as one corrupt entry
                }
                bytes += size - start;
            } catch (err) {
                throw fsError(err, 'read', file);
            } finally {
                await closeQuiet(fh);
            }
        }
        return { count, bytes };
    }

    // ---------------------------------------------------------------- internals: append

    async _appendOp(batch) {
        if (this._openBatch === batch) {
            this._openBatch = null; // enqueues from now on go into the next write
        }
        const items = batch.items;
        let totalCount = 0;
        let totalBytes = 0;
        for (const item of items) {
            totalCount += item.lines.length;
            totalBytes += item.bytes;
        }
        const release = () => {
            this._unwrittenCount -= totalCount;
            this._unwrittenBytes -= totalBytes;
        };
        let plan;
        try {
            this._assertReady();
            plan = await this._planAppend(items);
        } catch (err) {
            release();
            for (const item of items) {
                item.reject(err);
            }
            return;
        }
        let writeError = null;
        if (plan.lines.length) {
            try {
                await this._writeLines(plan.lines);
            } catch (err) {
                writeError = err;
            }
        }
        // from here on synchronous until the callers are settled, so size never double counts
        release();
        for (const r of plan.rejected) {
            r.item.reject(r.error);
        }
        if (writeError) {
            for (const item of plan.accepted) {
                item.reject(writeError);
            }
            return;
        }
        this._count += plan.lines.length;
        this._bytes += plan.lineBytes;
        if (plan.dropped > 0) {
            if (plan.diskDrop > 0) {
                this._consumeAhead(plan.diskDrop);
            }
            this._droppedTotal += plan.dropped;
            this._log('warn', `queue full (limit ${limitText(this._maxMessages, this._maxBytes)}): dropped ` +
                `${plan.dropped} oldest message(s) to make room`);
        }
        for (const item of plan.accepted) {
            item.resolve({ dropped: item.dropped });
        }
        if (plan.diskDrop > 0) {
            await this._cursorMoved();
        }
    }

    /**
     * Decide, item by item and in call order, what gets written and what gets dropped/rejected, exactly
     * as if the coalesced enqueue calls had run one after another.
     */
    async _planAppend(items) {
        const maxM = this._maxMessages;
        const maxB = this._maxBytes;
        const accepted = [];
        const rejected = [];
        const group = [];          // lines accepted in this batch: { buf, item }
        let groupHead = 0;         // group lines before this index were dropped (oldest first)
        let groupBytes = 0;
        let diskDrop = 0;          // pending on-disk records to drop from the head
        let diskDropBytes = 0;
        let dropped = 0;
        for (const item of items) {
            const n = item.lines.length;
            const b = item.bytes;
            if ((maxM && n > maxM) || (maxB && b > maxB)) {
                rejected.push({ item, error: queueFullError(`Offline queue ${this._dir}: cannot queue ${n} ` +
                    `message(s) of ${b} bytes: larger than the whole queue limit (${limitText(maxM, maxB)})`) });
                continue;
            }
            const fits = () => {
                const count = (this._count - diskDrop) + (group.length - groupHead) + n;
                const bytes = (this._bytes - diskDropBytes) + groupBytes + b;
                return (!maxM || count <= maxM) && (!maxB || bytes <= maxB);
            };
            if (!fits()) {
                if (this._overflow === 'drop-newest') {
                    const count = (this._count - diskDrop) + (group.length - groupHead);
                    rejected.push({ item, error: queueFullError(`Offline queue ${this._dir} is full (${count} ` +
                        `messages queued, limit ${limitText(maxM, maxB)}); message rejected (overflow policy ` +
                        'drop-newest)') });
                    continue;
                }
                while (!fits()) {
                    if (this._count - diskDrop > 0) {
                        const have = await this._loadAhead(diskDrop + 1);
                        if (have <= diskDrop) {
                            // the reader reached the end: the in-memory count was off; trust the file
                            this._log('warn', `pending count was ${this._count}, found ${have}; corrected`);
                            this._count = have;
                            this._bytes = this._aheadBytes;
                            continue;
                        }
                        diskDropBytes += this._ahead[diskDrop].len;
                        diskDrop++;
                    } else if (groupHead < group.length) {
                        groupBytes -= group[groupHead].buf.length;
                        groupHead++;
                    } else {
                        break; // nothing left to drop (only reachable with inconsistent byte counts)
                    }
                    item.dropped++;
                    dropped++;
                }
            }
            for (const buf of item.lines) {
                group.push({ buf, item });
            }
            groupBytes += b;
            accepted.push(item);
        }
        const lines = new Array(group.length - groupHead);
        for (let i = groupHead; i < group.length; i++) {
            lines[i - groupHead] = group[i].buf;
        }
        return { accepted, rejected, lines, lineBytes: groupBytes, diskDrop, dropped };
    }

    /**
     * Write lines to the newest segment(s), rolling over when a segment would exceed segmentMaxBytes.
     * All or nothing: on failure everything written by this call is truncated/removed again.
     */
    async _writeLines(lines) {
        const chunks = [];
        const reuse = this._segments.length > 0 && !this._forceRoll;
        let seg = reuse ? this._newestSeg() : NEW_SEGMENT;
        let size = reuse ? this._segSizes.get(seg) : 0;
        let chunk = null;
        for (const line of lines) {
            if (size > 0 && size + line.length > this._segmentMaxBytes) {
                seg = NEW_SEGMENT;
                size = 0;
                chunk = null;
            }
            if (!chunk) {
                chunk = { seg, start: size, bufs: [], len: 0 };
                chunks.push(chunk);
            }
            chunk.bufs.push(line);
            chunk.len += line.length;
            size += line.length;
        }

        const touched = []; // { seg, fh, start, len, created }
        let file = this._dir;
        try {
            for (const c of chunks) {
                let t;
                if (c.seg === NEW_SEGMENT) {
                    const created = await this._createSegment();
                    t = { seg: created.seg, fh: created.fh, start: 0, len: c.len, created: true };
                } else {
                    t = { seg: c.seg, fh: await this._writerHandle(c.seg), start: c.start, len: c.len, created: false };
                }
                touched.push(t);
                file = this._segPath(t.seg);
                await writeFully(t.fh, c.bufs.length === 1 ? c.bufs[0] : Buffer.concat(c.bufs, c.len), t.start);
                this._writeOps++;
                await this._syncData(t.fh, file);
            }
            if (this._fsync && touched.some((t) => t.created)) {
                file = this._dir;
                await syncDir(this._dir); // make the new file names durable too
            }
        } catch (err) {
            await this._rollbackWrite(touched);
            throw fsError(err, 'write', file);
        }

        for (const t of touched) {
            if (t.created) {
                this._segments.push(t.seg);
            }
            this._segSizes.set(t.seg, t.start + t.len);
        }
        if (!this._cursor) {
            this._cursor = { seg: this._segments[0], off: 0 };
        }
        const last = touched[touched.length - 1];
        for (const t of touched) {
            if (t.created && t !== last) {
                await closeQuiet(t.fh);
            }
        }
        if (last.created) {
            await this._closeWriter();
            this._writer = { seg: last.seg, fh: last.fh };
            this._forceRoll = false;
        }
    }

    async _rollbackWrite(touched) {
        for (const t of touched.slice().reverse()) {
            if (t.created) {
                await closeQuiet(t.fh);
                await unlinkIfExists(this._segPath(t.seg)).catch((err) => this._log('warn', err.message));
                continue;
            }
            try {
                await t.fh.truncate(t.start);
                await this._syncData(t.fh, this._segPath(t.seg));
            } catch (err) {
                // the segment may now hold a partial record after its committed size: never append to it
                // again (it is read only up to its committed size in this process)
                this._log('warn', `could not undo a failed write in ${this._segPath(t.seg)}: ${err.message}`);
                this._forceRoll = true;
                await this._closeWriter();
            }
        }
    }

    async _createSegment() {
        for (let attempt = 0; attempt < 1000; attempt++) {
            const seg = this._nextSeq++;
            const file = this._segPath(seg);
            try {
                // 'wx': never overwrite a file we do not know about
                return { seg, fh: await fsp.open(file, 'wx') };
            } catch (err) {
                if (!err || err.code !== 'EEXIST') {
                    throw fsError(err, 'create', file);
                }
            }
        }
        throw new Error(`Offline queue: cannot allocate a new segment file in ${this._dir}`);
    }

    async _writerHandle(seg) {
        if (this._writer && this._writer.seg === seg) {
            return this._writer.fh;
        }
        await this._closeWriter();
        const file = this._segPath(seg);
        try {
            this._writer = { seg, fh: await fsp.open(file, 'r+') };
        } catch (err) {
            throw fsError(err, 'open', file);
        }
        return this._writer.fh;
    }

    /**
     * fdatasync when fsync is enabled. A file system that cannot sync at all (EINVAL/ENOTSUP on some
     * FUSE, 9p or SMB mounts) would otherwise make every append fail: warn once and carry on without it.
     */
    async _syncData(fh, file) {
        if (!this._fsync || this._fileSyncUnsupported) {
            return;
        }
        try {
            await fh.datasync();
        } catch (err) {
            if (!err || !FILE_SYNC_UNSUPPORTED.has(err.code)) {
                throw fsError(err, 'fsync', file);
            }
            this._fileSyncUnsupported = true;
            this._log('warn', `the file system does not support fsync (${err.code}); continuing without it, so ` +
                'queued messages written just before a power loss may be lost');
        }
    }

    async _closeWriter() {
        const w = this._writer;
        this._writer = null;
        if (w) {
            await closeQuiet(w.fh);
        }
    }

    // ---------------------------------------------------------------- internals: read side

    async _resetReader() {
        const rd = this._reader;
        this._reader = null;
        this._ahead = [];
        this._aheadBytes = 0;
        if (rd) {
            await closeQuiet(rd.fh);
        }
    }

    async _ensureReader() {
        if (this._reader) {
            return this._reader;
        }
        if (!this._cursor || !this._segments.length) {
            return null;
        }
        // invariant: without a reader nothing is buffered, so reading starts at the cursor
        this._ahead = [];
        this._aheadBytes = 0;
        const seg = this._cursor.seg;
        const file = this._segPath(seg);
        try {
            const fh = await fsp.open(file, 'r');
            this._reader = { seg, fh, off: this._cursor.off, parts: [], partsLen: 0 };
        } catch (err) {
            throw fsError(err, 'open', file);
        }
        return this._reader;
    }

    /**
     * Read the next chunk into this._ahead. Returns false when everything written so far has been read.
     */
    async _readMore() {
        const rd = await this._ensureReader();
        if (!rd) {
            return false;
        }
        const size = this._segSizes.get(rd.seg);
        if (rd.off < size) {
            const len = Math.min(this._readChunk, size - rd.off);
            const buf = Buffer.allocUnsafe(len);
            try {
                const n = await readFully(rd.fh, buf, len, rd.off);
                this._readOps++;
                if (n < len) {
                    throw new Error(`unexpected end of file at byte ${rd.off + n} (expected ${size})`);
                }
            } catch (err) {
                await this._resetReader();
                throw fsError(err, 'read', this._segPath(rd.seg));
            }
            this._splitLines(rd, buf);
            rd.off += len;
            return true;
        }
        const i = this._segments.indexOf(rd.seg);
        if (i < 0 || i === this._segments.length - 1) {
            return false; // caught up with the writer
        }
        if (rd.partsLen > 0) {
            // an older segment ending without '\n' can only come from damage: hand it out as corrupt so it
            // is dead-lettered instead of silently skipped (and so the cursor can pass it)
            const raw = Buffer.concat(rd.parts, rd.partsLen);
            this._ahead.push({ raw: raw.toString('utf8'), pos: { seg: rd.seg, end: size }, len: rd.partsLen, torn: true });
            this._aheadBytes += rd.partsLen;
            rd.parts = [];
            rd.partsLen = 0;
        }
        await closeQuiet(rd.fh);
        const next = this._segments[i + 1];
        const file = this._segPath(next);
        try {
            rd.fh = await fsp.open(file, 'r');
        } catch (err) {
            rd.fh = null;
            await this._resetReader();
            throw fsError(err, 'open', file);
        }
        rd.seg = next;
        rd.off = 0;
        return true;
    }

    /** Split a chunk read at rd.off into complete lines; keep the incomplete rest in rd.parts. */
    _splitLines(rd, buf) {
        let lineStart = rd.off - rd.partsLen; // file offset of the first byte of the pending line
        let start = 0;
        for (;;) {
            const i = buf.indexOf(NEWLINE, start);
            if (i === -1) {
                if (start < buf.length) {
                    rd.parts.push(buf.subarray(start));
                    rd.partsLen += buf.length - start;
                }
                return;
            }
            let line = buf.subarray(start, i);
            if (rd.partsLen > 0) {
                rd.parts.push(line);
                line = Buffer.concat(rd.parts, rd.partsLen + line.length);
                rd.parts = [];
                rd.partsLen = 0;
            }
            const len = line.length + 1;
            const end = lineStart + len;
            this._ahead.push({ raw: line.toString('utf8'), pos: { seg: rd.seg, end }, len });
            this._aheadBytes += len;
            lineStart = end;
            start = i + 1;
        }
    }

    /** Make sure at least k entries are buffered (if they exist). Returns how many are buffered. */
    async _loadAhead(k) {
        while (this._ahead.length < k) {
            if (!(await this._readMore())) {
                break;
            }
        }
        return this._ahead.length;
    }

    async _peekOp(max) {
        this._assertReady();
        if (max <= 0) {
            return [];
        }
        let caughtUp = false;
        while (this._ahead.length < max && this._aheadBytes < this._peekMaxBytes) {
            if (!(await this._readMore())) {
                caughtUp = true;
                break;
            }
        }
        const out = [];
        let bytes = 0;
        for (const e of this._ahead) {
            if (out.length >= max || (out.length > 0 && bytes + e.len > this._peekMaxBytes)) {
                break;
            }
            bytes += e.len;
            out.push(toEntry(e));
        }
        if (out.length === 0 && caughtUp && this._count !== 0) {
            // everything on disk has been read and nothing is pending: the count drifted (damaged files)
            this._log('warn', `pending count was ${this._count} but no records are left; corrected`);
            this._count = 0;
            this._bytes = 0;
        }
        return out;
    }

    // ---------------------------------------------------------------- internals: ack / cursor

    async _ackOp(target) {
        this._assertReady();
        if (!this._cursor || comparePos(target, { seg: this._cursor.seg, end: this._cursor.off }) <= 0) {
            return 0;
        }
        let removed = 0;
        for (;;) {
            let k = 0;
            while (k < this._ahead.length && comparePos(this._ahead[k].pos, target) <= 0) {
                k++;
            }
            if (k > 0) {
                this._consumeAhead(k);
                removed += k;
            }
            if (this._ahead.length > 0) {
                break; // the next buffered record is beyond the target
            }
            if (!(await this._readMore())) {
                break; // target is beyond the data (forged position): everything up to the end is consumed
            }
        }
        if (removed > 0) {
            await this._cursorMoved();
        }
        return removed;
    }

    /** Remove the first k buffered entries and move the in-memory cursor behind them. */
    _consumeAhead(k) {
        const removed = this._ahead.splice(0, k);
        let bytes = 0;
        for (const e of removed) {
            bytes += e.len;
        }
        const last = removed[removed.length - 1].pos;
        this._aheadBytes -= bytes;
        this._cursor = { seg: last.seg, off: last.end };
        this._count = Math.max(0, this._count - removed.length);
        this._bytes = Math.max(0, this._bytes - bytes);
    }

    /**
     * Persist a cursor that moved forward and delete fully consumed segments; when nothing is pending any
     * more, remove all files. Failures are logged, never thrown: the in-memory state already moved on and
     * the worst outcome of a stale cursor file is a re-delivery after a restart (at-least-once).
     */
    async _cursorMoved() {
        const segs = this._segments;
        if (!this._cursor || !segs.length) {
            return;
        }
        let i = segs.indexOf(this._cursor.seg);
        if (i < 0) {
            i = 0;
            this._cursor = { seg: segs[0], off: 0 };
        }
        while (i < segs.length - 1 && this._cursor.off >= this._segSizes.get(segs[i])) {
            i++;
            this._cursor = { seg: segs[i], off: 0 };
        }
        const newest = segs[segs.length - 1];
        if (this._cursor.seg === newest && this._cursor.off >= this._segSizes.get(newest)) {
            this._count = 0;
            this._bytes = 0;
            await this._resetEmpty(false);
            return;
        }
        try {
            await this._persistCursor(this._cursor);
        } catch (err) {
            this._log('error', `${err.message} (messages may be sent again after a restart)`);
        }
        await this._deleteSegmentsBefore(this._cursor.seg);
    }

    async _persistCursor(cursor) {
        const file = this._cursorPath();
        const tmp = path.join(this._dir, CURSOR_TMP_FILE);
        const data = Buffer.from(JSON.stringify({ seg: cursor.seg, off: cursor.off }) + '\n', 'utf8');
        let fh;
        try {
            fh = await fsp.open(tmp, 'w');
            await writeFully(fh, data, 0);
            await this._syncData(fh, tmp);
        } catch (err) {
            throw fsError(err, 'write', tmp);
        } finally {
            await closeQuiet(fh);
        }
        await renameWithRetry(tmp, file);
        if (this._fsync) {
            await syncDir(this._dir);
        }
    }

    async _deleteSegmentsBefore(seg) {
        while (this._segments.length > 1 && this._segments[0] < seg) {
            const old = this._segments.shift();
            this._segSizes.delete(old);
            if (this._reader && this._reader.seg === old) {
                await this._resetReader();
            }
            if (this._writer && this._writer.seg === old) {
                await this._closeWriter();
            }
            await unlinkIfExists(this._segPath(old)).catch((err) => {
                this._log('warn', `${err.message} (it will be removed on the next start)`);
            });
        }
    }

    /**
     * Forget every pending record and remove the segment files. The cursor is first moved to the end
     * and persisted so a crash half-way through the deletions cannot resurrect consumed records.
     * @param {boolean} strict throw the first failure (clear()) instead of logging it
     */
    async _resetEmpty(strict) {
        const segs = this._segments.slice();
        const errors = [];
        if (segs.length) {
            const newest = segs[segs.length - 1];
            try {
                await this._persistCursor({ seg: newest, off: this._segSizes.get(newest) });
            } catch (err) {
                errors.push(err);
            }
        }
        await this._resetReader();
        await this._closeWriter();
        this._segments = [];
        this._segSizes = new Map();
        this._cursor = null;
        this._count = 0;
        this._bytes = 0;
        this._forceRoll = false;
        let allDeleted = true;
        for (const seg of segs) {
            try {
                await unlinkIfExists(this._segPath(seg));
            } catch (err) {
                allDeleted = false;
                errors.push(err);
            }
        }
        if (allDeleted) {
            // keep the cursor file otherwise: it marks the leftover segment(s) as consumed
            try {
                await unlinkIfExists(this._cursorPath());
            } catch (err) {
                errors.push(err);
            }
        }
        if (this._fsync) {
            try {
                await syncDir(this._dir);
            } catch (err) {
                errors.push(err);
            }
        }
        if (errors.length) {
            if (strict) {
                throw errors[0];
            }
            this._log('warn', `cleanup of the drained queue was incomplete: ${errors[0].message}`);
        }
    }

    // ---------------------------------------------------------------- internals: dead letters

    async _deadLetterOp(line) {
        this._assertReady();
        const file = path.join(this._dir, DEAD_LETTER_FILE);
        if (this._deadLetterSize === null) {
            try {
                this._deadLetterSize = (await fsp.stat(file)).size;
            } catch (err) {
                if (!err || err.code !== 'ENOENT') {
                    throw fsError(err, 'stat', file);
                }
                this._deadLetterSize = 0;
            }
        }
        let newFile = this._deadLetterSize === 0;
        if (this._deadLetterMaxBytes > 0 && this._deadLetterSize > 0 &&
            this._deadLetterSize + line.length > this._deadLetterMaxBytes) {
            await renameWithRetry(file, path.join(this._dir, DEAD_LETTER_ROTATED_FILE));
            this._deadLetterSize = 0;
            newFile = true;
        }
        let fh;
        try {
            fh = await fsp.open(file, 'a');
            await writeFully(fh, line, null);
            await this._syncData(fh, file);
        } catch (err) {
            this._deadLetterSize = null; // unknown after a failed append: stat again next time
            throw fsError(err, 'write', file);
        } finally {
            await closeQuiet(fh);
        }
        this._deadLetterSize += line.length;
        this._deadLettered++;
        if (newFile && this._fsync) {
            await syncDir(this._dir);
        }
    }
}

/** Turn a buffered line into the entry handed to callers (fresh objects: callers may mutate them). */
function toEntry(e) {
    const pos = { seg: e.pos.seg, end: e.pos.end };
    if (!e.torn) {
        try {
            return { record: JSON.parse(e.raw), pos };
        } catch (_) {
            // fall through: corrupt line
        }
    }
    return { corrupt: true, raw: e.raw, pos };
}

module.exports = {
    DiskQueue,
    // shared with lib/memory-queue.js
    intOption,
    normalizeQueueOptions,
    serializeRecord,
    comparePos,
    maxPosition,
    limitText,
    queueFullError,
    buildDeadLetter,
    handled
};
