'use strict';

/**
 * Consistent, crash-proof node.status() updates.
 *
 * - `set()` and the named helpers apply immediately and skip exact repeats.
 * - `activity()` is for high-frequency updates (queue depth while flushing):
 *   the first one applies at once, later ones within `throttleMs` are merged and
 *   only the latest is shown when the (unref()'d) timer fires.
 * - After `dispose()` (called from the close handler) every call is a no-op, so
 *   a late callback can never overwrite the status of a closed node.
 */

const { truncate, toInt } = require('./util');

/** Longest status text shown; longer texts end with '…'. Nodes size their reasons by it. */
const MAX_TEXT = 48;

class StatusReporter {
    /**
     * @param {{status: Function}} node the Node-RED node
     * @param {{throttleMs?: number}} [options]
     */
    constructor(node, options) {
        const o = options && typeof options === 'object' ? options : {};
        this._node = node;
        this._throttleMs = toInt(o.throttleMs, 1000, { min: 0 });
        this._lastKey = null;
        this._lastEmit = 0;
        this._pending = null;
        this._timer = null;
        this._disposed = false;
    }

    /** True after dispose(). */
    get disposed() {
        return this._disposed;
    }

    /**
     * Show a status immediately (cancels a pending throttled activity update,
     * which would otherwise overwrite this newer state).
     *
     * @param {string} fill red|green|yellow|blue|grey
     * @param {string} shape ring|dot
     * @param {string} [text]
     */
    set(fill, shape, text) {
        if (this._disposed) return;
        this._cancelPending();
        this._apply({ fill, shape, text: text == null ? '' : truncate(text, MAX_TEXT) });
    }

    /**
     * Throttled status update: at most one write per `throttleMs`; the latest
     * call wins.
     *
     * @param {string} fill
     * @param {string} shape
     * @param {string} [text]
     */
    activity(fill, shape, text) {
        if (this._disposed) return;
        try {
            const status = { fill, shape, text: text == null ? '' : truncate(text, MAX_TEXT) };
            const wait = this._lastEmit + this._throttleMs - Date.now();
            if (wait <= 0 && !this._timer) {
                this._apply(status);
                return;
            }
            this._pending = status;
            if (!this._timer) {
                this._timer = setTimeout(() => this._flush(), Math.max(0, wait));
                if (typeof this._timer.unref === 'function') this._timer.unref();
            }
        } catch (_) {
            // a status update must never break the caller
        }
    }

    /** Yellow ring. */
    connecting(text = 'connecting') {
        this.set('yellow', 'ring', text);
    }

    /** Green dot. */
    connected(text = 'connected') {
        this.set('green', 'dot', text);
    }

    /** Yellow ring (e.g. waiting to reconnect). */
    waiting(text) {
        this.set('yellow', 'ring', text);
    }

    /** Red ring. */
    error(text) {
        this.set('red', 'ring', text);
    }

    /** Grey ring. */
    idle(text) {
        this.set('grey', 'ring', text);
    }

    /** Remove the status. */
    clear() {
        if (this._disposed) return;
        this._cancelPending();
        this._apply({});
    }

    /** Cancel timers; every later call is a no-op. Safe to call repeatedly. */
    dispose() {
        this._cancelPending();
        this._disposed = true;
    }

    _flush() {
        this._timer = null;
        const status = this._pending;
        this._pending = null;
        if (status && !this._disposed) this._apply(status);
    }

    _cancelPending() {
        try {
            if (this._timer) clearTimeout(this._timer);
        } catch (_) {
            // ignore
        }
        this._timer = null;
        this._pending = null;
    }

    _apply(status) {
        try {
            const key = JSON.stringify(status);
            if (key === this._lastKey) return;
            this._lastKey = key;
            this._lastEmit = Date.now();
            this._node.status(status);
        } catch (_) {
            // node.status can throw if the node is half torn down: never propagate
        }
    }
}

module.exports = { StatusReporter, MAX_TEXT };
