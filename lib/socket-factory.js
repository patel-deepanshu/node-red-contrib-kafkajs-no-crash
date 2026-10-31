'use strict';

/**
 * A kafkajs `socketFactory` equivalent to kafkajs' default
 * (src/network/socketFactory.js) plus one fix.
 *
 * kafkajs gives up on a connection attempt (connection timeout) by calling
 * `socket.end()` + `socket.unref()` only. A socket that is still connecting
 * (SYN_SENT towards a black-holed address) or whose peer never answers the
 * FIN stays open for minutes, so every reconnect attempt during such an
 * outage leaks a file descriptor. After `end()` we therefore `destroy()` the
 * socket if it has not closed by itself within a short grace period.
 */

const net = require('net');
const tls = require('tls');

const KEEP_ALIVE_DELAY = 60000; // same as kafkajs
const DESTROY_AFTER_END_MS = 2000;

/** Wrap socket.end() so that a socket that does not close is destroyed. */
function destroyAfterEnd(socket, delayMs) {
    const originalEnd = socket.end;
    if (typeof originalEnd !== 'function') return;
    let timer = null;
    const clear = () => {
        if (timer) {
            clearTimeout(timer);
            timer = null;
        }
    };
    socket.end = function endThenDestroy(...args) {
        try {
            return originalEnd.apply(this, args);
        } finally {
            try {
                if (!timer && !socket.destroyed) {
                    timer = setTimeout(() => {
                        timer = null;
                        try {
                            if (!socket.destroyed) socket.destroy();
                        } catch (_) {
                            // destroy() of a broken socket: nothing more we can do
                        }
                    }, delayMs);
                    // never keep Node-RED alive just to clean up a socket
                    if (typeof timer.unref === 'function') timer.unref();
                }
            } catch (_) {
                // never let the cleanup scheduling break kafkajs' disconnect()
            }
        }
    };
    socket.once('close', clear);
}

/**
 * Create a kafkajs socket factory.
 *
 * @param {{destroyAfterEndMs?: number}} [options] grace period after end() (default 2000 ms)
 * @returns {(args: {host: string, port: number, ssl: (boolean|object), onConnect: Function}) => net.Socket}
 */
function createSocketFactory(options) {
    const raw = options && options.destroyAfterEndMs;
    const delayMs = typeof raw === 'number' && Number.isFinite(raw) && raw >= 0 ? raw : DESTROY_AFTER_END_MS;
    return ({ host, port, ssl, onConnect }) => {
        // Same semantics as kafkajs: synchronous throws (e.g. invalid port) are
        // intentionally NOT caught here; kafkajs turns them into a
        // KafkaJSConnectionError('Failed to connect: ...').
        const socket = ssl
            ? tls.connect(Object.assign({ host, port }, !net.isIP(host) ? { servername: host } : {}, ssl), onConnect)
            : net.connect({ host, port }, onConnect);
        try {
            socket.setKeepAlive(true, KEEP_ALIVE_DELAY);
        } catch (_) {
            // keep-alive is an optimisation only
        }
        try {
            destroyAfterEnd(socket, delayMs);
        } catch (_) {
            // fall back to kafkajs' default behaviour
        }
        return socket;
    };
}

module.exports = { createSocketFactory, DESTROY_AFTER_END_MS };
