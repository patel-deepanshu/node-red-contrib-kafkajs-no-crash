'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const tls = require('node:tls');
const { execFileSync } = require('node:child_process');
const defaultKafkajsSocketFactory = require('kafkajs/src/network/socketFactory');
const { createSocketFactory, DESTROY_AFTER_END_MS } = require('../../lib/socket-factory');

/** A server that accepts connections but never closes its side (half-open peers). */
function startHalfOpenServer(createServer = (opts, fn) => net.createServer(opts, fn), extra = {}) {
    return new Promise((resolve) => {
        const sockets = new Set();
        const server = createServer({ allowHalfOpen: true, ...extra }, (s) => {
            s.on('error', () => {});
            s.resume(); // read (and ignore) everything, never end()
        });
        // track raw connections (also TLS ones that never finish the handshake) for cleanup
        server.on('connection', (raw) => {
            sockets.add(raw);
            raw.on('error', () => {});
            raw.on('close', () => sockets.delete(raw));
        });
        server.listen(0, '127.0.0.1', () => {
            resolve({
                server,
                port: server.address().port,
                close: () => new Promise((r) => {
                    for (const s of sockets) s.destroy();
                    server.close(() => r());
                })
            });
        });
    });
}

function waitForClose(socket, timeoutMs) {
    return new Promise((resolve) => {
        if (socket.destroyed) return resolve(true);
        const t = setTimeout(() => resolve(false), timeoutMs);
        socket.once('close', () => {
            clearTimeout(t);
            resolve(true);
        });
    });
}

function connectWith(factory, opts) {
    return new Promise((resolve, reject) => {
        const socket = factory({ ...opts, onConnect: () => resolve(socket) });
        socket.on('error', reject);
    });
}

describe('lib/socket-factory', function () {
    this.timeout(10000);

    let srv;
    beforeEach(async function () {
        srv = await startHalfOpenServer();
    });
    afterEach(async function () {
        await srv.close();
    });

    it('connects like the kafkajs default factory and enables keep-alive (60 s)', async function () {
        const calls = [];
        const original = net.Socket.prototype.setKeepAlive;
        net.Socket.prototype.setKeepAlive = function (...args) {
            calls.push(args);
            return original.apply(this, args);
        };
        let socket;
        try {
            socket = await connectWith(createSocketFactory(), { host: '127.0.0.1', port: srv.port, ssl: false });
        } finally {
            net.Socket.prototype.setKeepAlive = original;
        }
        assert.ok(socket instanceof net.Socket);
        assert.ok(!(socket instanceof tls.TLSSocket));
        assert.deepEqual(calls[0], [true, 60000]);
        socket.destroy();
    });

    it('destroys a half-open socket shortly after end() (configurable delay)', async function () {
        const socket = await connectWith(createSocketFactory({ destroyAfterEndMs: 150 }),
            { host: '127.0.0.1', port: srv.port, ssl: false });
        const start = Date.now();
        socket.end();
        assert.equal(await waitForClose(socket, 2000), true, 'closed');
        const elapsed = Date.now() - start;
        assert.ok(elapsed >= 100, `not before the grace period (${elapsed} ms)`);
        assert.equal(socket.destroyed, true);
    });

    it(`with defaults, end() leads to destroy() within ~2.5 s (kafkajs' own factory leaves the socket open)`,
        async function () {
            assert.equal(DESTROY_AFTER_END_MS, 2000);
            const ours = await connectWith(createSocketFactory(), { host: '127.0.0.1', port: srv.port, ssl: false });
            const theirs = await connectWith(defaultKafkajsSocketFactory(), { host: '127.0.0.1', port: srv.port, ssl: false });
            const start = Date.now();
            // exactly what kafkajs does in Connection.disconnect()
            ours.end();
            ours.unref();
            theirs.end();
            theirs.unref();
            // ~2 s; generous upper bound so a loaded CI machine does not make this flaky
            assert.equal(await waitForClose(ours, 4000), true, 'our socket closed');
            const elapsed = Date.now() - start;
            assert.ok(elapsed >= 1900 && elapsed <= 4000, `closed after ${elapsed} ms`);
            assert.equal(theirs.destroyed, false, 'the default kafkajs socket is still half-open (the leak we fix)');
            theirs.destroy();
        });

    it('end() before the connection is established still ends in destroy()', async function () {
        const factory = createSocketFactory({ destroyAfterEndMs: 100 });
        const socket = factory({ host: '127.0.0.1', port: srv.port, ssl: false, onConnect: () => {} });
        socket.on('error', () => {});
        socket.end(); // still connecting
        assert.equal(await waitForClose(socket, 2000), true);
    });

    it('a normal close cancels the destroy timer', async function () {
        await srv.close();
        srv = await startHalfOpenServer((opts, fn) => net.createServer({}, (s) => {
            s.on('error', () => {});
            s.resume();
            fn(s);
        }));
        const socket = await connectWith(createSocketFactory({ destroyAfterEndMs: 5000 }),
            { host: '127.0.0.1', port: srv.port, ssl: false });
        let destroyCalls = 0;
        const originalDestroy = socket.destroy;
        socket.destroy = function (...args) {
            destroyCalls++;
            return originalDestroy.apply(this, args);
        };
        const start = Date.now();
        socket.end();
        assert.equal(await waitForClose(socket, 2000), true);
        assert.ok(Date.now() - start < 1000, 'closed by the peer, not by our timer');
        const before = destroyCalls;
        await new Promise((r) => setTimeout(r, 50));
        assert.equal(destroyCalls, before);
    });

    it('end() can be called several times and returns what net.Socket#end returns', async function () {
        const socket = await connectWith(createSocketFactory({ destroyAfterEndMs: 100 }),
            { host: '127.0.0.1', port: srv.port, ssl: false });
        assert.equal(socket.end(), socket);
        assert.doesNotThrow(() => socket.end());
        assert.equal(await waitForClose(socket, 2000), true);
        assert.doesNotThrow(() => socket.end());
    });

    it('throws synchronously for an invalid port, exactly like kafkajs (it reports "Failed to connect")', function () {
        const ours = createSocketFactory();
        const theirs = defaultKafkajsSocketFactory();
        let a;
        let b;
        try {
            ours({ host: 'localhost', port: NaN, ssl: false, onConnect: () => {} });
        } catch (e) {
            a = e;
        }
        try {
            theirs({ host: 'localhost', port: NaN, ssl: false, onConnect: () => {} });
        } catch (e) {
            b = e;
        }
        assert.ok(a && b);
        assert.equal(a.code, b.code);
        assert.equal(a.message, b.message);
    });

    it('ignores bad options', function () {
        assert.equal(typeof createSocketFactory({ destroyAfterEndMs: 'abc' }), 'function');
        assert.equal(typeof createSocketFactory(null), 'function');
    });

    describe('TLS', function () {
        let dir;
        let key;
        let cert;
        let tlsSrv;
        const sni = [];

        before(function () {
            try {
                dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-nc-socket-'));
                execFileSync('openssl', ['req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1',
                    '-nodes', '-keyout', path.join(dir, 'key.pem'), '-out', path.join(dir, 'cert.pem'), '-days', '1',
                    '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], { stdio: 'ignore' });
                key = fs.readFileSync(path.join(dir, 'key.pem'));
                cert = fs.readFileSync(path.join(dir, 'cert.pem'));
            } catch (_) {
                this.skip(); // no openssl available: the TLS path is identical to kafkajs' and optional here
            }
        });

        beforeEach(async function () {
            sni.length = 0;
            tlsSrv = await startHalfOpenServer((opts, fn) => {
                const server = tls.createServer({ ...opts, key, cert }, fn);
                server.on('secureConnection', (s) => sni.push(s.servername));
                server.on('tlsClientError', () => {});
                return server;
            });
        });

        afterEach(async function () {
            if (tlsSrv) await tlsSrv.close();
        });

        after(function () {
            if (dir) fs.rmSync(dir, { recursive: true, force: true });
        });

        it('connects with TLS, sends SNI = host for host names, verifies with the given CA', async function () {
            const socket = await connectWith(createSocketFactory(), { host: 'localhost', port: tlsSrv.port, ssl: { ca: cert } });
            assert.ok(socket instanceof tls.TLSSocket);
            assert.equal(socket.authorized, true);
            await new Promise((r) => setTimeout(r, 20));
            assert.deepEqual(sni, ['localhost']);
            socket.destroy();
        });

        it('sends no SNI for IP addresses and lets ssl.servername override', async function () {
            const a = await connectWith(createSocketFactory(),
                { host: '127.0.0.1', port: tlsSrv.port, ssl: { rejectUnauthorized: false } });
            const b = await connectWith(createSocketFactory(),
                { host: 'localhost', port: tlsSrv.port, ssl: { rejectUnauthorized: false, servername: 'kafka.example' } });
            await new Promise((r) => setTimeout(r, 20));
            assert.deepEqual(sni, [false, 'kafka.example']);
            a.destroy();
            b.destroy();
        });

        it('ssl: true uses system CAs and rejects the self-signed certificate', async function () {
            const err = await connectWith(createSocketFactory(), { host: 'localhost', port: tlsSrv.port, ssl: true })
                .then(() => null, (e) => e);
            assert.ok(err);
            assert.equal(err.code, 'DEPTH_ZERO_SELF_SIGNED_CERT');
        });

        it('destroys a half-open TLS socket after end()', async function () {
            const socket = await connectWith(createSocketFactory({ destroyAfterEndMs: 150 }),
                { host: 'localhost', port: tlsSrv.port, ssl: { ca: cert } });
            socket.end();
            assert.equal(await waitForClose(socket, 2000), true);
        });
    });
});
