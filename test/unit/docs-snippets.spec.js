'use strict';

/**
 * The code snippets of docs/OFFLINE-QUEUE.md, run against real queue files.
 *
 * Each snippet follows a `<!-- snippet: <name> ... -->` marker in the
 * document. The queue files are written by the real DiskQueue with the real
 * record format of the producer (toRecord), so a change of the format that
 * breaks the documented snippets fails here.
 */

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const { DiskQueue } = require('../../lib/disk-queue');
const { MessageError } = require('../../lib/errors');
const serialize = require('../../lib/serialize');
const { _internals: producer } = require('../../nodes/kafka-producer.js');
const { tmpDir } = require('../helpers/common');

const DOC = path.join(__dirname, '..', '..', 'docs', 'OFFLINE-QUEUE.md');

/** The first ```js block after `<!-- snippet: name`. */
function snippet(name) {
    const text = fs.readFileSync(DOC, 'utf8');
    const at = text.indexOf(`<!-- snippet: ${name}`);
    assert.ok(at >= 0, `snippet marker "${name}" not found in ${DOC}`);
    const m = /```js\n([\s\S]*?)```/.exec(text.slice(at));
    assert.ok(m, `no js code block after the "${name}" marker`);
    return m[1];
}

/** Queue records exactly as the producer writes them. */
function records() {
    const cfg = producer.parseConfig({});
    const now = Date.UTC(2026, 9, 28, 12, 0, 0);
    const msgs = [
        { topic: 'telemetry', key: 'sensor-1', payload: { temp: 21.5 }, headers: { source: 'line-3', tags: ['a', 'b'] } },
        { topic: 'images', payload: Buffer.alloc(1000, 7), partition: 2, headers: { raw: Buffer.from([0, 255]) } },
        { topic: 'telemetry', key: 'sensor-2', payload: null, timestamp: 1793188800000 }
    ];
    return msgs.map((msg) => producer.toRecord(producer.buildEntries(msg, cfg, now)[0], now));
}

async function openQueue(dir) {
    const q = new DiskQueue({ dir });
    await q.open();
    return q;
}

describe('docs/OFFLINE-QUEUE.md snippets', function () {
    describe('inspect-queue', function () {
        let script;

        before(function () {
            script = path.join(tmpDir('kafkajs-nc-snippet-'), 'inspect-queue.js');
            fs.writeFileSync(script, snippet('inspect-queue'));
        });

        const run = (dir) => execFileSync(process.execPath, [script, dir], { encoding: 'utf8' });

        it('lists every waiting record of a queue that has not sent anything yet', async function () {
            const dir = path.join(tmpDir('kafkajs-nc-snippet-q-'), 'queue');
            const q = await openQueue(dir);
            await q.enqueueMany(records());
            await q.close();
            const out = run(dir).trim().split('\n');
            assert.equal(out.length, 4, out.join('\n'));
            assert.match(out[0], /^2026-10-28T12:00:00\.000Z telemetry key: sensor-1 value: \{"temp":21\.5\}$/);
            assert.match(out[1], /^2026-10-28T12:00:00\.000Z images key: - value: <1000 bytes>$/);
            assert.match(out[2], /telemetry key: sensor-2 value: null$/);
            assert.equal(out[3], `3 message(s) waiting in ${dir}`);
        });

        it('starts at the cursor, across segments, and skips a line that is being written', async function () {
            const dir = path.join(tmpDir('kafkajs-nc-snippet-q-'), 'queue');
            const q = new DiskQueue({ dir, segmentMaxBytes: 200 }); // one record per segment
            await q.open();
            await q.enqueueMany(records());
            await q.enqueueMany(records());
            await q.ack(await q.peek(4)); // 4 of 6 sent: the cursor is in the middle
            await q.close();
            assert.ok(fs.existsSync(path.join(dir, 'cursor.json')));
            const newest = fs.readdirSync(dir).filter((f) => f.startsWith('seg-')).sort().pop();
            fs.appendFileSync(path.join(dir, newest), '{"v":1,"t":"half'); // an append in progress
            const out = run(dir).trim().split('\n');
            assert.equal(out[out.length - 1], `2 message(s) waiting in ${dir}`);
            assert.match(out[0], /images key: - value: <1000 bytes>/);
        });

        it('reports a damaged line instead of failing', async function () {
            const dir = path.join(tmpDir('kafkajs-nc-snippet-q-'), 'queue');
            const q = await openQueue(dir);
            await q.enqueueMany(records().slice(0, 1));
            await q.close();
            const seg = fs.readdirSync(dir).find((f) => f.startsWith('seg-'));
            fs.appendFileSync(path.join(dir, seg), 'not json\n');
            const out = run(dir);
            assert.match(out, /damaged line \(it will go to the dead-letter file\): not json/);
            assert.match(out, /2 message\(s\) waiting/);
        });
    });

    describe('replay-dead-letters', function () {
        /** Run the function-node body like Node-RED does (msg, node and Buffer in scope). */
        function runFunction(body, payload) {
            const warnings = [];
            const fn = new Function('msg', 'node', 'Buffer', body);
            const result = fn({ payload, topic: 'from-file-in', parts: { index: 0 } }, { warn: (w) => warnings.push(w) }, Buffer);
            return { result, warnings };
        }

        it('turns every dead-letter line into a message that the producer sends exactly as queued', async function () {
            const dir = path.join(tmpDir('kafkajs-nc-snippet-dl-'), 'queue');
            const q = await openQueue(dir);
            const recs = records();
            await q.enqueueMany(recs);
            const entries = await q.peek(10);
            const tooLarge = new Error('The request included a message larger than the max message size the server will accept');
            tooLarge.name = 'KafkaJSProtocolError';
            tooLarge.type = 'MESSAGE_TOO_LARGE';
            await q.deadLetter(entries[0], tooLarge);
            await q.deadLetter(entries[1], new MessageError('Partition 2 does not exist for topic images'));
            await q.deadLetter(entries[2], 'expired: queued longer than 24 h (queueMaxAgeHours)');
            await q.deadLetter({ corrupt: true, raw: '{"v":1,"t":"brok', pos: entries[2].pos }, new MessageError('Corrupt queue record'));
            await q.close();

            // what a "file in" node in "a msg per line" mode emits, including the empty part after the last newline
            const lines = fs.readFileSync(path.join(dir, 'dead-letter.jsonl'), 'utf8').split('\n');
            assert.equal(lines.length, 5);
            const body = snippet('replay-dead-letters');
            const out = lines.map((line) => runFunction(body, line));

            assert.equal(out[4].result, null, 'the empty last line is skipped');
            assert.equal(out[3].result, null, 'a damaged queue line is skipped');
            assert.match(out[3].warnings[0], /Skipped a damaged queue line: \{"v":1,"t":"brok/);

            // the producer (Topic/Key/Partition empty, msg.headers on) rebuilds the original Kafka message
            const cfg = producer.parseConfig({});
            for (let i = 0; i < 3; i++) {
                const msg = out[i].result;
                const [entry] = producer.buildEntries(msg, cfg, Date.now());
                const original = serialize.decodeMessage(recs[i].m);
                assert.equal(entry.topic, recs[i].t, `topic of record ${i}`);
                assert.deepEqual(entry.message, original, `message of record ${i}`);
            }
            assert.equal(out[2].result.payload, null, 'tombstone');
            assert.equal(out[2].result.timestamp, '1793188800000');
            assert.ok(Buffer.isBuffer(out[1].result.payload));
        });
    });
});
