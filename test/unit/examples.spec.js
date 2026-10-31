'use strict';

/**
 * The example flows in examples/*.json (Node-RED lists them under
 * Import > Examples, using the file name as the menu label).
 *
 * Static checks, per file: one tab, unique Node-RED-style ids, only core
 * Node-RED nodes and this palette's nodes, wires and config references that
 * resolve inside the file, every editor default of our nodes present with a
 * value the editor's own validators accept (definitions taken from
 * nodes/*.html), and no credentials other than ${ENV} references. Across all
 * files: every non-config node of the palette is used by an example (flow
 * library scorecard rule N02).
 *
 * Runtime checks, per file, with the fake kafkajs: the flow loads into
 * Node-RED without errors or warnings, every node is constructed, the Kafka
 * nodes connect, and clicking every inject button and feeding each consumer
 * one message only produces the errors the example demonstrates on purpose
 * (EXPECTED_LOG below).
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const helper = require('node-red-node-test-helper');

const { createFakeKafkajs } = require('../helpers/fake-kafkajs');
const { tmpDir, sleep, waitFor, statusCalls } = require('../helpers/common');
const factory = require('../../lib/kafka-factory');
const brokerNode = require('../../nodes/kafka-broker.js');
const producerNode = require('../../nodes/kafka-producer.js');
const consumerNode = require('../../nodes/kafka-consumer.js');
const adminNode = require('../../nodes/kafka-admin.js');

helper.init(require.resolve('node-red'));

const ROOT = path.join(__dirname, '..', '..');
const EXAMPLES_DIR = path.join(ROOT, 'examples');
const NODES_DIR = path.join(ROOT, 'nodes');
const OUR_MODULES = [brokerNode, producerNode, consumerNode, adminNode];

/** Flow-level types that are not nodes of any module. */
const FLOW_TYPES = new Set(['tab', 'group', 'junction']);
/** Core types the test helper loads by itself when a flow contains them (loading them twice would fail). */
const HELPER_PRELOADED = new Set(['catch', 'status', 'complete']);
/** Core types without an input port: nothing may be wired to them. */
const NO_INPUT = new Set(['inject', 'catch', 'status', 'complete', 'link in', 'comment']);
/** Keys any node may have besides its defaults (x/y/z/wires, group, disabled, show label, credentials). */
const STANDARD_KEYS = new Set(['id', 'type', 'z', 'g', 'x', 'y', 'wires', 'd', 'l', 'credentials']);
const ENV_REF = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/;

/**
 * Values for the environment variables the examples reference (08 reads the
 * broker address and the SASL credentials from them). A new reference must be
 * added here, which is a reminder to document it in the example.
 */
const EXAMPLE_ENV = {
    KAFKA_BROKERS: 'localhost:9093',
    KAFKA_USER: 'example-user',
    KAFKA_PASSWORD: 'example-password'
};

/**
 * Errors and warnings an example produces on purpose when every button is
 * clicked, per file name prefix: [regex, reason]. Anything else reported at
 * warn level or above (node.error/node.warn, including errors that a catch
 * node handles, see problemsSince) fails the test.
 */
const EXPECTED_LOG = {
    '07': [
        [/msg\.topic: invalid Kafka topic name "sensors\/temp"/,
            'the "topic sensors/temp" button shows that MQTT-style topics are rejected'],
        [/No topic: set the node Topic or msg\.topic/,
            'the "no topic" button shows a message without any topic']
    ]
};

// ---------------------------------------------------------------- definitions

/** registerType(type, definition) calls of our nodes/*.html, evaluated like the editor does. */
function editorDefinitions() {
    const defs = {};
    for (const file of fs.readdirSync(NODES_DIR).filter((f) => f.endsWith('.html')).sort()) {
        const html = fs.readFileSync(path.join(NODES_DIR, file), 'utf8');
        for (const m of html.matchAll(/<script type="text\/javascript">([\s\S]*?)<\/script>/g)) {
            const sandbox = {
                RED: {
                    nodes: {
                        registerType(type, def) {
                            defs[type] = def;
                        }
                    },
                    _: (key) => key
                },
                // the validators only look for open edit dialogs: there are none here
                $: () => ({ length: 0 })
            };
            vm.runInNewContext(m[1], sandbox, { filename: path.join('nodes', file) });
        }
    }
    return defs;
}

/** Every node type registered by node-red's core nodes, with the module that implements it. */
function coreNodeTypes() {
    const nodesPkg = require.resolve('@node-red/nodes/package.json', {
        paths: [path.dirname(require.resolve('node-red'))]
    });
    const coreDir = path.join(path.dirname(nodesPkg), 'core');
    const types = new Map();
    const walk = (dir) => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                walk(full);
            } else if (entry.name.endsWith('.html')) {
                const html = fs.readFileSync(full, 'utf8');
                for (const m of html.matchAll(/RED\.nodes\.registerType\(\s*(['"])([^'"]+)\1/g)) {
                    types.set(m[2], full.replace(/\.html$/, '.js'));
                }
            }
        }
    };
    walk(coreDir);
    return types;
}

const DEFS = editorDefinitions();
const OUR_TYPES = Object.keys(DEFS);
const CORE = coreNodeTypes();

const FILES = fs.existsSync(EXAMPLES_DIR)
    ? fs.readdirSync(EXAMPLES_DIR).filter((f) => f.endsWith('.json')).sort()
    : [];

function readExample(file) {
    return JSON.parse(fs.readFileSync(path.join(EXAMPLES_DIR, file), 'utf8'));
}

function isConfigType(type) {
    if (DEFS[type]) return DEFS[type].category === 'config';
    return type === 'tls-config' || type === 'mqtt-broker' || type === 'http proxy' ||
        type === 'websocket-client' || type === 'websocket-listener' || type === 'global-config';
}

/** `${VAR}` names used in node properties and credentials (whole-value references, as Node-RED resolves them). */
function envReferences(flow) {
    const names = new Set();
    const visit = (v) => {
        if (typeof v === 'string') {
            const m = ENV_REF.exec(v);
            if (m) names.add(m[1]);
        } else if (v !== null && typeof v === 'object') {
            for (const x of Object.values(v)) visit(x);
        }
    };
    for (const n of flow) {
        if (n.type === 'comment' || n.type === 'tab') continue;
        visit(n);
    }
    return names;
}

// ---------------------------------------------------------------- runtime helpers

/**
 * Warnings the test harness itself causes on every load, not the examples:
 * [regex, reason].
 */
const HARNESS_LOG = [
    [/Deprecated call to RED\.runtime\.nodes\.registerType/,
        'node-red-node-test-helper registers its "helper" node type with the old API'],
    [/^\[runtime\] nodes\.credentials\.unencrypted$/,
        'the test runtime has no credentialSecret (a real Node-RED generates one)']
];

/** The Node.prototype error/warn spies installed by the test helper (shared by every node). */
function nodeSpies(flow) {
    const any = flow.map((n) => helper.getNode(n.id)).find(Boolean);
    return { error: any.error, warn: any.warn };
}

/** Where the problem lists stand now; pass it to problemsSince(). */
function mark(flow) {
    const spies = nodeSpies(flow);
    return { error: spies.error.args.length, warn: spies.warn.args.length, log: helper.log().args.length };
}

function describeArg(arg) {
    if (arg instanceof Error) return arg.message;
    if (arg !== null && typeof arg === 'object' && typeof arg.message === 'string') return arg.message;
    return String(arg);
}

/**
 * Everything reported at warn level or above since `since` (default: since
 * the flow was loaded): node.error() and node.warn() calls of every node,
 * including errors that a catch node handles (Node-RED does not log those),
 * and runtime log entries that do not come from a node. HARNESS_LOG entries
 * are left out.
 */
function problemsSince(flow, since = { error: 0, warn: 0, log: 0 }) {
    const out = [];
    const spies = nodeSpies(flow);
    for (const level of ['error', 'warn']) {
        const spy = spies[level];
        for (let i = since[level]; i < spy.args.length; i++) {
            const node = spy.thisValues[i] || {};
            out.push({ level, text: `[${node.type}:${node.id}] ${describeArg(spy.args[i][0])}` });
        }
    }
    const log = helper.log();
    for (let i = since.log; i < log.args.length; i++) {
        const entry = log.args[i][0] || {};
        // entries of nodes were counted above, from the node spies
        if (entry.id || typeof entry.level !== 'number' || entry.level > log.WARN) continue;
        out.push({ level: entry.level === log.WARN ? 'warn' : 'error', text: `[runtime] ${describeArg(entry.msg)}` });
    }
    return out.filter((p) => !HARNESS_LOG.some(([re]) => re.test(p.text)));
}

/** True once every Kafka client node of the flow is idle: connected (or deliberately stopped), nothing in flight. */
function settled(flow) {
    for (const n of flow) {
        const node = helper.getNode(n.id);
        if (!node) continue;
        if (n.type === 'kafkajs-nc-producer') {
            const s = node.producerRuntime.snapshot();
            if (s.state !== 'connected' || s.mode !== 'live' || s.flushing || s.working || s.pending > 0) return false;
        } else if (n.type === 'kafkajs-nc-consumer') {
            const s = node.consumerRuntime.snapshot();
            if (s.state !== 'connected' && s.state !== 'stopped') return false;
        } else if (n.type === 'kafkajs-nc-admin') {
            const calls = statusCalls(node);
            const last = calls[calls.length - 1];
            if (!last || last.fill === 'blue' || /^connecting/.test(last.text || '')) return false;
        }
    }
    return true;
}

/** A message for a consumer of the examples: JSON that every example's processing understands. */
function sampleRecord(i) {
    return {
        key: 'sensor-1',
        value: JSON.stringify({ n: i, orderId: 1000 + i, customer: 'alice', total: 9.5, sensor: 'sensor-1', temperature: 21.5, ts: Date.now() }),
        headers: { event: 'order-created', source: 'node-red' }
    };
}

// ---------------------------------------------------------------- tests

describe('example flows (examples/*.json)', function () {
    it('exist, and are named "NN - Title.json" so that the Import > Examples menu is sorted', function () {
        assert.ok(FILES.length > 0, 'no examples/*.json files');
        for (const file of FILES) assert.match(file, /^\d{2} - [^\\/]+\.json$/, file);
    });

    it('use every non-config node of this palette (flow library scorecard N02)', function () {
        const used = new Set();
        for (const file of FILES) for (const n of readExample(file)) used.add(n.type);
        const nonConfig = OUR_TYPES.filter((t) => DEFS[t].category !== 'config');
        assert.ok(nonConfig.length >= 3, `expected the producer, consumer and admin definitions, got ${nonConfig}`);
        assert.deepEqual(nonConfig.filter((t) => !used.has(t)), [], 'node types not used in any example');
    });

    it('do not describe or set behaviour that changed before 1.0 (idempotent producer, "at most once" auto commit)', function () {
        for (const file of FILES) {
            const text = JSON.stringify(readExample(file));
            // the producer option was removed (kafkajs needs its lossy produce retry for it)
            assert.doesNotMatch(text, /idempotent/i, file);
            // automatic commits re-deliver the records handed over since the last stored position
            assert.doesNotMatch(text, /at most once/i, file);
        }
    });

    for (const file of FILES) {
        describe(file, function () {
            let flow;
            let byId;

            before(function () {
                flow = readExample(file);
                byId = new Map(flow.map((n) => [n.id, n]));
            });

            it('is an array with one tab, unique 16-hex-digit ids and an explanatory comment', function () {
                assert.ok(Array.isArray(flow), 'not a JSON array');
                const ids = flow.map((n) => n.id);
                assert.equal(new Set(ids).size, ids.length, 'duplicate ids');
                for (const n of flow) {
                    assert.match(String(n.id), /^[0-9a-f]{16}$/, `id of ${n.type} "${n.name || ''}"`);
                    assert.equal(typeof n.type, 'string', `type of ${n.id}`);
                }
                const tabs = flow.filter((n) => n.type === 'tab');
                assert.equal(tabs.length, 1, 'expected exactly one tab');
                assert.ok(String(tabs[0].label || '').trim(), 'the tab needs a label');
                for (const n of flow) {
                    if (n.type === 'tab') continue;
                    if (isConfigType(n.type)) {
                        assert.ok(n.z === undefined || n.z === tabs[0].id, `config node ${n.id} on an unknown flow`);
                    } else {
                        assert.equal(n.z, tabs[0].id, `${n.type} ${n.id} is not on the tab`);
                        assert.ok(Number.isFinite(n.x) && Number.isFinite(n.y), `${n.type} ${n.id} has no position`);
                    }
                }
                const comments = flow.filter((n) => n.type === 'comment' && String(n.info || '').trim());
                assert.ok(comments.length > 0, 'no comment node with an explanation (info)');
            });

            it('only uses core Node-RED nodes and the nodes of this palette', function () {
                for (const n of flow) {
                    assert.ok(FLOW_TYPES.has(n.type) || CORE.has(n.type) || DEFS[n.type],
                        `${n.id}: "${n.type}" is neither a core Node-RED node nor one of ours`);
                }
            });

            it('wires and references only point to nodes in the same file', function () {
                for (const n of flow) {
                    if (n.wires !== undefined) {
                        assert.ok(Array.isArray(n.wires) && n.wires.every(Array.isArray), `${n.id}: wires must be an array of arrays`);
                        for (const target of n.wires.flat()) {
                            const t = byId.get(target);
                            assert.ok(t, `${n.type} ${n.id} is wired to missing node ${target}`);
                            assert.ok(!NO_INPUT.has(t.type) && !isConfigType(t.type), `${n.id} is wired to ${t.type} ${target}, which has no input`);
                        }
                        const outputs = DEFS[n.type] ? DEFS[n.type].outputs : n.outputs;
                        if (outputs !== undefined) {
                            assert.equal(n.wires.length, Number(outputs), `${n.type} ${n.id}: one wires entry per output`);
                        }
                    }
                    if (['catch', 'status', 'complete'].includes(n.type) && Array.isArray(n.scope)) {
                        for (const id of n.scope) assert.ok(byId.has(id), `${n.type} ${n.id}: scope lists missing node ${id}`);
                    }
                    if (n.type === 'link in' || n.type === 'link out') {
                        const other = n.type === 'link in' ? 'link out' : 'link in';
                        for (const id of n.links || []) {
                            assert.equal(byId.get(id) && byId.get(id).type, other, `${n.type} ${n.id}: link to ${id}`);
                        }
                    }
                    const def = DEFS[n.type];
                    if (!def) continue;
                    for (const [key, d] of Object.entries(def.defaults)) {
                        if (!d.type) continue;
                        const ref = n[key];
                        if (ref === '' || ref === undefined || ref === null) {
                            assert.ok(!d.required, `${n.type} ${n.id}: ${key} (a ${d.type} node) is required`);
                            continue;
                        }
                        const target = byId.get(ref);
                        assert.ok(target, `${n.type} ${n.id}: ${key} points to missing node ${ref}`);
                        assert.equal(target.type, d.type, `${n.type} ${n.id}: ${key} must point to a ${d.type} node`);
                    }
                }
            });

            it("sets every property of this palette's nodes, with values the editor accepts", function () {
                const ours = flow.filter((n) => DEFS[n.type]);
                assert.ok(ours.length > 0, 'no node of this palette');
                for (const n of ours) {
                    const def = DEFS[n.type];
                    const where = `${n.type} ${n.id} ("${n.name || ''}")`;
                    for (const [key, d] of Object.entries(def.defaults)) {
                        assert.ok(Object.prototype.hasOwnProperty.call(n, key), `${where}: missing property ${key} ` +
                            '(the editor would add it and mark the node as changed)');
                        if (d.required) assert.ok(n[key] !== '' && n[key] != null, `${where}: ${key} is required`);
                        if (typeof d.validate === 'function') {
                            const verdict = d.validate.call(n, n[key], { label: key });
                            assert.equal(verdict, true, `${where}: invalid ${key} = ${JSON.stringify(n[key])}: ${verdict}`);
                        }
                        const defaultType = typeof d.value;
                        if (d.value !== undefined && d.value !== null && !d.type && defaultType !== 'object' &&
                            typeof n[key] !== defaultType) {
                            // numbers may be saved as text by the editor, everything else keeps its type
                            assert.ok(defaultType === 'number' && typeof n[key] === 'string',
                                `${where}: ${key} should be a ${defaultType}, not ${JSON.stringify(n[key])}`);
                        }
                    }
                    for (const key of Object.keys(n)) {
                        assert.ok(STANDARD_KEYS.has(key) || Object.prototype.hasOwnProperty.call(def.defaults, key),
                            `${where}: unknown property ${key}`);
                    }
                    if (n.type === 'kafkajs-nc-broker') {
                        assert.ok(n.brokers === 'localhost:9092' || ENV_REF.test(n.brokers),
                            `${where}: brokers should be localhost:9092 or an \${ENV} reference, not ${n.brokers}`);
                    }
                }
            });

            it('contains no credentials other than ${ENV} references', function () {
                for (const n of flow) {
                    if (n.credentials === undefined) continue;
                    assert.ok(n.credentials && typeof n.credentials === 'object', `${n.id}: credentials must be an object`);
                    for (const [key, value] of Object.entries(n.credentials)) {
                        assert.match(String(value), ENV_REF, `${n.type} ${n.id}: credential ${key} must be an \${ENV} reference`);
                    }
                }
                for (const name of envReferences(flow)) {
                    assert.ok(Object.prototype.hasOwnProperty.call(EXAMPLE_ENV, name),
                        `\${${name}} is not in EXAMPLE_ENV of this test: add it (and document it in the example)`);
                }
            });

            describe('in a Node-RED runtime (fake kafkajs)', function () {
                let fake;
                const savedEnv = {};

                before(function () {
                    fake = createFakeKafkajs();
                    factory.setKafkajs(fake);
                });

                after(function () {
                    factory.setKafkajs(null);
                });

                beforeEach(function (done) {
                    fake.reset();
                    fake.behaviour.clusterTopics = Array.from(new Set(flow.flatMap((n) =>
                        String(n.topics || n.topic || '').split(/[\s,]+/).filter((t) => /^[A-Za-z0-9._-]+$/.test(t)))));
                    for (const name of envReferences(flow)) {
                        savedEnv[name] = process.env[name];
                        process.env[name] = EXAMPLE_ENV[name];
                    }
                    helper.settings({ userDir: tmpDir('kafkajs-nc-examples-') });
                    helper.startServer(done);
                });

                afterEach(async function () {
                    await helper.unload();
                    await helper.stopServer();
                    for (const [name, value] of Object.entries(savedEnv)) {
                        if (value === undefined) delete process.env[name];
                        else process.env[name] = value;
                        delete savedEnv[name];
                    }
                });

                /** Load the example as Node-RED would after an import and a deploy. */
                async function load() {
                    const credentials = {};
                    const nodes = flow.map((n) => {
                        const { credentials: c, ...rest } = n;
                        if (c) credentials[n.id] = c;
                        return rest;
                    });
                    const core = [];
                    for (const n of flow) {
                        const file = CORE.get(n.type);
                        if (!file || HELPER_PRELOADED.has(n.type)) continue;
                        const mod = require(file);
                        if (!core.includes(mod)) core.push(mod);
                    }
                    await helper.load([...OUR_MODULES, ...core], nodes, credentials);
                    await waitFor(() => settled(flow), { timeout: 5000, message: 'the Kafka nodes did not connect' });
                }

                it('loads without errors or warnings, and every node starts', async function () {
                    await load();
                    for (const n of flow) {
                        if (n.type === 'tab') continue;
                        assert.ok(helper.getNode(n.id), `${n.type} ${n.id} ("${n.name || ''}") was not created`);
                    }
                    for (const n of flow.filter((x) => x.type === 'kafkajs-nc-broker')) {
                        const b = helper.getNode(n.id);
                        assert.equal(b.configError, null, `broker ${n.id}: ${b.configError && b.configError.message}`);
                    }
                    // environment variables (example 08) reached the kafkajs client options
                    for (const kafka of fake.instances) {
                        const text = JSON.stringify({ brokers: kafka.options.brokers, sasl: kafka.options.sasl });
                        assert.ok(!text.includes('${'), `unresolved environment variable in the client options: ${text}`);
                    }
                    await sleep(100);
                    assert.deepEqual(problemsSince(flow), [], 'reported while loading');
                });

                it('only logs the errors it demonstrates when every button is clicked and each consumer gets a message', async function () {
                    this.timeout(30000);
                    await load();
                    const start = mark(flow);
                    for (const n of flow.filter((x) => x.type === 'inject')) {
                        helper.getNode(n.id).receive({});
                        await sleep(30);
                        await waitFor(() => settled(flow), { timeout: 5000, message: `after clicking "${n.name}"` });
                    }
                    let i = 0;
                    for (const n of flow.filter((x) => x.type === 'kafkajs-nc-consumer')) {
                        const rt = helper.getNode(n.id).consumerRuntime;
                        if (rt.snapshot().state !== 'connected' || !rt.consumer) continue;
                        const topic = rt.cfg.topics[0];
                        assert.equal(await rt.consumer.deliver({ topic, message: sampleRecord(++i) }), true,
                            `consumer ${n.id} did not take the message (paused?)`);
                        if (n.commitMode === 'manual') {
                            // the example wires processed messages back with msg.action = "commit"
                            await waitFor(() => rt.consumer.committed.length > 0,
                                { timeout: 3000, message: `the manual commit loop of consumer ${n.id} did not commit` });
                        }
                    }
                    await sleep(200);
                    await waitFor(() => settled(flow), { timeout: 5000, message: 'after the consumer messages' });

                    const expected = EXPECTED_LOG[file.slice(0, 2)] || [];
                    const problems = problemsSince(flow, start);
                    const unexpected = problems.filter((p) => !expected.some(([re]) => re.test(p.text)));
                    assert.deepEqual(unexpected, [], 'unexpected errors or warnings');
                    for (const [re, reason] of expected) {
                        assert.ok(problems.some((p) => re.test(p.text)), `expected an error for: ${reason} (${re})`);
                    }
                });
            });
        });
    }
});
