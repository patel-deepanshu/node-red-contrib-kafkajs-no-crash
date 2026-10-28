'use strict';

/**
 * kafkajs-nc-admin: run Kafka admin operations (list/create/delete topics,
 * offsets, consumer groups, cluster and config info) from a flow.
 *
 * Design (SPEC §13, §17):
 *  - The admin client is created lazily by the first message and then kept.
 *    Any connection-level failure (transient, auth, config, unknown, or a
 *    timeout) drops it, so the next message starts from a fresh client.
 *  - Messages are processed strictly one after another (an internal promise
 *    chain); each is bounded by the node's timeout, which also covers
 *    connecting.
 *  - Input and close handlers are synchronous; every promise is handled.
 *    Failures go to `done(err)`, so Catch nodes receive the original message.
 */

const { ErrorKind, MessageError, classifyError, shortMessage, problemText, asConfigError } = require('../lib/errors');
const { getKafkajs } = require('../lib/kafka-factory');
const { StatusReporter } = require('../lib/status');
const { noop, once, toBool, toInt, withTimeout } = require('../lib/util');

const DEFAULT_TIMEOUT_MS = 30000;
const TIMEOUT_LIMITS = { min: 100, max: 600000 };
const DISCONNECT_TIMEOUT_MS = 3000;

// kafkajs ConfigResourceTypes, used when the module does not expose them.
const FALLBACK_RESOURCE_TYPES = { UNKNOWN: 0, TOPIC: 2, BROKER: 4, BROKER_LOGGER: 8 };

/** Errors created by our own operation timeout (they always drop the client). */
const timeoutErrors = new WeakSet();

// ---------------------------------------------------------------- parameters

const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);

/** msg.payload as an object of named parameters ({} for anything else). */
function params(payload) {
    return payload !== null && typeof payload === 'object' && !Array.isArray(payload) && !Buffer.isBuffer(payload)
        ? payload
        : {};
}

function isBlank(v) {
    return v === undefined || v === null || (typeof v === 'string' && v.trim() === '');
}

/**
 * A list of names from a string (split on commas/whitespace when `split`) or
 * an array of strings. Returns null when the value is missing.
 */
function nameList(value, what, action, { split = true } = {}) {
    if (isBlank(value)) return null;
    let list;
    if (typeof value === 'string') {
        list = split ? value.split(/[\s,]+/) : [value];
    } else if (Array.isArray(value)) {
        list = value.map((v) => {
            if (typeof v !== 'string') throw new MessageError(`${action}: every ${what} must be a string`);
            return v;
        });
    } else {
        throw new MessageError(`${action}: ${what}s must be a string or an array of strings`);
    }
    list = list.map((s) => s.trim()).filter((s) => s !== '');
    return list.length > 0 ? list : null;
}

function requireString(value, name, action, hint) {
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    if (typeof value !== 'string' || value.trim() === '') {
        throw new MessageError(`${action}: msg.payload.${name} is required${hint ? ` (${hint})` : ''}`);
    }
    return value.trim();
}

/** An optional integer parameter (numbers or numeric strings), checked against `min`. */
function optionalInt(obj, name, action, min) {
    if (!hasOwn(obj, name) || isBlank(obj[name])) return undefined;
    const n = toInt(obj[name], undefined);
    if (n === undefined || n !== Number(obj[name]) || n < min) {
        throw new MessageError(`${action}: ${name} must be an integer >= ${min}`);
    }
    return n;
}

function optionalBool(obj, name) {
    if (!hasOwn(obj, name) || isBlank(obj[name])) return undefined;
    return toBool(obj[name], undefined);
}

/** Copy only the defined values (kafkajs applies its own defaults to missing keys). */
function compact(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) if (v !== undefined) out[k] = v;
    return out;
}

/** configEntries as kafkajs wants them: [{name, value}] with string values. Accepts a {name: value} map too. */
function configEntries(value, topic, action) {
    if (value === undefined || value === null) return undefined;
    const toEntry = (name, v) => ({ name: String(name), value: v === null || v === undefined ? '' : String(v) });
    if (Array.isArray(value)) {
        return value.map((entry, i) => {
            if (!entry || typeof entry !== 'object' || isBlank(entry.name)) {
                throw new MessageError(`${action}: configEntries[${i}] of topic "${topic}" must be {name, value}`);
            }
            return toEntry(entry.name, entry.value);
        });
    }
    if (typeof value === 'object') return Object.entries(value).map(([name, v]) => toEntry(name, v));
    throw new MessageError(`${action}: configEntries of topic "${topic}" must be an array of {name, value} or an object`);
}

function topicSpec(spec, action) {
    if (typeof spec === 'string') return { topic: requireString(spec, 'topics[]', action) };
    if (!spec || typeof spec !== 'object') throw new MessageError(`${action}: every topic must be a name or {topic, ...}`);
    const topic = requireString(spec.topic, 'topics[].topic', action);
    const replicationFactor = optionalInt(spec, 'replicationFactor', action, -1);
    const numPartitions = optionalInt(spec, 'numPartitions', action, -1);
    if (numPartitions === 0) throw new MessageError(`${action}: numPartitions must be >= 1 (or -1 for the broker default)`);
    if (replicationFactor === 0) {
        throw new MessageError(`${action}: replicationFactor must be >= 1 (or -1 for the broker default)`);
    }
    return compact({
        topic,
        numPartitions,
        replicationFactor,
        replicaAssignment: Array.isArray(spec.replicaAssignment) ? spec.replicaAssignment : undefined,
        configEntries: configEntries(spec.configEntries, topic, action)
    });
}

function timestampMs(value, action) {
    if (value instanceof Date) {
        const t = value.getTime();
        if (Number.isFinite(t)) return t;
    } else if (typeof value === 'number' || (typeof value === 'string' && /^\s*\d+\s*$/.test(value))) {
        const t = toInt(value, undefined, { min: 0 });
        if (t !== undefined) return t;
    } else if (typeof value === 'string' && value.trim() !== '') {
        const t = Date.parse(value);
        if (Number.isFinite(t)) return t;
    }
    throw new MessageError(`${action}: msg.payload.timestamp is required (milliseconds since epoch, a Date or an ISO date string)`);
}

function resourceType(type, action) {
    let types = FALLBACK_RESOURCE_TYPES;
    try {
        const t = getKafkajs().ConfigResourceTypes;
        if (t && typeof t.TOPIC === 'number') types = t;
    } catch (_) {
        // use the fallback constants
    }
    if (typeof type === 'number' && Object.values(types).includes(type)) return type;
    if (typeof type === 'string') {
        const key = type.trim().toUpperCase().replace(/[\s-]/g, '_');
        if (hasOwn(types, key)) return types[key];
        if (/^\d+$/.test(key) && Object.values(types).includes(Number(key))) return Number(key);
    }
    throw new MessageError(`${action}: unknown resource type "${type}" (use ${Object.keys(types).join(', ')})`);
}

function groupIds(payload, action) {
    const p = params(payload);
    const list = nameList(typeof payload === 'string' || Array.isArray(payload) ? payload
        : (hasOwn(p, 'groupIds') ? p.groupIds : p.groupId), 'group id', action, { split: false });
    if (!list) throw new MessageError(`${action}: msg.payload.groupIds is required (a group id or an array of group ids)`);
    return list;
}

function topicList(payload, action) {
    const p = params(payload);
    const value = typeof payload === 'string' || Array.isArray(payload) ? payload
        : (hasOwn(p, 'topics') ? p.topics : p.topic);
    return nameList(value, 'topic', action);
}

/**
 * Every supported action: a function that validates msg.payload (throwing a
 * MessageError with a helpful text) and returns `(admin) => Promise`.
 * Validation runs before any connection is made.
 */
const ACTIONS = {
    listTopics: () => (admin) => admin.listTopics(),

    createTopics(payload, action) {
        const p = params(payload);
        let specs;
        if (Array.isArray(payload)) specs = payload; // names and/or {topic, ...} objects
        else if (typeof payload === 'string') specs = nameList(payload, 'topic', action);
        else if (Array.isArray(p.topics)) specs = p.topics;
        else if (typeof p.topics === 'string') specs = nameList(p.topics, 'topic', action);
        else if (!isBlank(p.topic)) specs = [p]; // a single {topic, numPartitions, ...}
        if (!specs || specs.length === 0) {
            throw new MessageError(`${action}: msg.payload.topics is required ` +
                '(e.g. {"topics":[{"topic":"orders","numPartitions":3,"replicationFactor":1}]} or "orders")');
        }
        const args = compact({
            topics: specs.map((s) => topicSpec(s, action)),
            validateOnly: optionalBool(p, 'validateOnly'),
            waitForLeaders: optionalBool(p, 'waitForLeaders'),
            timeout: optionalInt(p, 'timeout', action, 1)
        });
        return (admin) => admin.createTopics(args);
    },

    deleteTopics(payload, action) {
        const topics = topicList(payload, action);
        if (!topics) throw new MessageError(`${action}: msg.payload.topics is required (a topic name or an array of names)`);
        const args = compact({ topics, timeout: optionalInt(params(payload), 'timeout', action, 1) });
        return (admin) => admin.deleteTopics(args);
    },

    createPartitions(payload, action) {
        const p = params(payload);
        let list;
        if (Array.isArray(payload)) list = payload;
        else if (Array.isArray(p.topicPartitions)) list = p.topicPartitions;
        else if (!isBlank(p.topic)) list = [p];
        if (!list || list.length === 0) {
            throw new MessageError(`${action}: msg.payload.topicPartitions is required ` +
                '(e.g. {"topicPartitions":[{"topic":"orders","count":6}]})');
        }
        const topicPartitions = list.map((tp) => {
            if (!tp || typeof tp !== 'object') throw new MessageError(`${action}: every entry must be {topic, count}`);
            const count = optionalInt(tp, 'count', action, 1);
            if (count === undefined) throw new MessageError(`${action}: count (the new total number of partitions) is required`);
            return compact({
                topic: requireString(tp.topic, 'topicPartitions[].topic', action),
                count,
                assignments: Array.isArray(tp.assignments) ? tp.assignments : undefined
            });
        });
        const args = compact({
            topicPartitions,
            validateOnly: optionalBool(p, 'validateOnly'),
            timeout: optionalInt(p, 'timeout', action, 1)
        });
        return (admin) => admin.createPartitions(args);
    },

    fetchTopicMetadata(payload, action) {
        const topics = topicList(payload, action);
        // no topics => every topic of the cluster
        return (admin) => admin.fetchTopicMetadata(topics ? { topics } : {});
    },

    fetchTopicOffsets(payload, action) {
        const topic = requireString(typeof payload === 'string' ? payload : params(payload).topic, 'topic', action);
        return (admin) => admin.fetchTopicOffsets(topic);
    },

    fetchTopicOffsetsByTimestamp(payload, action) {
        const p = params(payload);
        const topic = requireString(p.topic, 'topic', action);
        const timestamp = timestampMs(p.timestamp, action);
        return (admin) => admin.fetchTopicOffsetsByTimestamp(topic, timestamp);
    },

    fetchOffsets(payload, action) {
        const p = params(payload);
        const groupId = requireString(typeof payload === 'string' ? payload : p.groupId, 'groupId', action);
        const topics = nameList(hasOwn(p, 'topics') ? p.topics : p.topic, 'topic', action);
        const args = compact({ groupId, topics: topics || undefined, resolveOffsets: optionalBool(p, 'resolveOffsets') });
        return (admin) => admin.fetchOffsets(args);
    },

    resetOffsets(payload, action) {
        const p = params(payload);
        const args = {
            groupId: requireString(p.groupId, 'groupId', action),
            topic: requireString(p.topic, 'topic', action),
            // kafkajs resets to the LATEST offset unless earliest is true
            earliest: toBool(p.earliest, false)
        };
        return (admin) => admin.resetOffsets(args);
    },

    setOffsets(payload, action) {
        const p = params(payload);
        const groupId = requireString(p.groupId, 'groupId', action);
        const topic = requireString(p.topic, 'topic', action);
        if (!Array.isArray(p.partitions) || p.partitions.length === 0) {
            throw new MessageError(`${action}: msg.payload.partitions is required (e.g. [{"partition":0,"offset":"42"}])`);
        }
        const partitions = p.partitions.map((entry) => {
            if (!entry || typeof entry !== 'object') throw new MessageError(`${action}: every partition must be {partition, offset}`);
            const partition = optionalInt(entry, 'partition', action, 0);
            if (partition === undefined) throw new MessageError(`${action}: partitions[].partition is required`);
            const offset = entry.offset;
            const text = typeof offset === 'bigint' ? offset.toString() : (typeof offset === 'number' ? String(Math.trunc(offset)) : offset);
            if (typeof text !== 'string' || !/^-?\d+$/.test(text.trim())) {
                throw new MessageError(`${action}: partitions[].offset must be an integer (number or string)`);
            }
            return { partition, offset: text.trim() };
        });
        return (admin) => admin.setOffsets({ groupId, topic, partitions });
    },

    listGroups: () => (admin) => admin.listGroups(),

    describeGroups(payload, action) {
        const ids = groupIds(payload, action);
        return (admin) => admin.describeGroups(ids);
    },

    deleteGroups(payload, action) {
        const ids = groupIds(payload, action);
        return (admin) => admin.deleteGroups(ids);
    },

    describeCluster: () => (admin) => admin.describeCluster(),

    describeConfigs(payload, action) {
        const p = params(payload);
        let list = Array.isArray(payload) ? payload : p.resources;
        if (!Array.isArray(list) && !isBlank(p.type) && !isBlank(p.name)) list = [p];
        if (!Array.isArray(list) || list.length === 0) {
            throw new MessageError(`${action}: msg.payload.resources is required ` +
                '(e.g. {"resources":[{"type":"TOPIC","name":"orders"}]})');
        }
        const resources = list.map((r) => {
            if (!r || typeof r !== 'object') throw new MessageError(`${action}: every resource must be {type, name}`);
            return compact({
                type: resourceType(r.type, action),
                name: requireString(r.name, 'resources[].name', action),
                configNames: r.configNames === undefined || r.configNames === null
                    ? undefined
                    : nameList(r.configNames, 'config name', action) || undefined
            });
        });
        const args = compact({ resources, includeSynonyms: optionalBool(p, 'includeSynonyms') });
        return (admin) => admin.describeConfigs(args);
    }
};

const ACTION_NAMES = Object.keys(ACTIONS);
const ACTION_BY_LOWER = new Map(ACTION_NAMES.map((a) => [a.toLowerCase(), a]));

/**
 * Validate the action and its parameters.
 *
 * @param {*} name config.action or msg.action
 * @param {*} payload msg.payload
 * @returns {{action: string, run: (admin: object) => Promise}}
 * @throws {MessageError}
 */
function prepare(name, payload) {
    if (isBlank(name)) {
        throw new MessageError(`No admin action: set the node's Action or msg.action (one of ${ACTION_NAMES.join(', ')})`);
    }
    const action = typeof name === 'string' ? ACTION_BY_LOWER.get(name.trim().toLowerCase()) : undefined;
    if (!action) {
        let shown;
        try {
            shown = String(name);
        } catch (_) {
            shown = typeof name;
        }
        throw new MessageError(`Unknown admin action "${shown}". Valid actions: ${ACTION_NAMES.join(', ')}`);
    }
    return { action, run: ACTIONS[action](payload, action) };
}

/** Disconnect without ever rejecting or taking longer than DISCONNECT_TIMEOUT_MS. */
function disconnectQuietly(admin) {
    let p;
    try {
        p = Promise.resolve(admin.disconnect());
    } catch (_) {
        return Promise.resolve();
    }
    p.catch(noop);
    return withTimeout(p, DISCONNECT_TIMEOUT_MS);
}

/**
 * Settle with `promise`, or reject with a timeout error after `ms`, or with
 * the error passed to `abort(err)`. The timer is always cleared; the original
 * promise keeps a handler attached so a late rejection can never become
 * unhandled.
 *
 * @returns {{promise: Promise, abort: (err: Error) => void}}
 */
function withDeadline(promise, ms, action) {
    let abort = noop;
    const raced = new Promise((resolve, reject) => {
        let timer = null;
        const settle = (fn, value) => {
            if (timer) clearTimeout(timer);
            timer = null;
            fn(value);
        };
        abort = (err) => settle(reject, err);
        timer = setTimeout(() => {
            try {
                const err = new Error(`Kafka admin ${action} timed out after ${ms}ms`);
                err.code = 'ETIMEDOUT';
                timeoutErrors.add(err);
                settle(reject, err);
            } catch (e) {
                // never throw out of a timer (it would crash Node-RED); still settle
                settle(reject, e);
            }
        }, ms);
        if (typeof timer.unref === 'function') timer.unref();
        promise.then((value) => settle(resolve, value), (err) => settle(reject, err));
    });
    return { promise: raced, abort };
}

/**
 * Call Node-RED's done() without letting it throw into our code. done(err)
 * clones the message for Catch nodes, which throws for messages that cannot
 * be cloned (e.g. throwing getters); the error is then at least logged.
 */
function safeDone(node, done, err) {
    try {
        if (err) done(err);
        else done();
    } catch (e) {
        try {
            node.error(`Could not complete the message: ${(err && err.message) || ''} (${e && e.message})`);
        } catch (_) {
            // nothing left to report to
        }
    }
}

module.exports = function (RED) {
    /**
     * @param {object} config { name, broker, action, timeout }
     */
    function KafkaAdminNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        const status = new StatusReporter(node);
        const state = {
            closing: false,
            gen: 0, // bumped whenever the current client is dropped: stale connects disconnect themselves
            admin: null, // connected client
            connecting: null, // client whose connect() is in flight
            chain: Promise.resolve(),
            queued: 0,
            busy: false, // a message has been accepted (its status wins over 'ready')
            inFlight: null, // { action, abort(err) } of the job being run
            startTimer: null
        };
        let broker = null;
        let configAction = '';
        let timeoutMs = DEFAULT_TIMEOUT_MS;

        // Close handler first: Node-RED never calls it if the constructor throws.
        node.on('close', function (removed, done) {
            const finish = once(() => {
                try {
                    done();
                } catch (_) {
                    // nothing left to do
                }
            });
            try {
                state.closing = true;
                state.gen++;
                if (state.startTimer) clearImmediate(state.startTimer);
                state.startTimer = null;
                status.dispose();
                // Settle the running job now (done(err)) rather than when its timeout fires.
                const inFlight = state.inFlight;
                state.inFlight = null;
                if (inFlight) inFlight.abort(new Error(`The Kafka admin node was stopped while running ${inFlight.action}`));
                const clients = [state.admin, state.connecting].filter(Boolean);
                state.admin = null;
                state.connecting = null;
                if (clients.length === 0) {
                    finish();
                    return;
                }
                // bounded: disconnectQuietly never takes longer than 3 s
                Promise.all(clients.map(disconnectQuietly)).then(finish, finish);
            } catch (_) {
                finish();
            }
        });

        /** Drop the current client(s) so that the next message connects again. */
        function dropClient() {
            state.gen++;
            const clients = [state.admin, state.connecting].filter(Boolean);
            state.admin = null;
            state.connecting = null;
            for (const client of clients) disconnectQuietly(client);
        }

        /** The connected admin client, connecting first if needed (gen-guarded). */
        async function ensureConnected() {
            if (state.admin) return state.admin;
            const gen = ++state.gen;
            status.connecting('connecting…');
            const kafka = broker.createKafka(node); // throws ConfigError when invalid
            let admin;
            try {
                admin = kafka.admin(); // may throw synchronously (kafkajs option checks)
            } catch (err) {
                throw asConfigError(err, 'Could not create the Kafka admin client');
            }
            state.connecting = admin;
            try {
                await admin.connect();
            } catch (err) {
                if (state.connecting === admin) state.connecting = null;
                disconnectQuietly(admin);
                throw err;
            }
            if (state.connecting === admin) state.connecting = null;
            if (state.closing || gen !== state.gen) {
                // closed or timed out meanwhile: this client must not survive (zombie)
                disconnectQuietly(admin);
                throw new Error('Kafka admin connection abandoned (the node is stopping or the request timed out)');
            }
            state.admin = admin;
            return admin;
        }

        function reportFailure(err, action) {
            const timedOut = timeoutErrors.has(err);
            let kind = ErrorKind.TRANSIENT;
            if (!timedOut) {
                try {
                    kind = classifyError(err, { op: 'admin' }).kind;
                } catch (_) {
                    kind = ErrorKind.UNKNOWN;
                }
            }
            // MESSAGE/TOPIC problems are about this request (e.g. topic exists);
            // the connection is fine. Everything else: start afresh next time.
            if (kind !== ErrorKind.MESSAGE && kind !== ErrorKind.TOPIC) dropClient();
            if (state.closing) return;
            if (timedOut) status.error(`${action} timed out`);
            // needs a human: same text as the producer/consumer ('auth failed', 'config error', 'TLS: ...')
            else if (kind === ErrorKind.AUTH || kind === ErrorKind.CONFIG) status.error(problemText(err, 48));
            else status.error(`${action}: ${shortMessage(err, 46 - action.length)}`);
        }

        /** Run one queued job. Never rejects; calls done exactly once. */
        async function runJob(msg, send, done, job) {
            state.queued = Math.max(0, state.queued - 1);
            const finish = once((err) => safeDone(node, done, err));
            let active = true; // false once this job has settled (e.g. timed out)
            const running = () => {
                if (active && !state.closing) {
                    status.set('blue', 'dot', `running ${job.action}${state.queued > 0 ? ` (+${state.queued} queued)` : ''}`);
                }
            };
            try {
                if (state.closing) {
                    finish(new Error(`The Kafka admin node stopped before running ${job.action}`));
                    return;
                }
                const started = Date.now();
                if (state.admin) running();
                const operation = ensureConnected().then((admin) => {
                    running();
                    return job.run(admin);
                });
                const deadline = withDeadline(operation, timeoutMs, job.action);
                const inFlight = { action: job.action, abort: deadline.abort };
                state.inFlight = inFlight;
                let result;
                try {
                    result = await deadline.promise;
                } finally {
                    active = false;
                    if (state.inFlight === inFlight) state.inFlight = null;
                }
                if (state.closing) {
                    finish();
                    return;
                }
                msg.payload = result === undefined ? true : result;
                msg.kafka = { action: job.action, durationMs: Date.now() - started };
                status.set('green', 'dot', `ok · ${job.action}`);
                try {
                    send(msg);
                } catch (err) {
                    // not a Kafka problem: report it for this message only
                    finish(err);
                    return;
                }
                finish();
            } catch (err) {
                try {
                    reportFailure(err, job.action);
                } catch (_) {
                    // reporting must not prevent done(err)
                }
                finish(err);
            }
        }

        try {
            configAction = typeof config.action === 'string' ? config.action.trim() : '';
            timeoutMs = toInt(config.timeout, DEFAULT_TIMEOUT_MS, TIMEOUT_LIMITS);
            broker = RED.nodes.getNode(config.broker) || null;

            node.on('input', function (msg, send, done) {
                let job;
                try {
                    if (state.closing) throw new Error('The Kafka admin node is stopping');
                    if (!broker) throw new Error('No Kafka broker configuration selected');
                    if (broker.configError) {
                        throw new Error(`Invalid Kafka broker configuration: ${configErrorText(broker.configError)}`);
                    }
                    job = prepare(configAction !== '' ? configAction : msg.action, msg.payload);
                } catch (err) {
                    safeDone(node, done, err);
                    return;
                }
                state.busy = true;
                state.queued++;
                // Strictly sequential; runJob never rejects, .catch is belt and braces.
                state.chain = state.chain.then(() => runJob(msg, send, done, job)).catch(noop);
            });

            // Statuses/errors from a constructor never reach Status/Catch nodes.
            state.startTimer = setImmediate(() => {
                state.startTimer = null;
                try {
                    if (state.closing) return;
                    if (!broker) {
                        status.error('no broker config');
                        node.error('No Kafka broker configuration selected');
                    } else if (broker.configError) {
                        status.error('config error');
                        node.error(`Invalid Kafka broker configuration: ${configErrorText(broker.configError)}`);
                    } else if (configAction !== '' && !ACTION_BY_LOWER.has(configAction.toLowerCase())) {
                        status.error('unknown action');
                        node.error(`Unknown admin action "${configAction}". Valid actions: ${ACTION_NAMES.join(', ')}`);
                    } else if (!state.busy) {
                        // a message may already be running (it arrived before this tick)
                        status.idle('ready');
                    }
                } catch (_) {
                    // status/logging must never throw out of a timer
                }
            });
        } catch (err) {
            try {
                status.error('error');
                node.error(`Kafka admin node failed to start: ${err && err.message}`);
            } catch (_) {
                // ignore
            }
        }
    }

    RED.nodes.registerType('kafkajs-nc-admin', KafkaAdminNode);
};

function configErrorText(configError) {
    try {
        if (Array.isArray(configError.details) && configError.details.length > 0) return configError.details.join('; ');
        return String(configError.message || configError);
    } catch (_) {
        return 'unknown problem';
    }
}

// exported for unit tests
module.exports.ACTION_NAMES = ACTION_NAMES;
module.exports._prepare = prepare;
