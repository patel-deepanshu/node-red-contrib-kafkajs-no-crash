'use strict';

/**
 * kafkajs-nc-broker: the shared Kafka connection settings (a config node).
 *
 * The broker node does NOT hold a connection itself. Each client node
 * (producer, consumer, admin) asks it for a brand-new kafkajs `Kafka`
 * instance on every connect attempt via `createKafka(node)`, so that:
 *  - kafkajs logs are attributed to the node that caused them;
 *  - a client that is torn down (redeploy, reconnect) can never be revived
 *    by a kafkajs retry loop that is still running in another instance.
 *
 * Everything here is validated up front and never throws: a broken
 * configuration (e.g. SASL selected but no password - the crash that started
 * this project) is recorded in `this.configError` and reported once; client
 * nodes check it and show "config error" instead of connecting.
 */

const clientConfig = require('../lib/client-config');
const { ConfigError, asConfigError } = require('../lib/errors');
const { getKafkajs } = require('../lib/kafka-factory');
const { createLogCreator } = require('../lib/logger');
const safetyNet = require('../lib/safety-net');
const { createSocketFactory } = require('../lib/socket-factory');
const { once, toBool } = require('../lib/util');

module.exports = function (RED) {
    /**
     * Config node constructor.
     *
     * Public API (used by the client nodes):
     *  - configError {ConfigError|null}  every problem found, in `.details`
     *  - warnings {string[]}
     *  - reconnect {{min, max, authMin}} supervisor backoff in ms
     *  - createKafka(forNode) a new kafkajs client for one connect attempt
     *
     * @param {object} config
     */
    function KafkaBrokerNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // Safe defaults first, so that a failure further down still leaves a
        // usable (if invalid) object behind for the client nodes.
        node._config = config || {};
        node._closed = false;
        node._releaseSafetyNet = null;
        node.configError = null;
        node.warnings = [];
        node.reconnect = {
            min: clientConfig.DEFAULTS.reconnectMin,
            max: clientConfig.DEFAULTS.reconnectMax,
            authMin: clientConfig.DEFAULTS.authRetryMin
        };

        // Register the close handler before anything can fail: Node-RED never
        // calls it when the constructor throws, which would leak the safety net.
        node.on('close', function (removed, done) {
            const finish = once(() => {
                try {
                    done();
                } catch (_) {
                    // nothing sensible left to do
                }
            });
            try {
                node._closed = true;
                // Client nodes may still be closing (Node-RED stops config nodes in parallel with them): the
                // safety net stays installed for a grace period after the last release (lib/safety-net.js).
                if (node._releaseSafetyNet) node._releaseSafetyNet();
                node._releaseSafetyNet = null;
            } catch (_) {
                // releasing is best effort; done() is what matters
            } finally {
                finish();
            }
        });

        try {
            const credentials = node.credentials || {};
            const normalized = clientConfig.normalizeBrokerConfig(config, credentials, earlyTlsCheck(config));

            node.reconnect = normalized.reconnect;
            node.warnings = normalized.warnings.slice();

            if (normalized.errors.length > 0) {
                node.configError = new ConfigError(normalized.errors.join('; '), normalized.errors);
                node.error(`Invalid Kafka broker configuration: ${normalized.errors.join('; ')}`);
            }
            for (const warning of node.warnings) node.warn(warning);

            // Last: the only resource this node owns.
            if (normalized.safetyNet) node._releaseSafetyNet = safetyNet.acquire(RED);
        } catch (err) {
            // Defensive: the lib functions never throw, but a constructor must not either.
            const text = `Invalid Kafka broker configuration: ${err && err.message}`;
            node.configError = new ConfigError(text, [text]);
            try {
                node.error(text);
            } catch (_) {
                // ignore
            }
        }
    }

    /**
     * The tls-config node can be checked at construction time only if it already
     * exists. Config nodes that reference other config nodes are normally created
     * after them, but that is not guaranteed across flows, so a node that cannot
     * be found yet is NOT an error here: createKafka() checks again lazily.
     *
     * @param {object} config
     * @returns {object|undefined} options for normalizeBrokerConfig
     */
    function earlyTlsCheck(config) {
        if (!toBool((config || {}).tls, false)) return undefined;
        const tlsNode = tlsNodeOf(config);
        return tlsNode ? { tlsNode } : undefined;
    }

    /** The tls-config node the configuration refers to, or null (none set, or not found). Never throws. */
    function tlsNodeOf(config) {
        try {
            const c = config || {};
            const id = typeof c.tlsConfig === 'string' ? c.tlsConfig.trim() : '';
            return id === '' ? null : RED.nodes.getNode(id) || null;
        } catch (_) {
            return null;
        }
    }

    RED.nodes.registerType('kafkajs-nc-broker', KafkaBrokerNode, {
        credentials: {
            username: { type: 'text' },
            password: { type: 'password' }
        }
    });

    /**
     * Create a NEW kafkajs Kafka client for one connect attempt of `forNode`.
     * Throws a ConfigError (with `.details`) when the configuration is invalid
     * or kafkajs refuses the options; callers must catch it.
     *
     * @param {object} [forNode] the client node (used to label kafkajs log lines)
     * @returns {object} kafkajs Kafka instance
     * @throws {ConfigError}
     */
    KafkaBrokerNode.prototype.createKafka = function (forNode) {
        if (this._closed) {
            throw new ConfigError('The Kafka broker configuration node has been stopped',
                ['The Kafka broker configuration node has been stopped']);
        }
        if (this.configError) throw this.configError;

        const config = this._config || {};
        const built = clientConfig.buildClientOptions(config, this.credentials || {}, {
            tlsNode: tlsNodeOf(config),
            socketFactory: createSocketFactory(),
            logCreator: createLogCreator(RED, logLabel(forNode, this))
        });
        if (built.error) {
            // Problems only visible now (e.g. an invalid tls-config node) are
            // permanent for this deployment: remember them for the client nodes.
            this.configError = built.error;
            throw built.error;
        }

        try {
            const { Kafka } = getKafkajs();
            return new Kafka(built.options);
        } catch (err) {
            throw asConfigError(err, 'Could not create the Kafka client');
        }
    };

    /** '[kafkajs-nc-producer:my producer]' - identifies the node in kafkajs log lines. */
    function logLabel(forNode, broker) {
        try {
            const n = forNode && typeof forNode === 'object' ? forNode : broker;
            return `[${n.type}:${n.name || n.id}]`;
        } catch (_) {
            return '[kafkajs-nc]';
        }
    }
};
