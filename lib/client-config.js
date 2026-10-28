'use strict';

/**
 * Broker configuration → validated kafkajs `new Kafka(options)`.
 *
 * The Node-RED editor saves every field as a string, and kafkajs breaks on
 * string numbers (`requestTimeout: '30000'` makes kafka.producer() throw,
 * string heartbeat/session timeouts are compared as strings, a string log
 * level disables filtering...). Everything is parsed defensively here, with
 * defaults and clamping, and every problem is collected so the user sees all
 * of them at once. Nothing in this module throws.
 */

const { toInt, toBool, envProblem, envReference } = require('./util');
const { ConfigError } = require('./errors');
const { toKafkaLogLevel } = require('./logger');

/** Default values of the broker config node properties (as the editor saves them). */
const DEFAULTS = Object.freeze({
    name: '',
    brokers: 'localhost:9092',
    clientId: 'node-red',
    authMechanism: 'none',
    tls: false,
    tlsConfig: '',
    connectionTimeout: 10000,
    authenticationTimeout: 10000,
    requestTimeout: 30000,
    retries: 5,
    initialRetryTime: 300,
    maxRetryTime: 30000,
    reconnectMin: 1000,
    reconnectMax: 60000,
    authRetryMin: 60000,
    logLevel: 'warn',
    safetyNet: true
});

/** Allowed ranges of the numeric properties. */
const LIMITS = Object.freeze({
    connectionTimeout: { min: 100, max: 300000 },
    authenticationTimeout: { min: 100, max: 300000 },
    requestTimeout: { min: 1000, max: 600000 },
    retries: { min: 0, max: 50 },
    initialRetryTime: { min: 50, max: 60000 },
    maxRetryTime: { min: 100, max: 600000 },
    reconnectMin: { min: 100, max: 600000 },
    reconnectMax: { min: 100, max: 3600000 }, // and at least reconnectMin
    authRetryMin: { min: 1000, max: 3600000 }
});

const MECHANISMS = Object.freeze({
    plain: 'PLAIN',
    'scram-sha-256': 'SCRAM-SHA-256',
    'scram-sha-512': 'SCRAM-SHA-512'
});

const LOG_LEVEL_NAMES = new Set(['off', 'nothing', 'error', 'warn', 'info', 'debug']);

const SCHEMES = new Set(['plaintext', 'ssl', 'sasl_ssl', 'sasl_plaintext', 'kafka', 'tcp']);

/**
 * A `${VAR}` problem in a credential. Only a reference that makes up the whole value counts (spaces around
 * it, which stop Node-RED from replacing it, are reported too): a password may legitimately contain '${'.
 */
function credentialEnvProblem(value) {
    const ref = envReference(value.trim());
    return ref && ref.whole ? envProblem(value) : null;
}

function safeString(v) {
    try {
        return v == null ? '' : String(v);
    } catch (_) {
        return '';
    }
}

/**
 * Parse the brokers setting.
 * Accepts a string (separated by commas, semicolons and/or whitespace) or an
 * array of strings. Strips scheme prefixes (PLAINTEXT://, SSL://, SASL_SSL://,
 * SASL_PLAINTEXT://, kafka://, tcp://) and reports the ones it saw in
 * `schemes` (lower case, see schemeWarnings()), appends ':9092' when the port
 * is missing, removes duplicates, and reports IPv6 literals (unsupported by
 * kafkajs), credentials in an address, invalid ports/hosts and `${VAR}`
 * references Node-RED did not replace.
 *
 * @param {string|string[]} input
 * @returns {{brokers: string[], errors: string[], schemes: string[]}}
 */
function parseBrokers(input) {
    const brokers = [];
    const errors = [];
    const schemes = [];
    let tokens = [];
    try {
        const list = Array.isArray(input) ? input : [input];
        for (const item of list) {
            // only text makes sense here; objects would stringify to '[object Object]'
            if (typeof item !== 'string' && typeof item !== 'number') continue;
            const text = String(item);
            // a whole-value ${VAR} left over is not set; one inside the list is never replaced by Node-RED
            const variable = envProblem(text);
            if (variable !== null) {
                errors.push(variable);
                continue;
            }
            tokens = tokens.concat(text.split(/[\s,;]+/).filter(Boolean));
        }
    } catch (_) {
        tokens = [];
    }
    for (const token of tokens) {
        let rest = token;
        let schemeName = null;
        const scheme = /^([A-Za-z][A-Za-z0-9+._-]*):\/\//.exec(rest);
        if (scheme) {
            schemeName = scheme[1].toLowerCase();
            if (!SCHEMES.has(schemeName)) {
                errors.push(`Invalid broker "${token}": unsupported scheme "${scheme[1]}://"`);
                continue;
            }
            rest = rest.slice(scheme[0].length);
        }
        rest = rest.replace(/\/+$/, '');
        if (rest.includes('@')) {
            // e.g. SASL_SSL://user:secret@host:9093 - never echo it: the token may contain a password
            errors.push('Invalid broker: credentials do not belong in the broker address (found "@"); ' +
                'set them on the Security tab');
            continue;
        }
        const colons = (rest.match(/:/g) || []).length;
        if (rest.startsWith('[') || rest.includes('::') || (colons >= 7 && /^[0-9A-Fa-f:.]+$/.test(rest))) {
            errors.push(`IPv6 literal brokers are not supported by kafkajs (${token})`);
            continue;
        }
        if (colons > 1) {
            errors.push(`Invalid broker "${token}": use host:port`);
            continue;
        }
        let host = rest;
        let port = '9092';
        const colon = rest.lastIndexOf(':');
        if (colon !== -1) {
            host = rest.slice(0, colon);
            port = rest.slice(colon + 1);
        }
        if (host === '') {
            errors.push(`Invalid broker "${token}": missing host name`);
            continue;
        }
        if (!/^[A-Za-z0-9._-]+$/.test(host)) {
            errors.push(`Invalid broker "${token}": invalid host name`);
            continue;
        }
        if (!/^\d{1,5}$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
            errors.push(`Invalid broker "${token}": port must be a number between 1 and 65535`);
            continue;
        }
        const broker = `${host}:${Number(port)}`;
        if (!brokers.includes(broker)) brokers.push(broker);
        if (schemeName && !schemes.includes(schemeName)) schemes.push(schemeName);
    }
    if (brokers.length === 0 && errors.length === 0) errors.push('No brokers configured');
    return { brokers, errors, schemes };
}

/**
 * Warnings for scheme prefixes (as pasted from a Kafka `bootstrap.servers`
 * setting) that contradict the TLS/SASL settings. The prefixes themselves are
 * ignored, and the mismatch would otherwise only show up as a generic
 * 'connection closed' that is retried forever.
 *
 * @param {string[]} schemes lower-case schemes from parseBrokers()
 * @param {boolean} tls
 * @param {string} mechanism 'none' or a SASL mechanism
 * @returns {string[]}
 */
function schemeWarnings(schemes, tls, mechanism) {
    const warnings = [];
    for (const scheme of schemes) {
        const prefix = `Brokers are given as ${scheme.toUpperCase()}://`;
        const wantsTls = scheme === 'ssl' || scheme === 'sasl_ssl';
        const wantsSasl = scheme === 'sasl_ssl' || scheme === 'sasl_plaintext';
        const plain = scheme === 'plaintext' || scheme === 'sasl_plaintext';
        if (wantsTls && !tls) warnings.push(`${prefix} but "Use TLS" is off`);
        if (plain && tls) warnings.push(`${prefix} but "Use TLS" is on`);
        if (wantsSasl && mechanism === 'none') warnings.push(`${prefix} but no SASL mechanism is selected`);
        if ((scheme === 'ssl' || scheme === 'plaintext') && mechanism !== 'none') {
            const name = (MECHANISMS[mechanism] || mechanism).toUpperCase();
            warnings.push(`${prefix} (no SASL) but SASL ${name} is selected`);
        }
    }
    return warnings;
}

/** Read one numeric property: value, whether the input was unusable (for a warning). */
function numeric(config, key, def, limits, warnings) {
    const raw = config[key];
    const value = toInt(raw, def, limits);
    const blank = raw === undefined || raw === null || (typeof raw === 'string' && raw.trim() === '');
    if (!blank && toInt(raw, undefined) === undefined) {
        const variable = envProblem(raw);
        warnings.push(variable !== null
            ? `${key}: ${variable}; using ${def}`
            : `${key} "${safeString(raw)}" is not a number; using ${def}`);
    }
    return value;
}

/**
 * Parse and validate everything (pure). Used by validateBrokerConfig and
 * buildClientOptions, and by the broker node for its reconnect settings.
 *
 * @param {object} config broker node config (strings from the editor)
 * @param {object} [credentials] { username, password }
 * @param {{tlsNode?: object|null}} [options] pass `tlsNode` (even null) to
 *   validate the tls-config reference; omit it to skip that check
 * @returns {{brokers: string[], clientId: string, mechanism: string, username: (string|undefined),
 *   password: (string|undefined), tls: boolean, tlsConfig: string, connectionTimeout: number,
 *   authenticationTimeout: number, requestTimeout: number, retries: number, initialRetryTime: number,
 *   maxRetryTime: number, reconnect: {min: number, max: number, authMin: number}, logLevelName: string,
 *   safetyNet: boolean, errors: string[], warnings: string[]}}
 */
function normalizeBrokerConfig(config, credentials, options) {
    const c = config && typeof config === 'object' ? config : {};
    const creds = credentials && typeof credentials === 'object' ? credentials : {};
    const errors = [];
    const warnings = [];

    const rawBrokers = c.brokers === undefined || c.brokers === null ? DEFAULTS.brokers : c.brokers;
    const parsed = parseBrokers(rawBrokers);
    errors.push(...parsed.errors);

    let clientId = safeString(c.clientId).trim();
    if (clientId === '') clientId = DEFAULTS.clientId;
    const clientIdVar = envProblem(safeString(c.clientId));
    if (clientIdVar !== null) {
        warnings.push(`clientId: ${clientIdVar}; using "${DEFAULTS.clientId}"`);
        clientId = DEFAULTS.clientId;
    }

    const mechanismInput = safeString(c.authMechanism).trim().toLowerCase();
    const mechanism = mechanismInput === '' ? 'none' : mechanismInput;
    let username;
    let password;
    if (mechanism !== 'none' && !Object.prototype.hasOwnProperty.call(MECHANISMS, mechanism)) {
        errors.push(`Unknown SASL mechanism "${safeString(c.authMechanism)}" (use plain, scram-sha-256 or scram-sha-512)`);
    }
    if (mechanism !== 'none') {
        // Node-RED delivers deleted credentials as undefined, but '' happens too.
        const user = creds.username == null ? '' : safeString(creds.username);
        const pass = creds.password == null ? '' : safeString(creds.password);
        const userVar = credentialEnvProblem(user);
        const passVar = credentialEnvProblem(pass);
        if (user.trim() === '') errors.push('SASL username is required');
        else if (userVar !== null) errors.push(userVar);
        else username = user;
        if (pass === '') errors.push('SASL password is required');
        else if (passVar !== null) errors.push(passVar);
        else password = pass;
    }

    const tls = toBool(c.tls, DEFAULTS.tls);
    const tlsConfig = safeString(c.tlsConfig).trim();
    if (tls && tlsConfig !== '' && options && Object.prototype.hasOwnProperty.call(options, 'tlsNode')) {
        const tlsNode = options.tlsNode;
        if (!tlsNode) errors.push('TLS configuration node not found');
        else if (tlsNode.valid === false) errors.push('TLS configuration is invalid (check its certificate/key files)');
    }
    if (mechanism === 'plain' && !tls) warnings.push('SASL PLAIN without TLS sends the password in clear text');
    warnings.push(...schemeWarnings(parsed.schemes, tls, mechanism));

    const connectionTimeout = numeric(c, 'connectionTimeout', DEFAULTS.connectionTimeout, LIMITS.connectionTimeout, warnings);
    const authenticationTimeout = numeric(c, 'authenticationTimeout', DEFAULTS.authenticationTimeout,
        LIMITS.authenticationTimeout, warnings);
    const requestTimeout = numeric(c, 'requestTimeout', DEFAULTS.requestTimeout, LIMITS.requestTimeout, warnings);
    const retries = numeric(c, 'retries', DEFAULTS.retries, LIMITS.retries, warnings);
    const initialRetryTime = numeric(c, 'initialRetryTime', DEFAULTS.initialRetryTime, LIMITS.initialRetryTime, warnings);
    const maxRetryTime = numeric(c, 'maxRetryTime', DEFAULTS.maxRetryTime, LIMITS.maxRetryTime, warnings);
    const reconnectMin = numeric(c, 'reconnectMin', DEFAULTS.reconnectMin, LIMITS.reconnectMin, warnings);
    // reconnectMax is never below reconnectMin (also when the default applies)
    const reconnectMax = Math.max(reconnectMin, numeric(c, 'reconnectMax', DEFAULTS.reconnectMax,
        { min: Math.max(LIMITS.reconnectMax.min, reconnectMin), max: LIMITS.reconnectMax.max }, warnings));
    const authRetryMin = numeric(c, 'authRetryMin', DEFAULTS.authRetryMin, LIMITS.authRetryMin, warnings);

    let logLevelName = safeString(c.logLevel).trim().toLowerCase();
    if (logLevelName === '') logLevelName = DEFAULTS.logLevel;
    if (!LOG_LEVEL_NAMES.has(logLevelName)) {
        warnings.push(`Unknown log level "${safeString(c.logLevel)}"; using "warn"`);
        logLevelName = 'warn';
    }
    if (logLevelName === 'nothing') logLevelName = 'off';

    return {
        brokers: parsed.brokers,
        clientId,
        mechanism,
        username,
        password,
        tls,
        tlsConfig,
        connectionTimeout,
        authenticationTimeout,
        requestTimeout,
        retries,
        initialRetryTime,
        maxRetryTime,
        reconnect: { min: reconnectMin, max: reconnectMax, authMin: authRetryMin },
        logLevelName,
        safetyNet: toBool(c.safetyNet, DEFAULTS.safetyNet),
        errors,
        warnings
    };
}

/**
 * Validate a broker configuration (pure, never throws).
 *
 * @param {object} config
 * @param {object} [credentials]
 * @param {{tlsNode?: object|null}} [options] see normalizeBrokerConfig
 * @returns {{errors: string[], warnings: string[]}}
 */
function validateBrokerConfig(config, credentials, options) {
    try {
        const n = normalizeBrokerConfig(config, credentials, options);
        return { errors: n.errors, warnings: n.warnings };
    } catch (err) {
        return { errors: [`Invalid configuration: ${err && err.message}`], warnings: [] };
    }
}

/**
 * One-line description, e.g. 'broker1:9092, broker2:9092 · SASL/SCRAM-SHA-256 · TLS'.
 *
 * @param {{brokers: string[], mechanism: string, tls: boolean}} n
 * @returns {string}
 */
function summarize(n) {
    const shown = n.brokers.slice(0, 3).join(', ');
    const more = n.brokers.length > 3 ? ` +${n.brokers.length - 3} more` : '';
    const parts = [n.brokers.length ? shown + more : 'no brokers'];
    if (n.mechanism !== 'none') parts.push(`SASL/${MECHANISMS[n.mechanism] || n.mechanism.toUpperCase()}`);
    if (n.tls) parts.push('TLS');
    return parts.join(' · ');
}

/**
 * Build the options for `new Kafka(options)`. Never throws: problems are
 * returned as `error` (a ConfigError whose `details` lists all of them);
 * `options` is still filled in as far as possible.
 *
 * @param {object} config broker node config
 * @param {object} [credentials] { username, password }
 * @param {object} [extra]
 * @param {object|null} [extra.tlsNode] the resolved tls-config node (if config.tlsConfig is set)
 * @param {Function} [extra.socketFactory]
 * @param {number} [extra.logLevel] kafkajs numeric level (default: from config.logLevel)
 * @param {Function} [extra.logCreator]
 * @returns {{options: object, error: (ConfigError|null), summary: string, warnings: string[]}}
 */
function buildClientOptions(config, credentials, extra) {
    const x = extra && typeof extra === 'object' ? extra : {};
    let n;
    try {
        n = normalizeBrokerConfig(config, credentials, { tlsNode: x.tlsNode === undefined ? null : x.tlsNode });
    } catch (err) {
        const message = `Invalid configuration: ${err && err.message}`;
        return { options: {}, error: new ConfigError(message, [message]), summary: '', warnings: [] };
    }
    const errors = n.errors.slice();

    let ssl = false;
    if (n.tls) {
        ssl = true;
        if (n.tlsConfig !== '' && x.tlsNode && x.tlsNode.valid !== false) {
            try {
                ssl = x.tlsNode.addTLSOptions({});
                if (!ssl || typeof ssl !== 'object') ssl = true;
                else if (typeof ssl.rejectUnauthorized !== 'boolean') {
                    // tls-config copies its checkbox as-is; anything but false means "verify"
                    ssl.rejectUnauthorized = toBool(ssl.rejectUnauthorized, true);
                }
            } catch (err) {
                errors.push(`TLS configuration is invalid (${err && err.message})`);
                ssl = true;
            }
        }
    }

    let logLevel = x.logLevel;
    if (typeof logLevel !== 'number') logLevel = toKafkaLogLevel(n.logLevelName);

    const options = {
        clientId: n.clientId,
        brokers: n.brokers,
        connectionTimeout: n.connectionTimeout,
        authenticationTimeout: n.authenticationTimeout,
        requestTimeout: n.requestTimeout,
        enforceRequestTimeout: true,
        retry: {
            retries: n.retries,
            initialRetryTime: n.initialRetryTime,
            maxRetryTime: n.maxRetryTime,
            factor: 0.2,
            multiplier: 2
        },
        ssl,
        logLevel
    };
    if (n.mechanism !== 'none' && MECHANISMS[n.mechanism]) {
        options.sasl = { mechanism: n.mechanism, username: n.username, password: n.password };
    }
    if (typeof x.socketFactory === 'function') options.socketFactory = x.socketFactory;
    if (typeof x.logCreator === 'function') options.logCreator = x.logCreator;

    const error = errors.length ? new ConfigError(errors.join('; '), errors) : null;
    return { options, error, summary: summarize(n), warnings: n.warnings };
}

module.exports = {
    DEFAULTS,
    LIMITS,
    parseBrokers,
    schemeWarnings,
    normalizeBrokerConfig,
    validateBrokerConfig,
    buildClientOptions,
    summarize
};
