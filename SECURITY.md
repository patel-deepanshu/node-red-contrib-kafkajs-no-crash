# Security Policy

## Supported versions

Security fixes are released for the latest minor version of the current major release. The palette supports
Node.js 18.5 or newer and Node-RED 3.1 or newer; please reproduce problems on a supported combination.

| Version | Supported |
| ------- | --------- |
| 1.x     | ✅        |
| < 1.0   | ❌        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security problems.**

Use GitHub's private vulnerability reporting instead:
**Security → Report a vulnerability** on
<https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/security/advisories/new>.

Please include:

- a description of the issue and its impact,
- steps to reproduce (a minimal flow export with **all secrets removed**),
- the versions of this package, Node-RED and Node.js you used.

You can expect an acknowledgement within 7 days. Once the issue is confirmed, a fix is
prepared privately and released together with a GitHub security advisory that credits
the reporter (unless you prefer to stay anonymous).

## Scope and hardening notes

Things that are in scope include:

- leaking credentials (SASL passwords, TLS keys/passphrases) into flows, logs, debug
  output or the offline-queue files,
- TLS verification being disabled when the configuration says it is enabled,
- path traversal through the offline-queue directory/name settings,
- anything that lets a crafted Kafka message or flow message crash Node-RED (for example a compressed batch: LZ4
  batches that would decompress to more than 256 MiB are rejected as a safeguard),
- the migration script (`kafkajs-nc-migrate-flows`) leaking the credentials it converts (it writes them to a file
  readable by its owner only).

Good practice when running this palette:

- Keep secrets in the broker node's **credentials** (username/password) or in environment
  variables (`${KAFKA_PASSWORD}`); never in plain node properties. Node-RED encrypts
  credentials with `credentialSecret` from `settings.js` — set one.
- Use TLS for anything that leaves the machine. SASL `PLAIN` without TLS sends the
  password in clear text (the node warns about this).
- The offline queue stores **message contents** on disk in plain text under the Node-RED
  user directory. Protect that directory with file-system permissions (and disk
  encryption if the data is sensitive), or switch the producer to the `memory` queue mode.
- Never write credentials into the broker list (`user:password@host`): the broker node refuses
  such an address and does not repeat it in the log, but the flow file would still contain it.
- When you report a problem, remember that debug logs (Node-RED's `logging.console.level: 'debug'`
  and the kafkajs log level `debug`) can contain topic names, broker addresses and message metadata.
