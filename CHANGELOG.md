# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-28

First release. A clean-room rewrite of the ideas of
[node-red-contrib-kafkajs](https://github.com/emrebekar/node-red-contrib-kafkajs) on KafkaJS 2.2.4 (pinned), with
different node types (`kafkajs-nc-*`) so that both palettes can be installed side by side. Requires Node.js 18.5 or
newer and Node-RED 3.1 or newer (tested with 3.1, 4.1 and 5.0). Node-RED 3.0 is not supported: it gives nodes inside
subflows a new id on every start, which would change their default queue directory and consumer group each time.

### Added

- **Broker configuration node** (`kafkajs-nc-broker`): broker list (commas, semicolons or spaces, default port 9092,
  `PLAINTEXT://`-style prefixes accepted, with a warning when a prefix contradicts the TLS/SASL settings;
  credentials written into an address are refused without being logged), client ID, SASL PLAIN, SCRAM-SHA-256 and
  SCRAM-SHA-512 with encrypted credentials, TLS through Node-RED's standard *TLS configuration* node (private CA,
  client certificates, server name, self-signed certificates), connection/authentication/request timeouts, kafkajs
  retries, reconnect backoff, a separate slow retry after authentication and configuration errors, the kafkajs log
  level, and the crash safety net. Every field accepts `${ENV_VAR}` as its whole value (a reference inside a longer
  value, which Node-RED never replaces, is reported as such). All problems of a configuration are reported at once.
- **Producer** (`kafkajs-nc-producer`): any payload (strings, Buffers, objects as JSON, numbers, booleans, dates;
  `null` as a tombstone); keys, partitions, timestamps and headers from the node or from `msg`; static headers;
  batches with `msg.messages`; a delivery report output (`sent` with partition and offset, or `queued`); acks
  all/leader/none, ack timeout, gzip/snappy/lz4 compression, default (Java-compatible) or legacy partitioner, batch
  size, topic auto-creation; control actions `flush`, `reconnect`, `queue-status` and `queue-clear` (matched
  ignoring case and surrounding spaces; any other `msg.action` is data and is produced).
- **Producer retries without silent loss:** kafkajs' own produce retry is switched off, because in kafkajs 2.2.4 a
  retried request can report success without having written the messages of a partition whose leader moved. Kafka
  errors that clear up by themselves (a leader moving, not enough replicas, a request timeout) are retried by the
  node, which resends the whole request up to 3 times (about 0.2, 0.6 and 1.6 s apart) before queueing the messages:
  at worst a duplicate. There is therefore no idempotent mode (kafkajs requires its retry for it); a flow that sets
  `idempotent` loads normally and logs one warning. The first messages to a topic that the broker creates
  automatically are sent (the node waits up to about 2.5 s for the new topic, logged at debug level only). A message
  for a partition added after the connection fetched the topic's metadata is sent again after a quick reconnect
  instead of failing as "does not exist"; a partition confirmed missing is remembered for a minute, so a flow that
  keeps using a partition that does not exist gets `done(err)` at once instead of reconnects and log lines, while
  other partitions of the topic (for example ones added meanwhile) are not affected.
- **Offline queue** (store-and-forward) in the producer: on disk (default), in memory, or off. Messages are stored
  while Kafka is unreachable or slow and sent in their original order after the reconnect (at-least-once). Size and
  message limits, *drop the oldest* or *reject new messages* when full, expiry, a dead-letter file (its rotation logs
  what it deletes; in memory mode the last 100 dead letters are returned by `queue-status`), optional `fsync` per
  write with group commit, crash recovery, and the `kafkajsNcProducerQueueDir` setting for the default location. A
  redeployed producer waits until the previous instance has really closed the queue and never writes the directory at
  the same time. Warnings name queued messages left behind by a changed *Directory* or *Queue name* and queue
  directories with messages that no producer in the deployed flows uses (disabled producers count as using theirs;
  the old directory of a renamed producer, or of one switched to the memory or off mode, is still reported).
  See [docs/OFFLINE-QUEUE.md](docs/OFFLINE-QUEUE.md).
- **Consumer** (`kafkajs-nc-consumer`): output as a UTF-8 string, a Buffer or parsed JSON (keys and header values as
  strings or Buffers); `msg.key`, `msg.headers`, `msg.offset`, `msg.timestamp` and `msg.kafka` (with the high
  watermark); a list of topics or a regular expression; a group ID that stays the same across deploys; read from the
  beginning for new groups; start on deploy or on demand; automatic commits (interval and threshold) or manual
  commits; control actions `pause`, `resume`, `stop`, `start`, `restart`, `seek` and `commit`; session, heartbeat,
  rebalance, fetch size, max wait and concurrency settings. `seek` resolves `earliest`/`latest` to the partition's
  real offsets, refuses offsets outside them and partitions not assigned to this member (kafkajs would otherwise
  jump to, and commit, its default position). `commit` only accepts the `msg.kafka` of this consumer (a producer node
  replaces it with its delivery report) and succeeds only once Kafka confirmed the commit. A subscription without
  partitions shows `connected · no partitions`.
- **Admin node** (`kafkajs-nc-admin`) with 15 actions: `listTopics`, `createTopics`, `deleteTopics`,
  `createPartitions`, `fetchTopicMetadata`, `fetchTopicOffsets`, `fetchTopicOffsetsByTimestamp`, `fetchOffsets`,
  `resetOffsets`, `setOffsets`, `listGroups`, `describeGroups`, `deleteGroups`, `describeCluster` and
  `describeConfigs`, from the node setting or `msg.action`, with a timeout per message counted from its arrival. At
  most 1000 messages wait, and after a failed connection attempt messages fail at once until the reconnect wait has
  passed. `resetOffsets` stores the real earliest/latest offsets (not kafkajs' markers, which would leave the choice
  to the consumers' *Read from the beginning*); `setOffsets` checks partitions and offsets first.
- Reconnects forever with exponential backoff and jitter; clear status texts (`ECONNREFUSED broker:9092 · retry 8s`,
  `auth failed`, `TLS: self-signed cert`, `offline · 57 queued`); one log line per outage instead of hundreds;
  kafkajs log lines in the Node-RED log, labelled with their node and de-duplicated.
- Snappy (snappyjs) and LZ4 compression, pure JavaScript. LZ4 is compressed with lz4js in a Kafka-compatible
  frame format and decompressed by a bounded decoder of this palette that verifies block checksums. Snappy and LZ4
  batches that claim more than their compressed size can hold, or more than 256 MiB, are refused before anything is
  allocated (kafkajs-snappy, used during development, let a 49-byte Snappy batch allocate gigabytes; the same class
  of problem as snappy-java's CVE-2023-43642). Interoperability with Kafka's Java client is tested both ways.
- Nine example flows (*Import → Examples*), detailed help for every node, and the guides
  [ARCHITECTURE](docs/ARCHITECTURE.md), [OFFLINE-QUEUE](docs/OFFLINE-QUEUE.md), [MIGRATION](docs/MIGRATION.md) and
  [TROUBLESHOOTING](docs/TROUBLESHOOTING.md).
- `scripts/migrate-flows.js`, installed as the command `kafkajs-nc-migrate-flows`
  (`npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows flows.json`): converts flows of
  node-red-contrib-kafkajs, including credentials from an unencrypted credentials file, and reports what needs
  attention.
- Crash safety net for unhandled kafkajs promise rejections: they are logged instead of stopping Node-RED. Other
  rejections are handed back to Node.js, which handles them exactly as without this palette; with
  `--unhandled-rejections=strict` the net is not installed and a warning says so.
- Tests: unit tests, node tests against a strict fake of kafkajs, end-to-end tests against Apache Kafka 3.9.1 (SASL,
  compression, broker crashes, freezes and restarts), and a crash trap that fails any test causing an unhandled
  rejection. CI on Node.js 18 to 24 with Node-RED 3.1, 4 and 5.

### Fixed

Problems of node-red-contrib-kafkajs 0.0.7 (the version the palette manager installs) and its unreleased git
version that do not exist in this palette:

- Node-RED exited when the producer's first connection failed: broker down, DNS failure, unreachable address, an
  empty or wrong SASL password, a SASL mechanism mismatch, or TLS against a plain listener. A *catch* node could not
  prevent it, and it also happened after a redeploy had deleted the producer.
- Node-RED exited when the consumer's *Clear offsets* option met a connection failure, and at deploy when the
  heartbeat interval and session timeout were compared as text.
- After a failed first connection the nodes never reconnected; the producer then dropped every message silently.
- Messages produced while Kafka was restarting were lost.
- A redeploy shortly after start left "zombie" consumers in their group, and closed nodes kept retrying and logging.
- A tombstone or a header without a value stopped the consumer for good, at the same offset after every restart.
- Numbers saved as text by the editor broke heartbeats (endless rebalancing) and produced 1 ms timeouts.
- *Acknowledgement: none* waited for answers that never came and wrote each message several times.
- Partition `0` (from the node or `msg.partition`) was ignored.
- Payloads `0`, `""` and `false` were dropped silently; objects, numbers and numeric header values failed with a
  vague error.
- `msg.headers` from *http in* nodes leaked into Kafka headers, or was ignored completely when static headers were
  set.
- The producer's topic auto-creation setting and the client's log level were ignored; kafkajs printed raw JSON to
  stdout.
- An empty group ID created a new random group on every deploy, so messages were skipped or replayed and orphan
  groups piled up on the broker; *Clear offsets* skipped unread messages on every deploy.
- Errors reached *catch* nodes without their cause or the original message; statuses were overwritten with "Idle".
- Topics compressed with Snappy or LZ4 stopped the consumer.
- Each failed reconnect attempt could leak a socket during an outage (a kafkajs behaviour, worked around here).

### Security

- TLS verifies the broker certificate by default. The old palette's *Self Sign* checkbox was inverted, so its
  default did not verify certificates at all.
- The TLS key passphrase is a credential of the *TLS configuration* node instead of plain text in `flows.json`.
- SASL/PLAIN without TLS shows a warning in the editor and in the log.
- An LZ4 batch that would decompress to more than 256 MiB is rejected before anything is allocated. lz4js alone sizes
  its output from the frame's own claims, so a few crafted bytes on a topic could abort Node-RED with an out-of-memory
  error, again after every restart.
- The crash safety net only swallows rejections of kafkajs and of this palette, so bugs of other nodes are not
  hidden; a Node-RED installed under a folder named `kafkajs` does not change that.

[Unreleased]: https://github.com/patel-deepanshu/node-red-contrib-kafkajs-no-crash/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/patel-deepanshu/node-red-contrib-kafkajs-no-crash/releases/tag/v1.0.0
