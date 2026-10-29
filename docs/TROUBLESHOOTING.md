# Troubleshooting

Find the text you see (the node status under the node, a line in the Node-RED log, or `msg.error.message` in a
*catch* node) in the tables below. Each entry says what it means and what to do. The short FAQ in the
[README](../README.md#troubleshooting-faq) covers the most common cases; this page goes further.

- [First steps](#first-steps)
- [Enabling debug logging](#enabling-debug-logging)
- [Status texts](#status-texts)
- [`config error`: what the log says](#config-error-what-the-log-says)
- [Producer: messages that fail](#producer-messages-that-fail)
- [Producer: offline queue messages](#producer-offline-queue-messages)
- [Consumer problems](#consumer-problems)
- [Admin node problems](#admin-node-problems)
- [Common Kafka setup problems](#common-kafka-setup-problems)
- [Other log lines](#other-log-lines)
- [Reporting a bug](#reporting-a-bug)

## First steps

1. **Read the status.** Yellow means the node is retrying by itself (`· retry 8s` says when). Red means a person
   has to change something. Long texts are shortened with `…`; the log always has the full text.
2. **Read the log.** The first failure of an outage is logged as an error with the whole cause chain, for example
   `Kafka connection failed: KafkaJSNumberOfRetriesExceeded: Connection error: connect ECONNREFUSED 127.0.0.1:9092 ←
   caused by KafkaJSConnectionError [ECONNREFUSED] — retrying in 1s`. Repeats of the same failure are only logged at
   debug level, with a warning every 10th attempt, so look for the first one.
3. **Ask the producer.** `msg.action = "queue-status"` returns the connection state, the queue size and the last
   error (`lastError`).
4. **Test from the same machine.** Most connection problems are about addresses, ports and certificates as seen
   *from where Node-RED runs* (inside a container this is not your laptop). Kafka's own command-line tools, or
   `nc -vz <host> <port>` and `openssl s_client -connect <host>:<port>`, run from that machine or container, tell
   you whether the network path works.

## Enabling debug logging

There are two independent switches:

1. **kafkajs messages:** in the broker configuration node, *Advanced → kafkajs log level*: `off`, `error`, `warn`
   (default), `info` or `debug` (very verbose). kafkajs errors and warnings appear as Node-RED warnings, info as
   info, debug as debug, labelled with the node they belong to:
   `[kafkajs:Connection] [kafkajs-nc-producer:my producer] Connection error: connect ECONNREFUSED ... (broker 127.0.0.1:9092)`.
   Identical lines are written at most once a minute, followed later by `(repeated N times)`.
2. **What Node-RED prints:** debug lines (from the nodes and from kafkajs) are only printed when Node-RED's own log
   level allows it. In `settings.js`:

   ```js
   logging: {
       console: {
           level: 'debug',   // default: 'info'
           metrics: false,
           audit: false
       }
   },
   ```

   Restart Node-RED after changing `settings.js`. On a Raspberry Pi installed with the official script the log is
   shown by `node-red-log`; in Docker by `docker logs -f <container>`; with systemd by `journalctl -u <service> -f`.

Set both back afterwards: `debug` writes a lot, and kafkajs debug lines can contain message metadata.

## Status texts

### Retrying by itself (yellow)

| Status | Cause | What to do |
|---|---|---|
| `connecting…`, `joining group…` | Connecting, or (consumer) joining the consumer group. Should take seconds. | If it stays: see the log. A consumer that keeps showing `joining group…` usually waits for other members of its group (see [rebalancing](#consumer-group-rebalancing)). |
| `ECONNREFUSED host:9092 · retry 8s` | Nothing accepts connections at that address: broker down, wrong port, firewall rejecting. | Check the address and port. If `host` is not what you configured, see [advertised.listeners](#the-broker-answers-with-addresses-node-red-cannot-reach-advertisedlisteners). |
| `broker unreachable (DNS) · retry …` | The host name cannot be resolved (`ENOTFOUND`, `EAI_AGAIN`). | Check the spelling, the DNS server, and [names inside Docker](#dns-and-localhost-inside-docker). |
| `connection timeout · retry …` | No answer within *Connect* timeout (10 s): a firewall that drops packets, a wrong IP, a VPN that is down. | Check the network path; raise the timeout only for very slow links. |
| `request timeout · retry …` | Connected, but the broker did not answer a request in time: overloaded, frozen, or a TLS/plaintext mismatch. | Check the broker's health; check that the listener's protocol matches the *Use TLS* setting. |
| `connection closed`, `connection reset` `· retry …` | The broker or something in between closed the connection. Typical for a client without SASL on a SASL port, or plaintext on a TLS port. | See [listener mismatches](#the-security-settings-do-not-match-the-listener). |
| `host unreachable`, `network unreachable` `· retry …` | No route to the host. | Network or VPN configuration. |
| `TLS handshake failed · retry …` | The connection closed during the TLS handshake: usually *Use TLS* on for a plaintext listener. | Use the TLS port, or turn *Use TLS* off. |
| `unknown topic/partition · retry …` | A topic in the node's configuration does not exist (yet), and the broker does not create it. A producer with messages in its queue shows `offline · N queued · retry …` instead. The consumer logs this as a warning, not an error, because the topic is often created a moment later. | Create the topic (admin node `createTopics`, or your Kafka tools), or check its name. The node keeps retrying and continues at its first retry after the topic was created. See [auto-creation](#topics-are-not-created-automatically). |
| `group coordinator not found · retry …` (consumer) | kafkajs found no coordinator for the consumer group. Either it is not available yet (brokers starting or recovering, the `__consumer_offsets` topic offline), or the user may not use the group: kafkajs reports a missing group ACL the same way. Retried with the normal backoff. | Wait a moment. If it stays, grant `READ` on the consumer group and check the brokers' health. |
| `connected · no partitions` (consumer, yellow dot) | Joined the group, but no partition is assigned to this node: the regular expression matches no topic (a warning is logged), or the other members of the group have all partitions (more consumers than partitions). | Check the expression or the topic names; after creating topics send `msg.action = "restart"`. A standby member needs nothing. |
| `offline · 57 queued · retry 8s` (producer) | Same as the lines above, with messages waiting in the offline queue. The reason is in the log and in `queue-status`. | Fix the connection; the queue is sent automatically. |
| `rebalancing…` (consumer) | The consumer group is redistributing partitions. | Normal for a few seconds after any member joins or leaves. If it repeats, see [rebalancing](#consumer-group-rebalancing). |
| `paused · orders` (consumer, yellow dot) | Paused with `msg.action = "pause"`. | Send `resume`. |
| `connected · queue unavailable` (producer, yellow dot) | Connected, but the offline queue could not be opened. The log line `Offline queue unavailable (<dir>): ...` says why. | Check permissions and free space of the directory, an unset `${VAR}`, or a *Queue name* used by two producers. |
| other lowercase texts, e.g. `leader not available`, `not coordinator for group` | A Kafka error code that resolves itself (leader election, broker restart). | Wait; if it persists, check the broker cluster. |

### A person has to act (red)

| Status | Cause | What to do |
|---|---|---|
| `auth failed · retry 60s` | The broker rejected the username or password. | Check the credentials, and the mechanism: SCRAM-SHA-256 and SCRAM-SHA-512 are separate credentials on the broker. Deploy after fixing. |
| `SASL mechanism unsupported · retry …` | The listener does not offer the selected mechanism (`sasl.enabled.mechanisms`). | Choose a mechanism the broker offers. See [SASL mismatches](#the-security-settings-do-not-match-the-listener). |
| `SASL not expected by listener · retry …` | SASL selected for a listener without SASL (`PLAINTEXT` or `SSL`). | Use the SASL port, or set *Mechanism* to *None*. |
| `TLS: self-signed cert`, `TLS: self-signed cert in chain`, `TLS: untrusted cert (CA?)` | The broker's certificate is not signed by a CA that Node-RED trusts. | Add the CA certificate in a *TLS configuration* node (see the [README recipes](../README.md#tls-with-a-private-ca-ssl-listener)). |
| `TLS: hostname mismatch` | The certificate is not valid for the host name used to connect. | See [TLS host name mismatch](#tls-host-name-mismatch). |
| `TLS: cert expired`, `TLS: cert not yet valid` | The certificate's validity period does not include *now*. | Renew the certificate, or check the clock of the Node-RED machine (see [clock skew](#clock-skew)). |
| `TLS: protocol error`, `TLS: wrong version number`, `TLS: alert unknown ca`, ... | Another TLS failure (old TLS versions, cipher mismatch, a client certificate the broker refuses). | Read the log line; compare with `openssl s_client`. |
| `TLS: invalid cert, key or CA · retry …` | The TLS library refused the certificate, key or CA of the *TLS configuration* node: a key that does not belong to the certificate, a damaged or unsupported file, a wrong passphrase. | Check the files of the TLS configuration node (`openssl x509 -noout -modulus` and `openssl rsa -noout -modulus` must match). |
| `not authorized (topic)`, `not authorized (group)`, `not authorized (cluster)` `· retry 60s` | Kafka ACLs: the user may not write/read the topic, use the consumer group, or perform the operation. | Grant the ACLs (`WRITE`/`DESCRIBE` on the topic for producers; `READ` on topic and group for consumers). |
| `compression codec missing · retry 60s` (consumer) | The topic holds zstd-compressed records; only gzip, snappy and lz4 are supported. | Use another compression on the producing side. |
| `broker version unsupported · retry …` | The broker does not support a request kafkajs needs (very old Kafka). | Use Kafka 0.10 or newer; check the protocol of Kafka-compatible services. |
| `config error`, `config error · 3 queued` | The broker configuration is invalid, or kafkajs refused an option. The log says what (next section). | Fix it and deploy. A producer keeps queueing meanwhile. |
| `no broker config` | No broker configuration node is selected. | Select one and deploy. |
| `no topic`, `invalid topic regex` (consumer) | *Topics* is empty or the regular expression is invalid. | Fix the setting. |
| `unknown action` (admin, red from the start) | The node's *Action* is not a valid action name. | Choose an action from the list. |
| `<action>: <reason>`, `<action> timed out` (admin) | The last admin action failed; the same reason went to the *catch* node. | See [admin node problems](#admin-node-problems). |
| `internal error` | A bug in this palette. | Please [report it](#reporting-a-bug) with the log. |

### Normal operation

| Status | Meaning |
|---|---|
| `starting` (grey), `12 queued` (grey) | The node just started; the producer is opening its queue. |
| `connected` (green dot) | Producer: messages are sent directly. |
| `connected · 12 queued` (green dot), `flushing · 1234 queued` (green ring) | Producer: connected and sending the offline queue. Watch the number go down. |
| `connected · orders, payments` (green dot) | Consumer: in its group and consuming. |
| `stopped` (grey) | Consumer: stopped with `msg.action = "stop"`, or *Start consuming on deploy* is off. Send `start`. |
| `ready` (grey), `running createTopics (+2 queued)` (blue), `ok · createTopics` (green) | Admin: idle, working, last action succeeded. |

## `config error`: what the log says

A broken configuration is logged once per node at deploy time: `Invalid Kafka broker configuration: <problems>`.
Nothing tries to connect until it is fixed and deployed again.

| Problem in the log | Fix |
|---|---|
| `No brokers configured` | Enter at least one `host:port`. |
| `Invalid broker "X": port must be a number between 1 and 65535`, `... invalid host name`, `... missing host name`, `... use host:port` | Use `host:port`, separated by commas, semicolons or spaces. |
| `Invalid broker: credentials do not belong in the broker address (found "@"); set them on the Security tab` | Remove `user:password@` from the address (the address is not shown in the log, so the password is not either). Enter the credentials on the *Security* tab. |
| `Invalid broker "X": unsupported scheme "Y://"` | Only `PLAINTEXT://`, `SSL://`, `SASL_SSL://`, `SASL_PLAINTEXT://`, `kafka://` and `tcp://` prefixes are accepted (and ignored). Remove others. |
| `IPv6 literal brokers are not supported by kafkajs (...)` | Use a host name that resolves to the IPv6 address. |
| `environment variable KAFKA_PASSWORD is not set` | The `${KAFKA_PASSWORD}` reference could not be resolved: set the variable for the Node-RED process (systemd unit, Docker `-e`, flow or group environment) and restart or redeploy. |
| `${REGION} is only replaced by Node-RED when it is the whole value` | Node-RED only replaces a reference that makes up the whole field, without spaces around it: `kafka-${REGION}:9092` stays as it is. Put the whole value into the variable (`${KAFKA_BROKERS}`). |
| `SASL username is required`, `SASL password is required` | Enter both on the *Security* tab (credentials are not part of exported or imported flows). |
| `Unknown SASL mechanism "X" (...)` | Choose PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. |
| `TLS configuration node not found` | The selected TLS configuration node was deleted or is in another flow file: select it again. |
| `TLS configuration is invalid (check its certificate/key files)` | The TLS configuration node could not read its files, or has a certificate without a key (or the reverse). Its own error is in the log just before. |
| `Could not create the Kafka client: ...`, `Could not create the Kafka producer: ...` | kafkajs refused an option. The settings are checked before they reach kafkajs, so this points to a bug: please [report it](#reporting-a-bug). |

Warnings that do not stop the node: `SASL PLAIN without TLS sends the password in clear text`;
`Brokers are given as SASL_SSL:// but "Use TLS" is off` (and the other combinations of a broker prefix that contradicts
*Use TLS* or the mechanism: the connection would otherwise just keep closing); `requestTimeout "abc" is not a number;
using 30000`; `connectionTimeout: environment variable X is not set; using 10000`; `Unknown log level "X"; using
"warn"`; producer `Unknown acks setting ...`, `Unknown compression ...`, `The "Idempotent producer" option is no
longer supported and is ignored ...` (a flow saved by an earlier development version; the node retries failed requests
itself, see the README's [Retries](../README.md#acknowledgements-retries-and-compression)); consumer `The heartbeat
interval (X ms) must be lower than the session timeout (Y ms): using Z ms`.

## Producer: messages that fail

These end the message with `done(err)`: a *catch* node receives the original message with `msg.error.message`.
Invalid messages are never queued.

| `msg.error.message` | Cause and fix |
|---|---|
| `No topic: set the node Topic or msg.topic` | Neither is set. |
| `msg.topic: invalid Kafka topic name "sensors/temp" (...)` | Kafka topic names allow letters, digits, `.`, `_` and `-`. Set the node's *Topic*, or rewrite `msg.topic` (e.g. replace `/` with `.`). The same check applies to the node's *Topic* setting (`Topic setting: ...`, logged at deploy). |
| `msg.payload is undefined` | Set a payload; `null` sends a tombstone. |
| `msg.payload has an unsupported type (function)`, `... cannot be converted to JSON: ...` | Functions, symbols and circular objects cannot be sent. |
| `msg.partition must be an integer >= 0 (got ...)` | Use a whole number, or leave it out. Watch out for `msg.partition` left over from a consumer. |
| `Partition 3 does not exist for topic orders (kafkajs wrote nothing for it)` | The topic has fewer partitions. Leave `msg.partition` out, or add partitions (admin `createPartitions`). Right after partitions were added this does not happen: when the producer's information about the topic may be outdated, it reconnects once (debug log `... is not in the producer's metadata ...: reconnecting to fetch the current list`) and sends the message again instead. After that check, further messages for the same missing partition fail at once for one minute; other partitions of the topic are not affected. |
| `msg.timestamp must be a Date or epoch milliseconds (got ...)` | ISO strings are not accepted: use `Date.parse()` first. |
| `msg.headers must be an object` | Headers are `{ name: value }`. |
| `msg.messages is an empty array: nothing to send`, `msg.messages[2].value is undefined`, `msg.messages[0] must be an object like ...` | See [Batches](../README.md#batches-msgmessages). One invalid element rejects the whole input. |
| `Kafka is not connected and the offline queue is off — message not sent` | Expected with *Offline queue: Off*. Also `... the offline queue is unavailable (...)`: the queue could not be opened (see the log). |
| `Offline queue <dir> is full (N messages queued, limit ...); message rejected (overflow policy drop-newest)` | The queue limit is reached and *When full* is *Reject new messages*. |
| `Offline queue <dir>: cannot queue N message(s) of B bytes: larger than the whole queue limit (...)` | One message is larger than *Maximum size*: raise the limit. |
| `Could not store the message in the offline queue: Offline queue: cannot write ...: ENOSPC ...` | The disk is full (or another file-system error). Free space or move the queue. |
| `The request included a message larger than the max message size the server will accept` (`message too large`) | See [message size](#messages-are-too-large). |
| `The Kafka producer node is closing — message not sent`, `The Kafka producer node closed before the message was sent` | The message arrived during a redeploy or shutdown (memory queue or queue off). |
| `Too many messages (10000) are waiting for Kafka and the offline queue is off — message not sent` | Kafka is much slower than the input. Turn the queue on, or slow the input down. |

`queue-clear` fails with `The offline queue is still opening — try again` right after a deploy, and with
`The offline queue is off or unavailable — nothing to clear` without a queue.

## Producer: offline queue messages

| Log line | Meaning |
|---|---|
| `The offline queue holds N message(s) from earlier; they are sent first once Kafka is reachable` | Messages from before a restart or redeploy were found. |
| `Sent N queued message(s) to Kafka; the offline queue is empty` | The backlog is delivered; the node sends directly again. |
| `Stored N unsent message(s) in the offline queue while closing` | Normal on redeploy and shutdown. |
| `Closing: X unsent message(s) stored in the offline queue, Y could not be stored (they failed with an error), Z still being written (slow disk)` | Not everything could be stored while closing: the Y messages failed with `done(err)` (see the errors before it); the Z messages are still being written and are reported when that finishes. |
| `Moved a queued message for topic X that cannot be delivered to <dir>/dead-letter.jsonl: <reason>` | Kafka refused it for good, or it expired or was damaged. See [dead letters](OFFLINE-QUEUE.md#limits-overflow-expiry-and-dead-letters) and [resending them](OFFLINE-QUEUE.md#resending-dead-letters). In memory mode the text ends `to the memory queue's dead letters (see msg.payload.deadLetters of queue-status; lost on restart)`. |
| `Offline queue <dir>: dead-letter.jsonl reached N bytes and replaced dead-letter.1.jsonl: M bytes of older dead letters were deleted` | The dead-letter file was rotated at 50 MB; the previous `.1` file was replaced. Copy the files away regularly if you need to keep them. |
| `Offline queue <dir>: queue full (limit ...): dropped N oldest message(s) to make room` | *Drop the oldest* removed messages. Raise the limit or fix the outage sooner. |
| `Offline queue unavailable (<dir>): <reason> — messages cannot be stored while Kafka is unreachable` | Permissions, a read-only disk, or `Queue directory <dir> is already used by node <id>` (two producers with the same *Queue name*, for example a copied node). |
| `Offline queue unavailable: Queue directory setting: ${HOME} is only replaced by Node-RED when it is the whole value ...`, `... Queue name setting: environment variable X is not set ...`, `... "~pi/queues" is not expanded; use an absolute path ...` | *Directory* or *Queue name* cannot be used as it is, so the queue is unavailable instead of a directory with that literal name being created. Use a whole `${VAR}` that holds the full path, or an absolute path. |
| `Offline queue unavailable (<dir>): Queue directory <dir> is still being closed by node <id> — ...; the queue is opened as soon as that node has closed it`, later `Offline queue <dir> is open now` | After a redeploy the previous instance was still writing the queue after 12 s (a very slow disk). The new instance never writes the directory at the same time: it runs without the queue until the old one is done. |
| `N unsent message(s) are left in the offline queue <dir>, which this node no longer uses (...)` | *Directory* or *Queue name* was changed, or the queue turned off, while messages were queued. Set the old values again to send them. |
| `Found N offline queue(s) under <base> with unsent messages that no producer in the deployed flows uses: <name> (<size>), ...` | Queue directories with messages that no producer of the deployed flows will send: a renamed queue, a deleted node. Producers that are disabled or in a disabled flow are not listed: their queues are sent when they run again. Set a producer's *Queue name* (and *Directory*) to that directory to send them. A directory may also belong to a flow you will import again (or a disabled one whose *Queue name* comes from an environment variable) or to another Node-RED instance using the same directory: delete it only when you are sure no producer needs those messages. Logged once per directory. |
| `Offline queue error while sending queued messages: ... — retrying in ...` | A file-system error while reading the queue. The messages stay queued. |
| `Offline queue <dir>: removed N bytes of an incomplete record at the end of ...` | Repair after a crash during a write; that message had not been reported `queued`. |
| `Offline queue <dir>: cursor file ... is unreadable (...); replaying the queue from the oldest segment (messages may be sent twice)` and similar `cursor ...` warnings | Repair after damage; at-least-once means some messages may be sent twice. |
| `Offline queue <dir>: the file system does not support fsync (...)` | Some network or FUSE file systems; the queue carries on without syncing. Prefer a local disk. |
| `Node removed with N unsent message(s) left in <dir>` | A deleted producer left messages behind. See [orphaned queues](OFFLINE-QUEUE.md#redeploy-rename-copy-and-delete). |
| `Memory queue closed with N unsent message(s); they are discarded` | Expected for *In memory* on redeploy or shutdown. |

## Consumer problems

**Nothing arrives, the status is green.**

- The group already has committed offsets at the end of the topic. *Read from the beginning* only applies to a
  group **without** committed offsets for a partition. See [offsets](#offsets-and-read-from-the-beginning).
- Another member of the same group has the partitions: a second Node-RED instance, a copy of the node with the same
  *Group ID*, the old palette still running, or more consumers than partitions (the extra ones stay idle).
- The consumer is paused (`paused · ...`), or subscribed to a different topic than you produce to.
- A regular expression subscription only sees topics that existed when the consumer started; send
  `msg.action = "restart"` after creating new ones.

**Messages arrive twice.** Expected with *Commit: Manually* when a record was not committed before a restart or
rebalance, and possible after producer-side reconnects (at-least-once delivery). De-duplicate with an id.

**Messages are missing after a crash or restart.** With *Commit: Automatically*, records that were handed to the
flow are committed even if the flow had not finished with them. Use manual commits for at-least-once processing.

**The topic regex consumes nothing (`connected · no partitions`).** The expression is matched against the topics
that exist when the consumer starts. A warning in the log says it matched none. Create the topics, then send
`msg.action = "restart"` (or redeploy).

**A `catch` node gets `Invalid JSON in message orders[0] offset 42: ...`.** *Payload: Parsed JSON* and the record is
not valid JSON. `msg.payload` holds the raw text; the consumer carries on.

**Control messages fail:**

| `msg.error.message` | Fix |
|---|---|
| `msg.action is required: this node only accepts control messages (pause, resume, stop, start, restart, seek, commit)` | The consumer's input is for control messages only. Do not wire data into it. |
| `Unknown msg.action "x": use one of ...` | Check the spelling; it is case-insensitive. |
| `Cannot seek: the Kafka consumer is not running (connecting)`, `Cannot commit: ...` | Wait until the status is green. |
| `Cannot seek: the consumer group is rebalancing; try again when the status shows connected` | Partitions are being reassigned; send the seek again after the rebalance. |
| `seek: partition P of topic "T" is not assigned to this consumer (assigned: T[0,1] ...)` | Another member of the group owns that partition. Seek from the member that has it, or stop the other members. |
| `seek: offset N is outside the offsets of T[P] (low to high); use "earliest", "latest" or an offset in that range` | Kafka would jump to its default position (and store it), so the seek is refused. Older records may have been deleted by retention. |
| `seek: partition P of topic "T" does not exist`, `Cannot seek: looking up the offsets of topic "T" timed out after 15000ms` | Before seeking, the node looks up the partition's offsets with a short admin connection. Check the partition, or Kafka's health. |
| `seek: msg.payload must be an object { topic, partition, offset }`, `seek: msg.payload.partition is required`, `seek: offset must be a whole number >= 0, "earliest" or "latest" (got ...)` | Fix the parameters. |
| `seek: msg.payload.topic is required because the node subscribes to several topics` | Add `topic`. The same applies to `commit` with `msg.kafka.topic`. |
| `commit: msg.kafka.partition is required`, `commit: msg.kafka.offset is required` (or `msg.partition` / `msg.offset` when there is no `msg.kafka`) | Keep `msg.kafka` from the consumer's output when you send the message back with `action: "commit"`. |
| `commit: msg.kafka is not a message of this consumer (it has no groupId). A producer node replaces msg.kafka with its delivery report: ...` | A producer between the consumer and the commit replaced `msg.kafka`. Keep a copy before the producer (`msg.consumed = msg.kafka`) and put it back before the commit (README: [Commit](../README.md#commit)). |
| `commit: msg.kafka is a message of consumer group "X", not of this consumer's group "Y"`, `commit: this consumer does not subscribe to topic "T"` | The message came from another consumer node; send it back to the consumer that produced it. |
| `Commit not stored: the Kafka consumer was stopping. The messages after the last stored position are delivered again when it has reconnected` | The consumer stopped or restarted while committing, and kafkajs skipped the commit. Nothing is lost: the records are delivered again. |

## Admin node problems

Every failure ends the message with `done(err)`; the status shows `<action>: <reason>`.

| `msg.error.message` | Fix |
|---|---|
| `No admin action: set the node's Action or msg.action (one of ...)`, `Unknown admin action "X". Valid actions: ...` | Set the node's *Action*, or `msg.action`. Action names are case-insensitive. |
| `createTopics: msg.payload.topics is required (...)` and similar `... is required` texts | The parameters are missing; the text shows the expected shape. Nothing was sent to Kafka. |
| `Kafka admin fetchOffsets timed out after 30000ms`, `... (12s of it waiting behind earlier messages)` | No answer within the node's *Timeout*, which counts from the message's arrival (waiting behind earlier messages and connecting included). The next message reconnects with a fresh client. |
| `Kafka admin X not run: the last connection attempt failed (<reason>); the next message after 2s connects again` | Kafka could not be reached a moment ago. Messages fail at once until the broker node's reconnect wait has passed (at least *After auth/config errors* for authentication and configuration problems). |
| `Kafka admin X not run: 1000 messages are already waiting (is Kafka reachable?)` | Messages arrive faster than Kafka answers, usually during an outage. Slow the input down. |
| `setOffsets: offset N for partition P of topic "T" is outside its offsets (low to high)`, `setOffsets: partition P does not exist in topic "T" (it has N partitions)`, `setOffsets: partitions[].offset must be a whole number from 0 to 2^63-1 (number or string)` | Offsets must be between the partition's oldest and end offset (see `fetchTopicOffsets`); `-1`/`-2` are not accepted: use `resetOffsets` for the earliest or latest position. |
| `The consumer group must have no running instances, current state: Stable` | `resetOffsets` and `setOffsets` (and `deleteGroups`) need a group without running members: stop its consumers first (`msg.action = "stop"`), everywhere they run. |
| `Invalid Kafka broker configuration: ...`, `No Kafka broker configuration selected` | See [config error](#config-error-what-the-log-says). |

## Common Kafka setup problems

### The broker answers with addresses Node-RED cannot reach (`advertised.listeners`)

A Kafka client connects to one of the configured brokers only to fetch the cluster's metadata. That metadata
contains the address of every broker as the broker advertises it (`advertised.listeners`), and all further
connections go there. If that address is `localhost`, a container name, or an internal IP, a client elsewhere
fails *after* the first connection succeeded.

**Symptoms:** the status shows `ECONNREFUSED localhost:9092`, `broker unreachable (DNS)`, or a host name you never
entered; it may even show `connecting…` and then fail.

**Fix:** set `advertised.listeners` so that every client can reach every address, typically with one listener per
network (for example `INTERNAL://kafka:29092` for containers and `EXTERNAL://my-server.example.com:9092` for
outside clients, plus the matching `listener.security.protocol.map`). In the Brokers field of the broker node, use
the listener meant for where Node-RED runs.

### DNS and `localhost` inside Docker

- Inside a container, `localhost` is the container itself, not the host.
- Container names resolve only for containers on the same user-defined Docker network.
- A broker on the Docker host is `host.docker.internal` with Docker Desktop. On Linux, add
  `--add-host=host.docker.internal:host-gateway` to `docker run` (or `extra_hosts` in Compose).
- `broker unreachable (DNS)` with `EAI_AGAIN` is a temporary DNS failure (no DNS server reachable yet, common right
  after boot): the node retries by itself.

### TLS host name mismatch

`TLS: hostname mismatch` means the broker's certificate does not list the host name used to connect (its subject
alternative names). Remember that, after the first connection, Node-RED connects to the **advertised** host names,
so the certificate must be valid for those.

- Connect with a name the certificate contains (not an IP address, unless the certificate lists that IP).
- Or set *Server Name* in the TLS configuration node to the name in the certificate. It is then used for every
  broker of that configuration.
- Or issue certificates that contain the advertised names.

Unticking *Verify server certificate* also makes the error go away, but then any server can pretend to be your
broker. Use it on test systems only.

### The security settings do not match the listener

Each broker port is one listener with one protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`. The broker
node must match it: *Use TLS* on for `SSL` and `SASL_SSL`, a *Mechanism* for `SASL_PLAINTEXT` and `SASL_SSL`.

| Broker node | Listener | What you see |
|---|---|---|
| TLS on | `PLAINTEXT` or `SASL_PLAINTEXT` | `TLS handshake failed` (or `TLS: protocol error`) |
| TLS off | `SSL` or `SASL_SSL` | typically `connection closed` or `request timeout` |
| SASL | `PLAINTEXT` or `SSL` | `SASL not expected by listener` |
| no SASL | `SASL_PLAINTEXT` or `SASL_SSL` | kafkajs' connect only asks for the API versions, which Kafka answers before authentication: the producer shows `connected` until its first real request is refused, then queues the messages and retries (`offline · N queued · retry …` or `connection closed · retry …`) |
| SASL mechanism not enabled on the listener | | `SASL mechanism unsupported` |
| SCRAM-SHA-256 for a user that only has SCRAM-SHA-512 credentials (or the reverse) | | `auth failed` |

### Topics are not created automatically

Many clusters (Confluent Cloud, Amazon MSK, most production setups) set `auto.create.topics.enable=false`.

- With *Let the broker create missing topics* on (the default), the producer first waits about 2.5 s for the topic
  to appear (a broker that creates topics needs that moment and answers *unknown topic* meanwhile; this is logged at
  debug level only). When the topic is still missing, it gives up as described below and, for one minute, does not
  wait for that topic again. Untick the option if your cluster never creates topics, so that nothing waits.
- A producer whose *Topic* is set keeps the messages in its offline queue and retries until the topic exists. Its
  status is `offline · N queued · retry …`, and the log says `Kafka send failed: ... [UNKNOWN_TOPIC_OR_PARTITION] ...
  (the messages are kept in the offline queue)`. With the queue off, each message fails with `done(err)` and the
  status shows `unknown topic/partition · retry …`.
- A topic taken from `msg.topic` only fails that message: `done(err)` for a live message, or a
  [dead letter](OFFLINE-QUEUE.md#limits-overflow-expiry-and-dead-letters) for a queued one.
- A consumer retries until the topic exists.

Create topics up front with the admin node (`createTopics`) or your Kafka tools. The producer's *Let the broker
create missing topics* only helps when the broker allows auto-creation.

### Consumer group rebalancing

Every time a member joins or leaves a group, Kafka redistributes the partitions (`rebalancing…`). During a
rebalance no member of the group consumes. A few seconds after a deploy is normal. Repeated rebalances mean
members keep coming and going:

- Two or more Node-RED instances, or copies of a flow, use the same *Group ID*. Members of one group share the
  work; each gets only part of the partitions. Give consumers that should all receive every message different
  group IDs.
- Frequent deploys: each one makes the consumer leave and join. *Deploy → Modified Nodes* restarts only what
  changed.
- Members that miss heartbeats: unstable networks, or a *Session timeout* that is too short for them. Raise it in
  the consumer's *Advanced* tab (the broker limits the range).
- A consumer that cannot join because of ACLs shows `not authorized (group)` (red) or `group coordinator not found`
  (yellow, also shown while the coordinator is not available yet) instead.

### Offsets and "Read from the beginning"

Kafka stores, per group and partition, the offset of the next record to read. That is why a consumer continues
where it stopped.

- *Read from the beginning when the group has no stored position* applies **only** when the group has no committed
  offset for a partition, for example a new group. It does not rewind an existing group.
- To read a topic again: `msg.action = "seek"` with `offset: "earliest"` while the consumer runs, or stop the
  consumers and use the admin action `resetOffsets` with `earliest: true`. A new *Group ID* also starts fresh.
- Kafka deletes the committed offsets of a group that has had no members for longer than
  `offsets.retention.minutes` (7 days by default). A consumer that was stopped longer than that starts like a new
  group: from the beginning or from the end, depending on *Read from the beginning*.
- An empty *Group ID* means `node-red-<node id>`. Copying or importing the node creates a new id and therefore a
  new group without offsets.

### Clock skew

The machine's clock matters in three places:

- **Message timestamps.** The producer stamps each message with the time it arrived (unless `msg.timestamp` is
  set). A device that boots with a wrong clock (a Raspberry Pi without a real-time clock before NTP has synced)
  writes wrong timestamps. Kafka's time-based retention may then delete those messages early, and brokers that limit
  timestamp differences reject them with `INVALID_TIMESTAMP`, a permanent error: the message fails with `done(err)`,
  or is moved to the dead-letter file when it came from the queue.
- **Certificates.** `TLS: cert not yet valid` or `TLS: cert expired` can be the local clock rather than the
  certificate.
- **Queue expiry.** *Expire after* compares the arrival time with the current time.

Keep the clock synchronised (NTP), and on devices without a real-time clock start Node-RED only after the time has
been synchronised (with systemd, for example `After=time-sync.target` and `Wants=time-sync.target`, with a service
such as `systemd-time-wait-sync` enabled).

### Messages are too large

`message too large` (`MESSAGE_TOO_LARGE`) and `batch too large` (`RECORD_LIST_TOO_LARGE`) mean the broker refused
the request:

- The limits are the broker's `message.max.bytes` (about 1 MB by default) and the topic's `max.message.bytes`.
- Kafka checks the size of the **record batch**: all messages for one partition in one request, after compression.
  The producer puts up to *Batch size* messages into one request, so many medium-sized messages can exceed the limit
  together. When a request with several input messages is refused, the producer retries them one at a time, and
  only a message that is too large on its own fails. To avoid the extra round trips, lower *Batch size* or enable
  compression.
- A `msg.messages` batch is one input: if it is refused, the whole input fails with `done(err)`.
- Raise `max.message.bytes` on the topic if large messages are intended (consumers on other platforms may need
  matching fetch limits), or split the data.

## Other log lines

| Log line | Meaning |
|---|---|
| `Connected to Kafka after N attempts` | Recovery after an outage. |
| `Kafka still failing after N attempts: ...` | A reminder every 10th failed attempt; the first failure was logged as an error. |
| `Kafka send failed: ... (the messages are kept in the offline queue)`, `Sending queued messages to Kafka failed: ...` | The connection broke while sending, or a Kafka error was still there after 3 retries (a topic that is being created: after about 2.5 s); the node reconnects. When Kafka reported a problem with the topic (missing, not authorised), the line names it: `Kafka send failed (topic X): ...`. Rarely the line ends with `(the messages are kept and sent first after the reconnect)`: newer messages were queued already, so the failed ones wait in memory in front of them. |
| `Kafka consumer failed: ...` | A running consumer stopped (connection loss, group problems); the node creates a new one after the backoff. Hints in parentheses explain the common causes. |
| `Kafka send to topic X failed (<reason>): sending its N message(s) again in 0.2s (retry 1 of 3)` (debug) | A Kafka error that usually clears by itself (a leader moving, a request timeout). The node sends the whole request again; after the 3rd retry the messages go to the offline queue. Duplicates are possible, losses are not. |
| `[kafkajs-nc] Prevented a crash: unhandled KafkaJS error: ...` | The [safety net](ARCHITECTURE.md#the-safety-net-and-why-it-is-narrow) caught an error inside kafkajs that would otherwise have stopped Node-RED. Please report it with the log. |
| `[kafkajs-nc] The crash safety net is not active: with --unhandled-rejections=strict, Node.js stops the process before any listener can handle an unhandled promise rejection.` | Node-RED runs with `--unhandled-rejections=strict` (in `NODE_OPTIONS` or the start command). Remove it to get the safety net back. |
| `Internal error (<where>): ...`, `Could not complete the message: ...` | A bug in this palette, or a message that Node-RED could not clone for a *catch* node. Please report it. |

## Reporting a bug

Open an issue at <https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/issues> with:

- what happened and what you expected;
- the versions of this palette, Node-RED and Node.js, and the platform (Raspberry Pi, Docker, Windows, ...);
- the broker setup (Kafka version or service, listener protocol, SASL mechanism, TLS);
- the relevant flow (*Export → Selected nodes*) with **all secrets removed**;
- the log around the problem, ideally with the kafkajs log level at `debug` and Node-RED's log level at `debug`
  (see [above](#enabling-debug-logging)), again without secrets.

Security problems go to the private channel described in [SECURITY.md](../SECURITY.md), not to public issues.
