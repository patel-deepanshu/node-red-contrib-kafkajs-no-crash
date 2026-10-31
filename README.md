# node-red-contrib-kafkajs-no-crash

[![CI](https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/actions/workflows/ci.yml/badge.svg)](https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/node-red-contrib-kafkajs-no-crash.svg)](https://www.npmjs.com/package/node-red-contrib-kafkajs-no-crash)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Apache Kafka nodes for [Node-RED](https://nodered.org), built on [KafkaJS](https://kafka.js.org) 2.2.4, that **never
crash Node-RED**. Problems such as an unreachable broker, a missing password or a bad certificate show up as a node
status, one clear log line and a message for your *catch* node. The nodes keep reconnecting on their own, and they do
not give up. While Kafka cannot be reached, the producer stores messages in an **offline queue on disk** and sends
them in their original order once the connection is back. The palette has a producer, a consumer, an admin node and a
shared broker configuration, and it supports SASL (PLAIN, SCRAM-SHA-256/512), TLS and mutual TLS.

## Contents

- [Why this palette?](#why-this-palette)
- [Features](#features)
- [Requirements & compatibility](#requirements--compatibility)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Nodes overview](#nodes-overview)
- [Broker configuration](#broker-configuration)
- [Producer](#producer)
- [Offline queue (store-and-forward)](#offline-queue-store-and-forward)
- [Consumer](#consumer)
- [Admin](#admin)
- [Status reference](#status-reference)
- [Error handling & monitoring](#error-handling--monitoring)
- [Examples](#examples)
- [Migrating from node-red-contrib-kafkajs](#migrating-from-node-red-contrib-kafkajs)
- [Troubleshooting FAQ](#troubleshooting-faq)
- [Development](#development)
- [Security](#security)
- [License & credits](#license--credits)

## Why this palette?

Node-RED runs all flows in one process. When a node leaves an asynchronous error unhandled, Node-RED does not just log
it: the process exits, and every other flow on that instance stops with it. We tested [node-red-contrib-kafkajs](https://github.com/emrebekar/node-red-contrib-kafkajs)
version 0.0.7, which is what the palette manager installs, with Node.js 22, Node-RED 4.1 and Kafka 3.9. Anything that
made the producer's first connection fail ended the Node-RED process with exit code 1. That included a stopped
broker, a typo in the host name, an empty or wrong SASL password, and TLS against a plain listener. A *catch* node
could not prevent it. The crash even happened after a redeploy had deleted the producer node, because the old Kafka
client kept retrying in the background. Later, unreleased changes in that project's git repository avoid this
particular crash. With those changes, though, the node stops for good after a failed connection and silently drops
every message after that.

This palette is a clean-room rewrite: no code was copied. Its node types have different names (`kafkajs-nc-*`), so
both palettes can be installed side by side while you migrate.

| Situation | node-red-contrib-kafkajs 0.0.7 (as tested) | This palette |
|---|---|---|
| Kafka unreachable when the flow starts | Node-RED exits | Yellow status with the reason, retries forever, messages are queued |
| SASL selected but the password is empty or wrong | Node-RED exits | Red `config error` (empty: no connection attempts until fixed) or `auth failed · retry 60s` (wrong: slow retries); messages are queued |
| Redeploy while Kafka is down | Node-RED exits, even if the node was deleted | Old clients are shut down within 10 s; nothing keeps retrying in the background |
| Kafka restarted while producing | Messages sent during the outage are lost (200 of 448 in a test with a 2-minute outage) | Stored on disk and sent in order after the reconnect |
| Kafka comes back after a failed first connection | Crash (0.0.7), or no reconnect until the next deploy (git version) | Reconnects automatically |
| A tombstone (record without value) or a header without value is consumed | The consumer stops and fails at the same offset after every restart | `msg.payload = null` / header value `null` |
| Payload `0`, `""`, a number or an object | Dropped silently or rejected | Sent as `"0"`, `""`, text or JSON |
| Something goes wrong | Generic "Kafka Producer Error", the cause is lost | The cause is shown in the status, the log and `msg.error` |
| kafkajs logging | The log level setting is ignored; raw JSON on stdout | Sent to the Node-RED log at the level you choose, with repeated lines suppressed |

The design notes behind these decisions are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Features

- **Never crashes Node-RED.** Every error is handled inside the nodes. A ref-counted *safety net* also catches stray
  kafkajs promise rejections as a second line of defence. Close handlers always finish within 10 seconds.
- **Reconnects forever.** Failed connections are retried with exponential backoff and jitter. Authentication, TLS and
  configuration errors are retried slowly (every 60 s by default), because they rarely fix themselves. A broker
  configuration that is invalid from the start (for example a missing password) is reported once and waits for a fix.
- **Clear errors.** The status text names the problem (`ECONNREFUSED broker:9092 · retry 8s`, `auth failed`,
  `TLS: self-signed cert`). The first failure of an outage is logged once, not hundreds of times. Messages that cannot
  be sent reach *catch* nodes together with the original `msg`.
- **Producer with store-and-forward.** While Kafka is unreachable, messages go to a disk queue, or to a memory queue,
  or you can turn the queue off. They are sent in their original order after the reconnect, with at-least-once
  delivery. Size limits, an overflow policy, expiry and a dead-letter file keep the queue under control.
- **Any payload.** Strings, Buffers, objects (as JSON), numbers, booleans and dates are accepted; `null` sends a
  tombstone. You can set keys, headers (static, or taken from `msg.headers`), partitions and timestamps, and send
  batches with `msg.messages`. Every input gets a delivery report: `sent` with the partition and offset, or
  `queued`.
- **Delivery options:** `acks` all/leader/none, gzip/snappy/lz4 compression, and the default or legacy partitioner.
  Kafka errors that clear up by themselves (a partition leader moving, a request timeout) are retried by resending
  the whole request, so a message is written at least once and never dropped silently.
- **Consumer.** Output as a UTF-8 string, a Buffer or parsed JSON. Subscribe to a list of topics or to a regular
  expression. The group ID stays the same across restarts. Commit automatically or manually. Control messages
  `pause`, `resume`, `stop`, `start`, `restart`, `seek` and `commit` steer the consumer.
- **Admin node** with 15 actions: topics, partitions, offsets, consumer groups, the cluster and configs.
- **Security:** SASL PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512, plus TLS through Node-RED's standard *TLS configuration*
  node (private CA, client certificates, self-signed certificates). Credentials are stored encrypted, and every field
  accepts `${ENV_VAR}`.
- **No ghost clients.** Every connection attempt uses a fresh kafkajs client. A late connect after a redeploy is shut
  down instead of living on as a hidden consumer group member.
- **Pure JavaScript dependencies** (kafkajs, snappyjs, lz4js), so nothing has to be compiled on a Raspberry Pi or
  in Alpine containers.

## Requirements & compatibility

| | |
|---|---|
| Node.js | 18.5 or newer. CI runs Node.js 18, 20, 22 and 24. |
| Node-RED | 3.1 or newer. Tested with 3.1, 4.1 and 5.0 (Node-RED 5 itself needs Node.js 22.9 or newer). Node-RED 3.0 is not supported: it gives the nodes inside a subflow a new id on every start, which would change their default queue directory and consumer group each time. |
| Kafka | Any broker KafkaJS 2.2.4 can talk to (KafkaJS states Kafka 0.10+). The end-to-end tests run against Apache Kafka 3.9.1 in KRaft mode. Kafka-compatible services such as Redpanda, Confluent Cloud, Amazon MSK or the Azure Event Hubs Kafka endpoint can work too; see the [recipes](#cloud-services). |
| Authentication | None, SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, mutual TLS. **Not supported:** OAUTHBEARER, AWS IAM (MSK IAM authentication), Kerberos/GSSAPI. |
| Compression | gzip, snappy and lz4, for both producing and consuming. **zstd is not supported.** |
| Not included | Transactions / exactly-once, the idempotent producer (see [Retries](#acknowledgements-retries-and-compression)), schema registry (Avro/Protobuf: consume as a Buffer and decode in a function node), IPv6 literal broker addresses (use a host name). |

## Installation

### Node-RED palette manager

In the Node-RED editor open **Menu → Manage palette → Install**, search for `node-red-contrib-kafkajs-no-crash` and
click **install**. The nodes appear in the **kafka** category of the palette.

### npm

Run this in your Node-RED user directory (usually `~/.node-red`), then restart Node-RED:

```sh
cd ~/.node-red
npm install node-red-contrib-kafkajs-no-crash
```

### Docker (official `nodered/node-red` image)

Either install the palette into a running container (the user directory of the image is `/data`):

```sh
docker exec -it mynodered bash -c "cd /data && npm install node-red-contrib-kafkajs-no-crash"
docker restart mynodered
```

or build your own image:

```dockerfile
FROM nodered/node-red:latest
RUN npm install --no-fund --no-update-notifier node-red-contrib-kafkajs-no-crash
```

The disk queue lives in the user directory (`/data/kafkajs-nc-queue` in the image). Keep `/data` on a volume
(`-v node_red_data:/data`), otherwise queued messages are lost when the container is recreated. Inside a container,
`localhost` means the container itself: use the broker's host name or IP address. With Docker Desktop, a broker on
the host is reachable as `host.docker.internal`.

### From source (contributors)

```sh
git clone https://github.com/patel-deepanshu/node-red-kafkajs-no-crash.git
cd node-red-kafkajs-no-crash
npm install

# link the working copy into your Node-RED user directory, then restart Node-RED
cd ~/.node-red
npm install /path/to/node-red-kafkajs-no-crash
```

See [Development](#development) for the tests and [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow.

## Quick start

You need a Kafka broker. For a local test broker, run the official Apache Kafka image:
`docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1`. It listens on `localhost:9092` without security.

1. **Install** the palette (see [Installation](#installation)).
2. **Add a producer.** Drag an *inject* node, a **kafka producer** and a *debug* node into a flow and wire them in that
   order. Open the producer and click the pencil next to **Broker**. **Brokers** already says `localhost:9092`, so
   click **Add**. Set the producer's **Topic** to `demo` and leave everything else at its default.
3. **Add a consumer.** Drag a **kafka consumer** and a *debug* node into the flow and wire them. Select the same broker,
   set **Topics** to `demo` and tick **Read from the beginning when the group has no stored position**. Click
   **Deploy**.
4. **Send something.** Click the inject button. The producer's debug output shows `msg.kafka.status: "sent"` with the
   partition and offset. The producer creates the topic on first use if the broker allows automatic topic creation,
   which is Kafka's default: it waits the moment the broker needs for that, so even the first message is reported
   `sent`. Until the topic exists, the consumer shows `unknown topic/partition · retry …` and keeps trying. At its next
   retry (within a minute at most) it joins and prints the message.
5. **Watch it survive an outage.** Run `docker stop kafka` and click inject a few times. The producer reports
   `msg.kafka.status: "queued"` (the first report can take a few seconds while kafkajs retries) and shows
   `offline · 3 queued · retry …`. Run `docker start kafka`. The queued messages are sent in order, and the consumer
   receives them.

Ready-made flows for every feature are in [`examples/`](examples). In Node-RED they are under **Menu → Import →
Examples → node-red-contrib-kafkajs-no-crash**.

## Nodes overview

| Node (palette label) | Type | Input | Output |
|---|---|---|---|
| **kafka broker** (configuration node) | `kafkajs-nc-broker` | – | – |
| **kafka producer** | `kafkajs-nc-producer` | A message to send, or a control message (`msg.action`) | A delivery report: the input `msg` with `msg.kafka` |
| **kafka consumer** | `kafkajs-nc-consumer` | Control messages only (`msg.action`) | One message per Kafka record |
| **kafka admin** | `kafkajs-nc-admin` | An action and its parameters | The input `msg` with the result in `msg.payload` |

The broker node holds the connection settings: brokers, security, timeouts and reconnect behaviour. It does not open a
connection itself. Every producer, consumer and admin node that uses it opens its own connection.

## Broker configuration

### Settings

The editor has three tabs. The *Property* column is the name in the flow JSON.

| Tab | Setting | Property | Default | Description |
|---|---|---|---|---|
| – | Name | `name` | | Label shown in the editor. |
| Connection | Brokers | `brokers` | `localhost:9092` | `host:port` list, separated by commas, semicolons or spaces. Port 9092 is used when none is given. Prefixes such as `PLAINTEXT://`, `SSL://`, `SASL_SSL://` or `SASL_PLAINTEXT://` are accepted, but security is set on the *Security* tab: a prefix that contradicts it (for example `SASL_SSL://` with *Use TLS* off) logs a warning. Credentials (`user:password@host`) are refused here. IPv6 literals are not supported by kafkajs. |
| Connection | Client ID | `clientId` | `node-red` | Sent with every request. Visible in broker logs and used for quotas. |
| Security | Mechanism | `authMechanism` | `none` | `none`, `plain`, `scram-sha-256` or `scram-sha-512`. |
| Security | Username / Password | credentials | | Both are required for SASL. They are stored encrypted in the credentials file, never in the flow. |
| Security | Use TLS | `tls` | off | Encrypts the connection. Needed for `SSL` and `SASL_SSL` listeners. |
| Security | TLS configuration | `tlsConfig` | none | Optional Node-RED *TLS configuration* node, for a private CA, a client certificate, a different server name or a self-signed certificate. Without one, the system CA certificates are trusted and the server certificate is verified. |
| Advanced | Connect | `connectionTimeout` | 10000 ms | Time to open the TCP/TLS connection (100 – 300000). |
| Advanced | Authentication | `authenticationTimeout` | 10000 ms | Time for the SASL handshake (100 – 300000). |
| Advanced | Request | `requestTimeout` | 30000 ms | Time to wait for each broker answer (1000 – 600000). |
| Advanced | Retries | `retries` | 5 | How often kafkajs itself retries one operation, such as connecting (0 – 50). Keep this small, because the nodes reconnect on their own afterwards. Produce requests are not retried by kafkajs: the producer resends them itself (see [Retries](#acknowledgements-retries-and-compression)). |
| Advanced | First retry after | `initialRetryTime` | 300 ms | First kafkajs retry delay (50 – 60000). |
| Advanced | Longest retry wait | `maxRetryTime` | 30000 ms | Longest kafkajs retry delay (100 – 600000). |
| Advanced | Reconnect: first wait | `reconnectMin` | 1000 ms | After a failed or lost connection, the node waits this long, then doubles the wait after each failure (±20 % jitter) (100 – 600000). |
| Advanced | Reconnect: longest wait | `reconnectMax` | 60000 ms | Upper limit of that wait (100 – 3600000; never below the first wait). |
| Advanced | After auth/config errors | `authRetryMin` | 60000 ms | Minimum wait after authentication, authorisation, TLS or configuration errors (1000 – 3600000). This avoids account lock-outs and log spam. |
| Advanced | kafkajs log level | `logLevel` | `warn` | `off`, `error`, `warn`, `info` or `debug`. See [Logs](#logs). |
| Advanced | Crash safety net | `safetyNet` | on | See [Safety net](#safety-net). |

Numbers outside their range are clamped to it. A value that is not a number falls back to the default and logs a
warning. The editor marks invalid values before you deploy.

### Environment variables

Any field, including the username and password, can be `${NAME}` to read the environment variable `NAME`, for
example Brokers `${KAFKA_BROKERS}`, Username `${KAFKA_USER}` and Password `${KAFKA_PASSWORD}`. Node-RED only replaces a
reference that makes up the **whole** field. For nodes inside a flow, environment variables defined on that flow or
its group in the editor work as well.

```sh
KAFKA_BROKERS="broker1:9093,broker2:9093" KAFKA_USER="node-red" KAFKA_PASSWORD="<secret>" node-red
# or with Docker:
docker run -e KAFKA_BROKERS=broker1:9093 -e KAFKA_USER=node-red -e KAFKA_PASSWORD=<secret> ... nodered/node-red
```

If a variable used in Brokers, Username or Password is not set, the configuration is invalid
(`environment variable KAFKA_PASSWORD is not set`). If one used in a numeric field is not set, the default is used and a
warning is logged. A reference inside a longer value, such as `kafka-${REGION}:9092`, is never replaced by Node-RED;
it is reported as `${REGION} is only replaced by Node-RED when it is the whole value`, and the editor marks it before
you deploy.

### When the configuration is broken (for example credentials missing)

Missing credentials were the crash that started this project. Here they are handled like any other configuration
problem:

- At deploy time the broker node logs every problem at once, for example
  `Invalid Kafka broker configuration: SASL password is required`. The editor already flags a missing username. A
  password cannot be checked in the editor.
- Every node that uses this broker shows a red **`config error`** status and logs the same reason. None of them
  tries to connect until you fix the configuration and deploy again.
- The **producer** keeps accepting messages and queues them (`config error · 3 queued`). They are sent after the fix.
  With the queue off, each message fails with `done(err)` instead.
- The **consumer** does not start. Control messages fail with the reason.
- The **admin** node fails each message with the reason, which goes to your *catch* node.

Other problems reported the same way: no brokers, an invalid `host:port`, credentials written into a broker address,
an IPv6 literal, an unknown mechanism, an unset environment variable, a TLS configuration node that is missing or
whose certificate/key files cannot be read. A certificate, key or CA that the TLS library refuses (for example a key
that does not belong to the certificate) shows `TLS: invalid cert, key or CA` and is retried slowly. SASL/PLAIN
without TLS is allowed but logs a warning, because the password then travels in clear text, and so is a broker prefix
that contradicts the security settings (`Brokers are given as SASL_SSL:// but "Use TLS" is off`).

### Recipes

#### Local broker without security (`PLAINTEXT`)

Brokers `localhost:9092`, Mechanism *None*, *Use TLS* off.

#### SASL/PLAIN or SCRAM on a `SASL_PLAINTEXT` listener

Mechanism *SASL/PLAIN*, *SASL/SCRAM-SHA-256* or *SASL/SCRAM-SHA-512*, matching how the user was created on the broker.
Enter the username and password, leave *Use TLS* off. Use this only on a trusted network: PLAIN sends the password in
clear text, and the data is never encrypted.

#### SASL over TLS (`SASL_SSL`)

As above, with *Use TLS* on. If the broker certificate comes from a public CA, no TLS configuration node is needed.

#### TLS with a private CA (`SSL` listener)

1. Turn *Use TLS* on, open the **TLS configuration** list and choose *Add new tls-config…*.
2. In the TLS configuration node, set **CA Certificate** to your CA file (`ca.pem`, upload it or point to a local file)
   and keep **Verify server certificate** ticked.
3. Mechanism *None*, or a SASL mechanism for `SASL_SSL`.

If you connect by IP address, through a tunnel, or with a name that is not in the certificate, set **Server Name** in
the TLS configuration node to the name the certificate was issued for. When the broker address is a host name, it is
sent as the server name automatically.

#### Mutual TLS (client certificate)

In the TLS configuration node set **Certificate** (client certificate), **Private Key**, the **Passphrase** if the key
is encrypted, and the **CA Certificate**. Leave Mechanism on *None*: the certificate is the identity.

#### Self-signed broker certificate

Preferred: put the self-signed certificate (or the CA that signed it) into **CA Certificate**, so the connection is
still verified. For a test system only, you can untick **Verify server certificate** instead. The status
`TLS: self-signed cert · retry 60s` tells you that one of the two is needed.

#### Cloud services

These settings follow each provider's published client settings. The underlying mechanisms (PLAIN, SCRAM, TLS, mutual
TLS) are covered by this project's tests, but the services themselves are not. Check your provider's documentation
for current host names and ports.

| Service | Brokers | Mechanism | Username / Password | TLS |
|---|---|---|---|---|
| Confluent Cloud | Bootstrap server, e.g. `pkc-xxxxx.<region>.<cloud>.confluent.cloud:9092` | SASL/PLAIN | API key / API secret | On, no TLS configuration |
| Redpanda Cloud | Bootstrap server URL from the console | SCRAM-SHA-256 (or -512, as created) | User / password | On, no TLS configuration |
| Aiven for Apache Kafka, SASL | Service host with the **SASL** port | SCRAM-SHA-256 | Service user / password | On, TLS configuration with the project CA (`ca.pem`) |
| Aiven for Apache Kafka, client certificate | Service host with the default TLS port | None | – | On, TLS configuration with `ca.pem`, `service.cert`, `service.key` |
| Azure Event Hubs (Kafka endpoint) | `<namespace>.servicebus.windows.net:9093` | SASL/PLAIN | `$ConnectionString` (literally) / the namespace connection string `Endpoint=sb://…` | On, no TLS configuration |
| Amazon MSK, SASL/SCRAM | Bootstrap brokers on port 9096 | SCRAM-SHA-512 | User / password from AWS Secrets Manager | On, no TLS configuration |
| Amazon MSK, mutual TLS | Bootstrap brokers on port 9094 | None | – | On, TLS configuration with your client certificate and key |

A `SASL_SSL://` prefix copied from a provider's console is fine, but it does not configure anything: you still have
to select the mechanism and turn on TLS (the broker node logs a warning when they do not match the prefix).
`$ConnectionString` is not an environment variable reference: Node-RED only treats `${NAME}` and `$(NAME)` that
way. A recommended pattern is Brokers `${KAFKA_BROKERS}`, Username `${KAFKA_USER}` and Password `${KAFKA_PASSWORD}`,
with the values kept outside the flow.

## Producer

The producer sends `msg.payload` (or a batch in `msg.messages`) to Kafka and passes the input message on as a
*delivery report* once Kafka has acknowledged it, or once it is safely in the offline queue.

### Input

| Property | Type | Notes |
|---|---|---|
| `msg.payload` | anything except `undefined` | The message value. See [How values are encoded](#how-values-are-encoded). Ignored when `msg.messages` is an array. |
| `msg.topic` | string | Used when the node's **Topic** is empty. |
| `msg.key` | string, Buffer, number, object | Used when the node's **Key** is empty. Messages with the same key go to the same partition, which keeps their order. |
| `msg.partition` | integer ≥ 0 | Used when the node's **Partition** is empty. Without it, the partitioner chooses (from the key, if there is one). |
| `msg.headers` | object (or `Map`) | Kafka headers, `{ name: value }`. A value may be an array for a header that repeats. Only used when **Also send msg.headers** is ticked (the default). Static headers win on equal names. |
| `msg.timestamp` | number (epoch ms), `Date`, or a string of digits | The Kafka timestamp. Defaults to the time the message reached the node, so messages sent later from the queue keep their original time. ISO date strings are rejected: use `Date.parse()` first. |
| `msg.messages` | array | A batch: see [Batches](#batches-msgmessages). |
| `msg.action` | string | `flush`, `reconnect`, `queue-status` or `queue-clear` make the message a [control action](#control-actions). Any other value is ordinary data: the message is sent. |

**Precedence:** a Topic, Key or Partition set in the node is used for every message, and the matching `msg` property
is ignored. Leave the field empty to take the value from the message. Headers are merged: `msg.headers` (if enabled)
plus the node's static headers, and a static header wins when both have the same name.

**Topic names** may contain letters, digits, `.`, `_` and `-` (at most 249 characters). An MQTT-style topic such as
`sensors/temp` is rejected: set the node's Topic, or change `msg.topic` first.

#### How values are encoded

| JavaScript value | Sent to Kafka as |
|---|---|
| string | UTF-8 text, as is |
| Buffer, typed array (`Uint8Array`, ...), `ArrayBuffer`, `DataView` | the bytes, unchanged |
| object or array | JSON text |
| number, boolean, bigint | text, e.g. `42`, `true` |
| `Date` | ISO 8601 text |
| `null` | no value: a **tombstone**, which deletes the key on compacted topics |
| `undefined`, a function, a symbol, a circular object | not sent: the message fails with `done(err)` |

Keys follow the same rules, except that `undefined`, `null` and `""` mean "no key". Header values follow them too:
`null`/`undefined` header values are left out, and an array value sends the header once per element.

#### Batches (`msg.messages`)

Set `msg.messages` to an array to send several Kafka messages from one input message:

```js
msg.messages = [
    { key: "a", value: { n: 1 } },
    { key: "b", value: { n: 2 }, headers: { source: "line-3" } },
    { topic: "audit", value: "batch sent" }
];
return msg;
```

- Each element is `{ value, key, headers, partition, timestamp, topic }`. `payload` is accepted instead of `value`.
  `msg.payload` is ignored.
- An element without `topic` uses the node's Topic or `msg.topic`. The other `msg` properties (`key`, `headers`,
  `partition`, `timestamp`) do **not** apply to the elements. Node settings still win, and element `headers` are only
  used when *Also send msg.headers* is ticked.
- Every element is checked before anything is sent: one invalid element rejects the whole input.
- The input gets one delivery report and one `done()`. The elements are sent in order, split into requests of at
  most *Batch size* messages and one topic each. If Kafka refuses part of the batch for good (for example *message
  too large*), the input fails with `done(err)`. Parts that were already written stay written.

#### Control actions

Send a message whose `msg.action` is one of these values. Upper or lower case and spaces around the value do not
matter (`" Queue-Status"` works too). These four values are reserved: a message carrying one of them is never sent
to Kafka. Any other `msg.action` value is ordinary data, and the message is sent to Kafka normally.

| `msg.action` | What it does | Output |
|---|---|---|
| `flush` | Retry the connection now instead of waiting for the backoff, or start sending the queue now. | none |
| `reconnect` | Same as `flush`. | none |
| `queue-status` | Report the queue and connection state. | `msg.payload` = statistics (see [Monitoring](#monitoring-the-queue)) |
| `queue-clear` | Delete every queued message without sending it (logged as a warning). Fails if the queue is off or not open yet. | `msg.payload` = statistics |

### Output: delivery report

The input message is sent on with `msg.kafka` added. An existing `msg.kafka` is replaced, including the one a consumer
set: keep a copy if you commit manually after producing (see [Commit](#commit)).

| Property | `sent` | `queued` | Meaning |
|---|---|---|---|
| `msg.kafka.status` | `"sent"` | `"queued"` | `sent`: Kafka acknowledged the message. `queued`: stored in the offline queue. It will be sent later, and no second report follows. |
| `msg.kafka.topic` | ✓ | ✓ | The topic (for a batch: the topic of its first message). |
| `msg.kafka.partition` | when known | – | The partition. |
| `msg.kafka.offset` | when known exactly | – | The offset, as a string. |
| `msg.kafka.results` | ✓ | – | kafkajs' answer for the request(s): `[{ topicName, partition, baseOffset, ... }]`. Empty with *Acknowledgement: none*. |
| `msg.kafka.count` | batches | batches | Number of Kafka messages, when the input carried more than one. |
| `msg.kafka.queueSize` | – | ✓ | Messages in the queue after this one was added. |

Messages that cannot be sent end with `done(err)`. A *catch* node then receives the original message with
`msg.error`. This happens for a missing or invalid topic, an `undefined` payload, a partition that does not exist, a
message too large for the broker, a full queue with *Reject new messages*, or Kafka unreachable with the queue off.

A partition counts as missing only when the producer's list of the topic's partitions is current: its connection is
less than 10 s old, or it reconnected to fetch the list because of exactly this partition. kafkajs keeps that list for
up to 5 minutes: after partitions were added, a message for a new partition may not be written at first. The producer
then reconnects at once, which fetches the current list, and sends that message again (logged at debug level; messages
arriving meanwhile wait behind it). A partition confirmed missing this way is remembered for one minute: a flow that
keeps sending to a partition that does not exist causes at most one such reconnect per partition per minute, and the
other messages for it fail at once. Other partitions of the topic are not affected, so a partition that was really
added is always used.

### Settings

| Tab | Setting | Property | Default | Description |
|---|---|---|---|---|
| – | Broker | `broker` | | The broker configuration node (required). |
| – | Name | `name` | | Label shown in the editor. |
| Message | Topic | `topic` | empty | Empty: use `msg.topic`. |
| Message | Key | `key` | empty | Empty: use `msg.key`. |
| Message | Partition | `partition` | empty | Empty: `msg.partition`, or the partitioner. |
| Message | Static headers | `headers` | none | Name/value list. A name used twice becomes a repeated header. |
| Message | Also send msg.headers | `useMsgHeaders` | on | Untick it if `msg.headers` holds something else, for example the HTTP headers that *http in* and *http request* nodes put there. |
| Delivery | Acknowledgement | `acks` | `all` | `all` (acks=-1, all in-sync replicas), `leader` (acks=1) or `none` (acks=0, fire and forget). |
| Delivery | Ack timeout | `ackTimeout` | 30000 ms | How long the broker may wait for the replicas (100 – 600000). |
| Delivery | Compression | `compression` | `none` | `none`, `gzip`, `snappy` or `lz4`. |
| Delivery | Partitioner | `partitioner` | `default` | `default` (same key-to-partition mapping as the Java client) or `legacy` (kafkajs 1.x mapping). |
| Delivery | Batch size | `batchSize` | 100 | At most this many messages per produce request (1 – 10000). Used both for messages that pile up while a request is in flight and for sending the queue. |
| Delivery | Let the broker create missing topics | `allowAutoTopicCreation` | on | Only has an effect if the broker allows it (`auto.create.topics.enable`). While on, the node waits up to about 2.5 s for a new topic to be created (see *New topics* below). |
| Offline queue | Offline queue | `queueMode` | `disk` | `disk`, `memory` or `off`. See [Offline queue](#offline-queue-store-and-forward). |
| Offline queue | Directory | `queueDir` | empty | Empty: `kafkajsNcProducerQueueDir` from `settings.js`, else `kafkajs-nc-queue`. Relative paths are relative to the Node-RED user directory. Disk mode only. |
| Offline queue | Queue name | `queueName` | empty | Empty: the node's id. Disk mode only. |
| Offline queue | Maximum size | `queueMaxMB` | 100 MB | 0 = unlimited. Fractions are allowed. |
| Offline queue | Maximum messages | `queueMaxMessages` | 0 | 0 = unlimited. |
| Offline queue | When full | `queueOverflow` | `drop-oldest` | `drop-oldest` (remove the oldest queued messages, with a warning) or `drop-newest` (reject the new message with `done(err)`). |
| Offline queue | Expire after | `queueMaxAgeHours` | 0 | Queued messages older than this many hours are moved to the dead-letter file instead of being sent. 0 = never. |
| Offline queue | Sync every write to disk | `queueFsync` | on | A message is only reported `queued` once it is on disk, even if the power fails. Disk mode only. |

Any setting can be `${NAME}`; Node-RED only replaces it when it is the whole field. If a variable used in *Topic*,
*Key*, *Partition* or the static headers is not set, every message fails with a clear error, so nothing is sent to an
unintended topic. In *Directory* or *Queue name*, an unset variable, or a `${NAME}` inside a longer path such as
`${HOME}/queues`, makes the offline queue unavailable (logged at deploy) instead of creating a directory with that
literal name. In *Acknowledgement*, *Compression* and *Offline queue* a warning is logged and the default is used; in
the other settings the default is used.

Flows saved by a development version before 1.0 may still contain `idempotent: true`. The option no longer exists:
it is ignored, and the node logs one warning at deploy (see [Retries](#acknowledgements-retries-and-compression) for
why).

### Acknowledgements, retries and compression

- **acks.** `all` is the safe default: Kafka confirms only after every in-sync replica has the message. `leader` is
  faster but can lose a message if the leader fails right after answering. `none` never waits for an answer, so the
  report says `sent` as soon as the request went out, and `msg.kafka.results` is empty.
- **Retries.** Kafka errors that usually clear up within a second or two (a partition leader moving during a broker
  restart or a leader rebalance, `NOT_ENOUGH_REPLICAS`, a request timeout) are retried by the node: it sends the
  **whole** request again, up to 3 times, after pauses of about 0.2, 0.6 and 1.6 s (logged at debug level). If it
  still fails, the messages go to the offline queue (or fail with `done(err)` with the queue off) and the node
  reconnects. kafkajs' own produce retry is switched off: in kafkajs 2.2.4 a retried request can report success
  without having written the messages for partitions whose leader moved to a broker that had already answered, and
  nothing reports the loss. Resending the whole request can only cause a duplicate, never a silent loss. The broker
  node's *Retries* setting still applies to connecting.
- **New topics.** While a broker creates a topic automatically it answers *unknown topic* for a moment (typically
  0.1 to 1 s). With *Let the broker create missing topics* on, the node sends the request again after 0.05, 0.1, 0.2,
  0.4, 0.8 and 0.8 s (debug log only), so the first messages to a new topic are reported `sent`. If the topic still
  does not exist after that (about 2.5 s), the broker does not create topics: the message fails (`msg.topic`) or is
  queued (the node's *Topic*), and for one minute further messages for that topic do not wait again. With the option
  off, *unknown topic* is not waited for. All pauses of one request together never exceed 5 s.
- **No idempotent producer.** kafkajs' idempotent mode only works with its own produce retry switched on, which is
  exactly the retry that can lose messages, so this palette does not offer it. Earlier development versions had an
  *Idempotent producer* option; a flow that still sets it loads normally, the option is ignored, and a warning says
  so. De-duplicate on the consuming side instead (see below).
- **Compression.** gzip is built into kafkajs. Snappy (through snappyjs) and LZ4 (compressed through lz4js) are
  registered automatically, and both are pure JavaScript. Both are decompressed with size checks: a batch that claims
  more than its compressed size can hold, or more than 256 MiB, is refused before any memory is allocated, so a
  crafted batch cannot exhaust the memory of Node-RED. The end-to-end tests check that Kafka's own Java client can
  read what this palette writes and that this palette reads what the Java client writes, for all three. zstd is not
  available.

### Ordering and delivery guarantees

- **Order.** Each producer node sends one request at a time, in the order the messages arrived. That includes
  messages that went through the offline queue: older queued messages are always sent before newer ones. Kafka keeps
  the order **per partition**, so give messages whose order matters the same key (or a fixed partition). Each request
  carries only one topic, so the order across topics is kept as well. Separate producer nodes are independent of each
  other.
- **At least once.** `sent` means Kafka acknowledged the message. `queued` means it is stored (on disk when
  *Sync every write* is on). If the connection breaks after Kafka wrote a message but before its answer arrived, or a
  retried request had partly been written, the messages are sent again, so in rare cases a consumer sees a
  **duplicate**. Messages are never dropped silently, except by the queue limits and expiry you configure. Consumers
  that must not process a message twice should de-duplicate, for example with a unique id in a header.

### Things to watch out for

- **Consumer → producer.** A consumer's output carries `msg.topic`, `msg.key`, `msg.partition`, `msg.headers` and
  `msg.timestamp`. Wired straight into a producer with empty settings, these are reused, and `msg.partition` may not
  exist in the target topic. Set the producer's Topic (and Partition, if needed), or delete those properties first.
- **HTTP headers.** *http in* and *http request* nodes put HTTP headers in `msg.headers`. Untick *Also send
  msg.headers*, or delete `msg.headers`, unless you want them as Kafka headers.
- **Right after a deploy**, until the first connection is up, messages are queued (with the queue on). Their report
  says `queued`, and they are sent a moment later. With *Offline queue: Off* they fail with `done(err)` instead.
- **An idle producer does not notice an outage** until it sends again: it shows `connected` until then. kafkajs
  first tries to reach the broker again for the next message (the broker node's *Retries*), so its `queued` report
  and the yellow status can take a few seconds.

## Offline queue (store-and-forward)

The offline queue makes sure messages are not lost while Kafka cannot be reached. More detail, including the file
format and recovery procedures, is in [docs/OFFLINE-QUEUE.md](docs/OFFLINE-QUEUE.md).

### How it works

```
              connected and the queue is empty ("live")
  msg ─► check & convert ───────────────────────────────► Kafka ─► report "sent"
              │  invalid content: done(err), never queued
              │
              │  not connected, or older messages still queued ("backlog")
              ▼
        offline queue ─► report "queued"
              │
              └─ after reconnecting: oldest first, in batches ─► Kafka
```

1. Every message is checked and converted first. A message with invalid content fails at once with `done(err)` and is
   never queued.
2. **Live:** while connected, with an empty queue, messages are sent directly, one request at a time. Messages that
   arrive meanwhile are batched (up to *Batch size*). The report is `sent`.
3. **Backlog:** while Kafka is not connected, every message is appended to the queue, the report `queued` is sent and
   the message is done. This also covers the time right after a deploy, an authentication problem and a broken broker
   configuration.
4. When a live send fails because of the connection (broker down, timeout, network), or a Kafka error that usually
   clears up by itself is still there after [3 retries](#acknowledgements-retries-and-compression), the failed
   messages and every message waiting behind them move to the queue, in order, and the node reconnects.
5. When Kafka is slow (a request has been in flight for more than 3 s) or more than 10000 messages wait in memory, new
   messages go to the queue, so they are safe on disk instead of piling up in memory.
6. After the reconnect, the queue is sent oldest-first, in batches of *Batch size*, one topic per request. Messages
   that arrive meanwhile are queued behind it. When the queue is empty, the node switches back to live mode and the
   queue's message files are deleted.
7. A queued message that Kafka refuses for good is moved to the [dead-letter file](#dead-letter-file) so that it
   cannot block the messages behind it.

### Queue modes

| | `disk` (default) | `memory` | `off` |
|---|---|---|---|
| While Kafka is unreachable | Messages appended to files | Messages kept in memory | Each message fails with `done(err)` |
| Survives a redeploy / restart / reboot | Yes | No (a warning says how many were discarded) | – |
| Limits, overflow, expiry | Yes | Yes | – |
| Dead letters | `dead-letter.jsonl` | Counted and logged. The last 100 are kept in memory: `queue-status` returns them in `msg.payload.deadLetters`. Lost on restart. | – |
| Use it for | Data you cannot afford to lose | Short outages, read-only file systems, sensitive data that must not touch the disk | Callers that handle failures themselves (*catch* node) |

### Where the files live

Messages are stored in `<directory>/<queue name>/`:

- **Directory:** the node's *Directory* setting. If empty, `kafkajsNcProducerQueueDir` from `settings.js` is used, and
  if that is empty too, `kafkajs-nc-queue`. A relative path is relative to the Node-RED user directory. A leading `~`
  means the home directory.
- **Queue name:** the node's *Queue name* setting, or the node's id if empty. Characters other than letters, digits,
  `.`, `_` and `-` are replaced by `_`.

So by default a producer with id `3f2a9c1e5b7d4a10` stores its queue in `~/.node-red/kafkajs-nc-queue/3f2a9c1e5b7d4a10/`
(`/data/kafkajs-nc-queue/...` in the Docker image).

An administrator can move all queues that use the default, for example to a larger disk, in `settings.js`:

```js
module.exports = {
    // ...
    kafkajsNcProducerQueueDir: '/var/lib/node-red/kafka-queue',
};
```

| File | Content |
|---|---|
| `seg-<16 digits>.jsonl` | The queued messages, one JSON object per line, in segments of about 4 MB. A segment is deleted once everything in it has been sent. |
| `cursor.json` | How far the queue has been sent. Replaced atomically. |
| `dead-letter.jsonl` | Messages that could not be delivered (see below). Rotated to `dead-letter.1.jsonl` at 50 MB, replacing the previous `.1` file; a warning says how many bytes of older dead letters that deleted. |

When the queue is empty, the segment and cursor files are deleted. The dead-letter file stays.

### Limits, overflow and expiry

- **Maximum size** (default 100 MB) and **Maximum messages** (default unlimited) limit what is waiting. The size
  counts the stored JSON, so binary payloads take about a third more than their raw size (base64).
- **When full:** *Drop the oldest* removes the oldest queued messages to make room and logs a warning with the number
  dropped. *Reject new messages* fails the new message with `done(err)`. An input message that is larger than the whole
  limit on its own is always rejected.
- **Expire after** (hours, 0 = never): when the queue is sent, messages older than this are moved to the dead-letter
  file instead of being sent. Use it when old data is worthless, for example sensor readings that are only useful
  for a day.

### Dead-letter file

A queued message that Kafka refuses for good cannot be sent at all. Examples: *message too large*, a partition that
does not exist, an invalid or unauthorised topic taken from the message, a damaged queue line, or an expired message.
Such a message is appended to `dead-letter.jsonl` in the queue directory, a warning is logged, and sending continues
with the next message. A batch that fails 5 times in a row with an error the node does not recognise is checked
message by message in the same way. Each line looks like this:

```json
{"ts":"2026-10-28T12:05:00.000Z",
 "error":{"name":"KafkaJSProtocolError","message":"...","kind":"message","reason":"MESSAGE_TOO_LARGE","retriable":false,"type":"MESSAGE_TOO_LARGE"},
 "record":{"v":1,"t":"telemetry","tf":0,"ts":1793188800000,
           "m":{"k":{"s":"sensor-1"},"v":{"s":"{\"temp\":21.5}"},"h":{"source":{"s":"line-3"}},"ts":"1793188800000"}}}
```

`record.t` is the topic and `record.ts` the time the message was queued. `record.m` holds the key (`k`), value (`v`),
headers (`h`), partition (`p`) and Kafka timestamp (`ts`). Values are `{ "s": "text" }`, `{ "b": "<base64>" }`, or
`null` for a tombstone. To resend a dead letter, read the file with a *file in* node (one message per line), rebuild the
message in a *function* node and pass it to a producer.

### Monitoring the queue

Send `msg.action = "queue-status"` to the producer, for example from an *inject* node every minute. The producer
outputs:

```json
{
  "size": 57, "bytes": 18432, "segments": 1, "droppedTotal": 0, "deadLettered": 0,
  "oldestTs": 1793188800000,
  "state": "waiting", "mode": "backlog", "queueMode": "disk",
  "queueDir": "/home/pi/.node-red/kafkajs-nc-queue/3f2a9c1e5b7d4a10",
  "pending": 0, "lastError": "ECONNREFUSED 127.0.0.1:9092"
}
```

| Field | Meaning |
|---|---|
| `size`, `bytes` | Messages and bytes waiting in the queue. |
| `segments` | Number of segment files (disk mode). |
| `droppedTotal` | Messages removed by *Drop the oldest* since the node started. |
| `deadLettered` | Messages moved to the dead-letter file since the node started. |
| `oldestTs` | When the oldest queued message arrived (epoch ms), when known. |
| `state` | Connection state: `idle` (starting, the queue is not open yet), `connecting`, `connected`, `waiting` (to reconnect), `stopped` (no broker / invalid configuration) or `closing`. |
| `mode` | `live` (sending directly) or `backlog` (going through the queue). |
| `queueMode` | `disk`, `memory`, or `off` (also when the queue could not be opened). |
| `queueDir` | The queue directory (disk mode), else `null`. |
| `pending` | Messages waiting in memory: behind the request in flight (live mode), or for the queue to open. |
| `queueError`, `lastError` | Only present after a problem: why the queue could not be opened; the last connection error. |
| `deadLetters` | Memory mode only: the last 100 dead letters, oldest first, each `{ ts, error, record }` like a line of [`dead-letter.jsonl`](#dead-letter-file). They exist nowhere else and are lost on restart. |

A *switch* node on `msg.payload.size` can then raise an alarm, for example when more than 10000 messages are waiting
or `oldestTs` is more than an hour old.

### Redeploy, restart, crash and power loss

| Event | `disk` | `memory` |
|---|---|---|
| Redeploy, or Node-RED stopped normally | The node waits up to 5 s for a request in flight, then writes everything not yet sent to the queue (`Stored N unsent message(s) in the offline queue while closing`; if some could not be stored or are still being written to a slow disk, the warning `Closing: X unsent message(s) stored in the offline queue, Y could not be stored ..., Z still being written` says so). The new instance waits until the old one has really closed the queue, then sends it first. On a very slow disk it starts without the queue after 12 s and opens it as soon as the old instance is done; it never writes the directory at the same time. | Queued messages are discarded with a warning. Messages still waiting in memory fail with `done(err)`. |
| Node-RED crashes or is killed (`kill -9`) | Messages reported `queued` are safe. A few already-sent messages may be sent again (duplicates). Messages that had not yet received a report may be lost, as in any Node-RED flow. | Everything waiting is lost. |
| Power loss | With *Sync every write to disk* on (default), every message reported `queued` is safe. With it off, the last messages written may be lost. | Everything waiting is lost. |
| Node deleted | The files stay on disk. A warning names the directory if messages were left. | Lost. |
| *Directory* or *Queue name* changed, or the queue turned off | The messages stay in the old directory and are not sent. A warning names it (`N unsent message(s) are left in the offline queue <dir>, which this node no longer uses ...`); set the old values again to send them. | Lost. |
| Node copied or imported | The copy gets a new id and so a new, empty queue. The old directory is not sent. | – |

A half-written last line from a crash is removed when the queue is opened again. That message had not been reported
as queued.

### Operational tips

- **Give production producers a Queue name.** Then the queue survives replacing the node, for example deleting it
  and importing it again. Two producers must never use the same queue name: the second one logs an error, shows
  `connected · queue unavailable` and runs without a queue. A copy of a node keeps its Queue name, so give the copy
  a name of its own.
- **Orphaned queue.** About 10 s after a disk queue opens, the producer looks at the other directories next to its
  own and warns once about those that hold messages no producer in the deployed flows uses (`Found N offline queue(s)
  under <directory> with unsent messages that no producer in the deployed flows uses: <name> (<size>), ...`).
  Producers that are disabled or in a disabled flow count as using their queue. If a node lost its queue because it
  got a new id, set its Queue name to the old directory's name (the old node id); the messages are then sent on the
  next deploy. A listed directory may also belong to a flow you will import again or to another Node-RED instance
  that uses the same directory: delete it only when you are sure no producer needs those messages.
- **One Node-RED per directory.** The lock that prevents two producers from sharing a directory only works inside one
  Node-RED process. Never point two Node-RED instances at the same queue directory, for example on a shared network
  drive. Use a local disk.
- **Persistent storage.** In containers, keep the user directory (or `kafkajsNcProducerQueueDir`) on a volume.
- **Size the limits.** A Raspberry Pi with a small SD card should have a *Maximum size* that leaves room for the
  system. *Expire after* stops very old data from being sent after a long outage.
- **Plain text on disk.** The queue holds message contents unencrypted. Protect the directory with file permissions
  or disk encryption, or use the memory mode for sensitive data. See [SECURITY.md](SECURITY.md).
- **Version control.** If your user directory is a git repository, add `kafkajs-nc-queue/` to `.gitignore`.
- **Clearing a queue:** send `msg.action = "queue-clear"`, or delete the directory while Node-RED is stopped.

## Consumer

The consumer subscribes to topics as a member of a consumer group and sends one message per Kafka record.

### Output

| Property | Type | Notes |
|---|---|---|
| `msg.payload` | string, Buffer, any (parsed JSON), or `null` | The value, in the chosen *Payload* format. `null` for a tombstone. |
| `msg.topic` | string | The topic. |
| `msg.key` | string, Buffer or `null` | The key, in the chosen *Key* format. `null` when there is none. |
| `msg.partition` | number | The partition. |
| `msg.offset` | string | The offset. A string, because offsets can exceed JavaScript's safe integers. |
| `msg.timestamp` | number | Kafka timestamp in ms since the epoch. |
| `msg.headers` | object | `{ name: value }`. Values are strings (or Buffers with *Header values: Buffers*). A repeated header is an array, and a header without a value is `null`. |
| `msg.kafka` | object | `{ groupId, topic, partition, offset, timestamp, highWatermark }`. `highWatermark` (string) is the offset after the last message in the partition when the batch was fetched, so `highWatermark - offset - 1` is roughly the lag. Keep `msg.kafka` if you commit manually: a producer node replaces it with its delivery report. |

**Formats.** *UTF-8 string* (default). *Buffer*, which keeps binary data such as Avro or Protobuf intact; each Buffer
is a private copy. *Parsed JSON*: a value that is not valid JSON is not sent. Instead a *catch* node receives it with
the raw text in `msg.payload`, and the consumer carries on with the next record. With automatic commits the bad
record is committed like any other, since reading it again would fail the same way. Keys and header values can be
strings or Buffers.

### Subscription

- **Topics:** a list separated by commas or spaces, e.g. `orders, payments`. Each name must be a valid Kafka topic
  name.
- **Topics is a regular expression:** the field is then one JavaScript regular expression, either plain (`^orders-.*`)
  or in slashes with flags (`/^orders-.*$/i`). The `g` and `y` flags are ignored. **Caveat:** the expression is
  matched against the topics that exist when the consumer starts. A matching topic created later is only consumed
  after `msg.action = "restart"`, a redeploy, or the next reconnect.
- **Group ID:** consumers with the same group ID share the partitions, and Kafka remembers per group how far it has
  read. Give two nodes different group IDs if both should receive every message. If empty, the group ID is
  `node-red-<node id>`. It stays the same across restarts and deploys, so the node continues where it stopped.
  Copying or importing the node creates a new id and therefore a new group: set a Group ID if the position must
  survive that. Inside a subflow the node id is `<subflow instance id>-<node id>`, so each instance of the subflow
  has a group of its own, which also stays the same across restarts.
- **Read from the beginning when the group has no stored position** (`fromBeginning`) only matters for a group without
  a committed offset for a partition, typically a new group. It then starts with the oldest message instead of only new
  ones. It does **not** rewind an existing group: use `seek`, or the admin action `resetOffsets` while the consumer is
  stopped.
- **Start consuming on deploy** (`autoStart`, default on): untick it to start the node in the `stopped` state, waiting
  for `msg.action = "start"`.
- A topic that does not exist yet is retried with the normal backoff (status `unknown topic/partition · retry …`, and
  a warning rather than an error in the log, since it is often created a moment later), and consuming starts at the
  first retry after the topic was created: at most *Reconnect: longest wait* (60 s by default) later. By default the
  consumer does not create topics itself.
- A regular expression that matches no topic, or a member of a group whose partitions all went to other members,
  shows the yellow status `connected · no partitions` instead of a green one. With a regular expression a warning is
  logged too.

### Commit modes and delivery guarantees

Kafka stores, per group and partition, the offset of the next record to read (the *committed offset*). When that is
stored decides what happens when Node-RED stops while records are being processed.

| Mode | When the position is stored | If Node-RED crashes | Use it for |
|---|---|---|---|
| **Automatically** (default) | Every *Every* ms (default 5000; 0 = only at the end of each batch), every *Or every* records (0 = off), and at the end of each fetched batch, as soon as the record was handed to the flow | Records handed to the flow since the last stored position (at most the last fetched batch) are delivered **again** (at least once, with possible duplicates). The position does not wait for your flow, so a record that was still being processed when its position was stored is **not** delivered again. | Telemetry, dashboards, anything where an occasional gap or duplicate is fine |
| **Manually** | Only when you send `msg.action = "commit"` with the record's `msg.kafka` | Every record not committed is delivered again after a restart, reconnect or rebalance (**at least once**) | Processing that must not lose records. Make it tolerate duplicates. |

The committed position is the record's offset + 1. Committing a record also commits every earlier record of the same
partition. Commit in order per partition: committing an older offset after a newer one moves the group back, so records
are delivered again (nothing is lost).

### Control messages

The consumer's input only takes control messages. `msg.action` is case-insensitive. Every control message completes
with `done()`, so a *complete* node can follow it. A failed one goes to *catch* nodes. Control messages are not passed
to the output.

| `msg.action` | Parameters | What it does |
|---|---|---|
| `pause` | – | Stop fetching all subscribed topics, keeping the group membership. It takes effect after the record being delivered. A pause sent before the consumer has joined its group is applied as soon as it joins, and it is re-applied after every reconnect and rebalance. |
| `resume` | – | Continue fetching. |
| `stop` | – | Leave the group and disconnect. The node does not reconnect until `start`. Status `stopped`. |
| `start` | – | Start a stopped consumer. Does nothing if it is already running. |
| `restart` | – | Disconnect and connect again now, which also picks up new topics that match a regular expression. |
| `seek` | `msg.payload = { topic, partition, offset }` | Continue reading a partition from another offset. `topic` may be left out when the node subscribes to a single topic. `offset` is a number, a string of digits, `"earliest"` (the oldest record still stored) or `"latest"` (only new records). See [Seek](#seek). |
| `commit` | `msg.kafka = { groupId, topic, partition, offset }` | Store the position after this record (manual mode). `msg.kafka` must be the one this consumer set on its output. Uses `msg.topic`, `msg.partition` and `msg.offset` when `msg.kafka` is missing. See [Commit](#commit). |

Examples, each in a *function* node wired to the consumer's input:

```js
// rewind partition 0 of "orders" to the oldest record
return { action: "seek", payload: { topic: "orders", partition: 0, offset: "earliest" } };
```

```js
// manual commit: after processing, send the record back with action "commit"
msg.action = "commit";   // msg.kafka is still the one the consumer set
return msg;
```

A typical at-least-once flow: consumer (Commit: *Manually*) → your processing → a *change* node that sets
`msg.action` to `commit` → a *link out* back to the consumer's input. A *catch* node on the processing step can log
failures: the uncommitted record is delivered again after the next restart.

#### Seek

Before seeking, the consumer looks up the partition's current offsets with a short admin connection (at most 15 s):

- `"earliest"` (also `"beginning"`) becomes the partition's oldest stored offset and `"latest"` (also `"end"`) its
  end. kafkajs itself would treat those names as "no position" and apply *Read from the beginning* instead.
- A number outside the partition's offsets is refused (`seek: offset N is outside the offsets of orders[0] (low to
  high); ...`): Kafka would otherwise jump to its default position and store it, even in manual mode, silently
  replacing the position your flow committed.
- A seek only works while the consumer is connected and not rebalancing, for a partition assigned to this member of
  the group; the error lists the assigned partitions.

With *Commit: Automatically* the new position is stored right away; with *Manually* only when you commit.

#### Commit

`msg.kafka` must be the one this consumer set on its output message. A commit is refused when its `groupId` is not
this consumer's group or its topic is not one the consumer subscribes to, and fields are never mixed between
`msg.kafka` and `msg`. This matters when the processing includes a **producer node**: the producer replaces
`msg.kafka` with its delivery report (the topic, partition and offset it wrote to), and committing that would store a
position for the wrong topic. Keep a copy before the producer and put it back before the commit:

```js
// function node before the producer
msg.consumed = msg.kafka;
return msg;
```

```js
// function node after the producer, wired back to the consumer's input
msg.kafka = msg.consumed;
msg.action = "commit";
return msg;
```

A commit fails (`Commit not stored: the Kafka consumer was stopping. ...`) when the consumer stopped or restarted
before Kafka stored the position. The records after the last stored position are then delivered again.

### Back-pressure with pause and resume

The consumer hands each record to the flow and immediately goes on with the next one: it does not wait for your flow
to finish. If the processing is slower than Kafka (a slow database, an HTTP API with rate limits), pause the consumer
while too much is in progress and resume it later. One way is a counter in flow context:

```js
// function node right after the consumer; output 1 → processing, output 2 → consumer input
const inFlight = (flow.get("inFlight") || 0) + 1;
flow.set("inFlight", inFlight);
return [msg, inFlight === 500 ? { action: "pause" } : null];
```

```js
// function node at the end of the processing; output → consumer input
const inFlight = Math.max(0, (flow.get("inFlight") || 0) - 1);
flow.set("inFlight", inFlight);
return inFlight === 50 ? { action: "resume" } : null;
```

If a message can fail along the way, decrease the counter on the error path (for example after a *catch* node) as
well, or the consumer stays paused.

### Settings

| Tab | Setting | Property | Default | Description |
|---|---|---|---|---|
| – | Broker | `broker` | | The broker configuration node (required). |
| – | Name | `name` | | Label shown in the editor. |
| Subscription | Topics | `topics` | | Required. A list, or one regular expression. |
| Subscription | Topics is a regular expression | `topicIsRegex` | off | |
| Subscription | Group ID | `groupId` | empty | Empty: `node-red-<node id>`. |
| Subscription | Read from the beginning ... | `fromBeginning` | off | Only for a group without a stored position. |
| Subscription | Start consuming on deploy | `autoStart` | on | |
| Output | Payload | `payloadFormat` | `string` | `string`, `buffer` or `json`. |
| Output | Key | `keyFormat` | `string` | `string` or `buffer`. |
| Output | Header values | `headersFormat` | `string` | `string` or `buffer`. |
| Commit | Commit | `commitMode` | `auto` | `auto` or `manual`. |
| Commit | Every | `autoCommitInterval` | 5000 ms | 0 = only at the end of each batch (0 – 86400000). |
| Commit | Or every | `autoCommitThreshold` | 0 | Records; 0 = off (0 – 1000000000). |
| Advanced | Session timeout | `sessionTimeout` | 30000 ms | A member without a heartbeat for this long is removed from the group (1000 – 3600000). The broker limits it too. |
| Advanced | Heartbeat interval | `heartbeatInterval` | 3000 ms | Must be lower than the session timeout. A value that is too high is replaced by a third of the session timeout, with a warning (100 – 3600000). |
| Advanced | Rebalance timeout | `rebalanceTimeout` | 60000 ms | (1000 – 3600000) |
| Advanced | Max per partition | `maxBytesPerPartition` | 1048576 | Bytes per partition per fetch. |
| Advanced | Max per fetch | `maxBytes` | 10485760 | Bytes per fetch. |
| Advanced | Max wait | `maxWaitTimeInMs` | 1000 ms | How long the broker may hold a fetch when there is no new data (0 – 300000). Stopping the node waits for that fetch, so keep it short. |
| Advanced | Concurrency | `partitionsConsumedConcurrently` | 1 | Partitions processed at the same time (1 – 100). Records of one partition always arrive in order. Above 1, records of different partitions may be interleaved. |
| Advanced | Let the broker create missing topics | `allowAutoTopicCreation` | off | |

**Compression:** records compressed with gzip, snappy or lz4 are decoded. A topic that contains **zstd**-compressed
records makes the consumer fail with `compression codec missing`. It then retries slowly, and the log explains why.

## Admin

The admin node runs one Kafka admin operation per input message. The action comes from the node's **Action** setting,
or from `msg.action` (case-insensitive) when the setting is *set by msg.action*. If both are present, the node setting
wins. The parameters come from `msg.payload`.

| Setting | Property | Default | Description |
|---|---|---|---|
| Broker | `broker` | | The broker configuration node (required). |
| Action | `action` | empty | One of the actions below, used for every message. Empty (*set by msg.action*): take it from `msg.action`. |
| Timeout | `timeout` | 30000 ms | Limit for each message, counted from its arrival: waiting behind earlier messages and connecting are included (100 – 600000). `setOffsets` and `resetOffsets` always get at least 15 s. |
| Name | `name` | | Label shown in the editor. |

- The node connects on the first message and then stays connected. Messages are handled one after another, in
  arrival order. At most 1000 messages wait; further ones fail at once (`Kafka admin X not run: 1000 messages are
  already waiting (is Kafka reachable?)`), so a polling flow cannot pile up stale requests during an outage.
- The *Timeout* counts from the moment a message arrives. A message that waited behind slow ones only gets the rest
  of it, and the error says how long it waited (`Kafka admin X timed out after 30000ms (12s of it waiting behind
  earlier messages)`).
- After a failed connection attempt, the following messages fail at once (`Kafka admin X not run: the last connection
  attempt failed (<reason>); the next message after 2s connects again`) until the broker node's reconnect wait has
  passed (at least *After auth/config errors* for authentication and configuration problems). Then the next message
  connects again.
- The result is put in `msg.payload`, and `msg.kafka = { action, durationMs }` is added. The rest of the message is
  passed through. Actions without a result output `true`.
- If an action fails, `done(err)` sends the original message to *catch* nodes, and the next message is processed
  normally. Unknown actions and missing parameters are reported the same way, with a text that says what is expected;
  nothing is sent to Kafka in that case. After a connection problem or a timeout, the next message reconnects with a
  fresh client.

| Action | `msg.payload` | Result in `msg.payload` |
|---|---|---|
| `listTopics` | – | `["orders", "payments", ...]` |
| `createTopics` | `"orders"`, `"a, b"`, `["a", "b"]`, `{ "topic": "orders", "numPartitions": 3 }` or `{ "topics": [ { "topic": "orders", "numPartitions": 3, "replicationFactor": 1, "configEntries": { "retention.ms": "86400000" } } ] }`. Optional: `validateOnly`, `waitForLeaders`, `timeout`. `numPartitions`/`replicationFactor` -1 = broker default. `configEntries` as an object or as `[{ name, value }]`. | `true`, or `false` when the topics already existed |
| `deleteTopics` | Topic name, list or array, or `{ "topics": [...] }`. Optional: `timeout`. | `true` |
| `createPartitions` | `{ "topicPartitions": [ { "topic": "orders", "count": 6 } ] }` or `{ "topic": "orders", "count": 6 }`. `count` is the new total; partitions can never be removed. Optional: `validateOnly`, `timeout`. | `true` |
| `fetchTopicMetadata` | Optional topic name(s) or `{ "topics": [...] }`; none = all topics | `{ topics: [ { name, partitions: [ { partitionId, leader, replicas, isr, ... } ] } ] }` |
| `fetchTopicOffsets` | `"orders"` or `{ "topic": "orders" }` | `[ { partition, offset, high, low } ]` (offsets as strings) |
| `fetchTopicOffsetsByTimestamp` | `{ "topic": "orders", "timestamp": 1793188800000 }`. A `Date` or an ISO date string (`"2026-10-28T12:00:00Z"`) works too; negative times and other text are refused. | `[ { partition, offset } ]`: the first offset at or after that time |
| `fetchOffsets` | `"my-group"` or `{ "groupId": "my-group", "topics": ["orders"] }` (topics optional). Optional: `resolveOffsets`. | `[ { topic, partitions: [ { partition, offset, metadata } ] } ]` |
| `resetOffsets` | `{ "groupId": "my-group", "topic": "orders", "earliest": true }`. Without `earliest: true`, the group moves to the **latest** offset. The node looks up the partitions' oldest or end offsets and stores those numbers. | `true` |
| `setOffsets` | `{ "groupId": "my-group", "topic": "orders", "partitions": [ { "partition": 0, "offset": "42" } ] }`. Offsets are whole numbers (number or string) between the partition's oldest and end offset (see `fetchTopicOffsets`); a partition must exist and be listed once. | `true` |
| `listGroups` | – | `{ groups: [ { groupId, protocolType } ] }` |
| `describeGroups` | `"my-group"`, `["a", "b"]` or `{ "groupIds": [...] }` | `{ groups: [ { groupId, state, protocol, protocolType, members, errorCode } ] }` |
| `deleteGroups` | `"my-group"`, `["a", "b"]` or `{ "groupIds": [...] }` | `[ { groupId, errorCode } ]` |
| `describeCluster` | – | `{ brokers: [ { nodeId, host, port } ], controller, clusterId }` |
| `describeConfigs` | `{ "resources": [ { "type": "TOPIC", "name": "orders", "configNames": ["retention.ms"] } ] }` or `{ "type": "BROKER", "name": "1" }`. `type` is `TOPIC`, `BROKER` or `BROKER_LOGGER`. Optional: `configNames`, `includeSynonyms`. | `{ resources: [ { resourceType, resourceName, configEntries: [ { configName, configValue, ... } ], errorCode, errorMessage } ] }` |

`resetOffsets`, `setOffsets` and `deleteGroups` only work while the group has no running members. Stop the consumers
first (for example with `msg.action = "stop"`). `resetOffsets` stores real offsets, so the group's consumers start
exactly there, whatever their *Read from the beginning* setting (kafkajs' own `resetOffsets` would store a marker
that leaves the choice to that setting). Both actions take about 5 s, because kafkajs joins the group briefly to
change its offsets.

`createTopics` also takes `replicaAssignment: [{ partition, replicas: [brokerId, ...] }]` per topic, and
`createPartitions` takes `assignments: [[brokerId, ...], ...]` (one array per new partition). Values of the wrong
shape are refused rather than ignored.

Example: create a topic from a *function* node in front of an admin node whose Action is *set by msg.action*:

```js
msg.action = "createTopics";
msg.payload = { topics: [{ topic: "orders", numPartitions: 3, replicationFactor: 1 }] };
return msg;
```

## Status reference

Statuses use the usual Node-RED colours. Green means it works, yellow means waiting or retrying by itself, red means a
person has to fix something, and grey means idle. `· retry 8s` shows when the next connection attempt is due. A
status shows at most 48 characters: the reason gets whatever the suffixes (`· 3 queued`, `· retry 8s`) leave, so the
usual reasons fit whole (`TLS: untrusted cert (CA?) · retry 60s`); a longer one is shortened with `…`, and the log
always has the full text. The broker configuration node has no status of its own: its problems appear on the nodes
that use it.

### Producer

| Status | Meaning | What to do |
|---|---|---|
| `starting` / `12 queued` (grey) | Starting up, opening the queue. | Nothing. |
| `connecting…` (yellow) | Connecting to Kafka. | Nothing. |
| `connected` (green dot) | Messages are sent directly. | – |
| `connected · 12 queued` (green dot), `flushing · 1234 queued` (green ring) | Connected, sending the offline queue. | Nothing. Watch the number go down. |
| `offline · 57 queued · retry 8s` (yellow) | Kafka cannot be reached; messages are kept. | Check the broker and the network. |
| `ECONNREFUSED broker:9092 · retry 8s` (yellow) | Same, with an empty queue, showing the reason. Other reasons: `broker unreachable (DNS)`, `connection timeout`, `request timeout`, `connection closed`, `TLS handshake failed`, `unknown topic/partition`. | See the [FAQ](#troubleshooting-faq). |
| `auth failed · retry 60s` (red) | User name or password rejected. | Fix the credentials and deploy. |
| `TLS: self-signed cert · retry 60s`, `TLS: untrusted cert (CA?)`, `TLS: hostname mismatch`, `TLS: cert expired` (red) | The broker's certificate is not trusted. | Add the CA in a TLS configuration node, or set the server name. |
| `TLS: invalid cert, key or CA · retry 60s` (red) | The TLS configuration node's certificate, key or CA cannot be used (for example a key that does not belong to the certificate). | Check the files of the TLS configuration node. |
| `not authorized (topic) · 1 queued · retry 60s` (red) | The user lacks rights (ACLs). Others: `SASL mechanism unsupported`, `SASL not expected by listener`, `broker version unsupported`. | Fix the ACLs or the configuration. |
| `config error · 3 queued` (red) | The broker configuration is invalid (see the log). Messages are queued. | Fix it and deploy. |
| `no broker config` (red) | No broker selected. Messages are queued. | Select a broker. |
| `connected · queue unavailable` (yellow dot) | Connected, but the offline queue could not be opened. | Check the log: permissions, disk, or a queue name used twice. |

### Consumer

| Status | Meaning | What to do |
|---|---|---|
| `starting` (grey), `connecting…`, `joining group…` (yellow) | Starting. | Nothing. |
| `connected · orders, payments` (green dot) | Consuming. | – |
| `connected · no partitions` (yellow dot) | In the group, but no partition is assigned to this node: the regular expression matches no topic (a warning is logged), or other members of the group have all partitions. | Check the expression; create the topics and send `restart`. |
| `paused · orders` (yellow dot) | Paused with `msg.action = "pause"`. | Send `resume`. |
| `rebalancing…` (yellow) | The group is redistributing the partitions. | Nothing. |
| `stopped` (grey) | Stopped with `stop`, or *Start consuming on deploy* is off. | Send `start`. |
| `ECONNREFUSED broker:9092 · retry 8s` (yellow) | Kafka cannot be reached. Other reasons as for the producer. | Check the broker and the network. |
| `unknown topic/partition · retry 2s` (yellow) | A subscribed topic does not exist (yet). Consuming starts at the first retry after it was created. | Create the topic, or check its name. |
| `group coordinator not found · retry 2s` (yellow) | The group's coordinator is not available (brokers starting or recovering, the `__consumer_offsets` topic offline), or the user may not use the group: kafkajs reports both the same way. Retried with the normal backoff. | If it stays, grant `READ` on the consumer group and check the brokers. |
| `auth failed · retry 60s` (red) | Credentials rejected. | Fix them and deploy. |
| `not authorized (group) · retry 60s`, `not authorized (topic) · retry 60s` (red) | Missing ACLs for the group or the topic. | Grant read access to the topic and the group. |
| `compression codec missing · retry 60s` (red) | The topic contains zstd-compressed records. | Use another compression on the producing side. |
| `config error`, `no topic`, `invalid topic regex`, `no broker config` (red) | The configuration is invalid (see the log). The node does not connect. | Fix it and deploy. |

### Admin

| Status | Meaning |
|---|---|
| `ready` (grey) | Waiting for a message (not connected yet). |
| `connecting…` (yellow) | Connecting for the first message, or after a failure. |
| `running createTopics (+2 queued)` (blue) | Working; more messages wait. |
| `ok · createTopics` (green) | The last action succeeded. |
| `createTopics: <reason>`, `fetchOffsets timed out`, `auth failed` (red) | The last action failed. The same reason is in `msg.error` for *catch* nodes. The next message runs normally. |
| `config error`, `no broker config`, `unknown action` (red, from the start) | Fix the configuration and deploy. `unknown action`: the node's *Action* setting names no action of this node. |

A node showing `internal error` (red) hit an unexpected bug. Please
[open an issue](https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/issues) with the log.

## Error handling & monitoring

### Catch node: failed messages

Problems that belong to one message end the message with `done(err)`. A *catch* node then receives the original
message with `msg.error.message` explaining what went wrong:

- **Producer:** invalid content (no topic, an invalid topic name, an `undefined` payload, an invalid key, partition or
  timestamp), a partition that does not exist, *message too large*, a full queue with *Reject new messages*, Kafka
  unreachable with the queue off, a failed control action. Messages already reported `queued` are done. If they
  later turn out to be undeliverable, they go to the [dead-letter file](#dead-letter-file), not to *catch*.
- **Consumer:** a record that is not valid JSON (*Parsed JSON* format), with the raw text in `msg.payload`; failed
  control messages (unknown action, `seek` while not connected or for a partition not assigned to this member, an
  offset outside the partition's offsets, a `commit` with a `msg.kafka` that is not this consumer's, a commit that
  Kafka did not store, ...).
- **Admin:** every failed action, including timeouts, invalid parameters, messages refused because 1000 were already
  waiting, and messages failed at once after a failed connection attempt.

Connection problems do not belong to a message, so they are **not** sent to *catch* nodes. Watch them with a *status*
node and in the log.

### Status node: connection state

A *status* node limited to the Kafka nodes receives every status change: `msg.status.fill` (`green`, `yellow`, `red`,
`grey`, `blue`), `msg.status.text` and `msg.status.source`. For example, a *switch* node on `msg.status.fill == "red"`
can send a notification when credentials or certificates need attention. A *trigger* node can raise an alarm when a
node has been yellow for more than five minutes.

### Logs

- **Node messages.** The first failure of an outage is logged as an error with the full cause chain, for example
  `Kafka connection failed: KafkaJSNumberOfRetriesExceeded: Connection error: connect ECONNREFUSED 127.0.0.1:9092 ←
  caused by KafkaJSConnectionError [ECONNREFUSED] — retrying in 1s`. Identical repeats are logged at debug level,
  with a warning every 10th attempt. A different kind of failure is logged as an error again (a consumer's missing
  topic only as a warning). On recovery the log says `Connected to Kafka after N attempts`. The producer also logs
  queue events (messages kept from earlier, sent after the reconnect, dropped, dead-lettered, queues left behind) and,
  at debug level, each time it sends a request again after a Kafka error.
- **kafkajs messages** are written to the Node-RED log, labelled with the node they belong to:
  `[kafkajs:Connection] [kafkajs-nc-producer:my producer] ...`. The broker's *kafkajs log level* selects which ones:
  `off`, `error`, `warn` (default), `info` or `debug` (very verbose). kafkajs errors and warnings are logged as
  Node-RED warnings, because most of them are connection noise that the nodes already report themselves. Identical
  lines are logged at most once a minute, followed by `(repeated N times)`.
- To see debug lines, set `logging.console.level` to `debug` in Node-RED's `settings.js`.

### Safety net

Node-RED stops completely on any unhandled promise rejection. These nodes handle their own errors. kafkajs, however,
runs some internal operations in the background, and an error from one of those would be unhandled. The *Crash safety
net* option of the broker node (on by default) adds one process-wide handler for unhandled **promise rejections**
while at least one broker node uses it, and for 30 s after the last one closed (client nodes may still be shutting
down then). It does not handle uncaught exceptions.

- A rejection that comes from kafkajs or from this palette is logged as a warning
  (`[kafkajs-nc] Prevented a crash: ...`) instead of stopping Node-RED. That means an error named `KafkaJS*` or
  `KafkaNc*`, or one whose stack runs through the copy of kafkajs this palette loads or through this palette's own
  files. kafkajs errors raised by another palette that also uses kafkajs cannot be told apart and are caught too.
- Any other rejection is handed back to Node.js, which handles it exactly as it would without this palette: by
  default Node-RED logs it and stops, so bugs in other nodes are not hidden.
- With `node --unhandled-rejections=strict`, Node.js stops the process before any handler runs. The safety net is
  then not installed, and a warning at startup says so.

Turn it off only if you install your own handler for unhandled rejections.

## Examples

These flows are in the [`examples/`](examples) folder, and in Node-RED under **Menu → Import → Examples →
node-red-contrib-kafkajs-no-crash**. Each one has comment nodes with step-by-step notes in the *Info* sidebar.
Examples 01 – 07 and 09 connect to `localhost:9092`; example 08 reads the broker address and the credentials from
environment variables.

| Example | Shows |
|---|---|
| [01 - Produce and consume basics](examples/01%20-%20Produce%20and%20consume%20basics.json) | Broker configuration, creating a topic with the admin node, producing text and a JSON object, delivery reports, consuming |
| [02 - Offline queue (store and forward)](examples/02%20-%20Offline%20queue%20%28store%20and%20forward%29.json) | The disk queue during an outage, `queue-status`, `flush`, `reconnect`, `queue-clear`, a *status* node on the producer |
| [03 - JSON, keys and headers](examples/03%20-%20JSON,%20keys%20and%20headers.json) | JSON values with a key and headers, *Parsed JSON* output, invalid JSON caught by a *catch* node |
| [04 - Batch messages](examples/04%20-%20Batch%20messages.json) | `msg.messages`: a small batch for two topics and a batch of 1000 sensor readings built in a function |
| [05 - Consumer control and manual commit](examples/05%20-%20Consumer%20control%20and%20manual%20commit.json) | `pause`, `resume`, `stop`, `start`, `restart`, `seek`, and at-least-once processing with manual `commit` |
| [06 - Admin operations](examples/06%20-%20Admin%20operations.json) | Topics (create, list, metadata, offsets, delete), consumer groups (list, describe, offsets, reset to earliest), the cluster, failed actions in a *catch* node |
| [07 - Error handling with Catch and Status](examples/07%20-%20Error%20handling%20with%20Catch%20and%20Status.json) | Failed messages in a *catch* node (queue off), alerts on connection changes with a *status* node |
| [08 - Secure connection with SASL, TLS and env vars](examples/08%20-%20Secure%20connection%20with%20SASL,%20TLS%20and%20env%20vars.json) | SASL/SCRAM-SHA-512 over TLS with a *TLS configuration* node, broker address and credentials from environment variables |
| [09 - IoT sensor buffering](examples/09%20-%20IoT%20sensor%20buffering.json) | An edge device buffering a reading per second in a bounded, expiring disk queue; a consumer building 10-second summaries |

## Migrating from node-red-contrib-kafkajs

The two palettes use different node types (`kafkajs-client`, `kafkajs-producer` and `kafkajs-consumer` there;
`kafkajs-nc-broker`, `kafkajs-nc-producer` and `kafkajs-nc-consumer` here), so they can be installed side by side and
you can migrate one flow at a time. The main differences:

- **Consumer output:** upstream puts the whole kafkajs record in `msg.payload` (the value is `msg.payload.value`).
  Here `msg.payload` **is** the value, and `msg.key`, `msg.offset`, `msg.headers`, `msg.timestamp` and `msg.kafka`
  carry the rest. Function nodes that read `msg.payload.value` need a change.
- **Group ID:** upstream picks a random group ID on every deploy when the field is empty. Here it is `node-red-<node id>`
  and stays the same, so the node continues where it stopped. Set the old group ID explicitly if you want to keep its
  committed position.
- **Clear offsets** (upstream consumer option) is replaced by the admin action `resetOffsets`, or by `seek`.
- **TLS files** are configured in a standard *TLS configuration* node instead of file paths in the broker node.
  *Use TLS* is off by default. Upstream's *Use SSL* was on by default for SASL.
- **Producer:** falsy payloads (`0`, `""`) and non-string payloads are now sent instead of being dropped or failing.
  Keyed messages produced by upstream 0.0.7 (kafkajs 1.x) were partitioned with what is now the *legacy* partitioner:
  choose *Partitioner: Legacy* to keep the same key-to-partition mapping.
- **Producing while Kafka is down** used to drop messages. Now they are queued on disk unless you set *Offline queue:
  Off*.

The script [`scripts/migrate-flows.js`](scripts/migrate-flows.js) converts a `flows.json` file for you and reports
what needs attention. It comes with the package as the command `kafkajs-nc-migrate-flows`:

```sh
npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows ~/.node-red/flows.json   # writes flows.migrated.json
```

From a git checkout it is `node scripts/migrate-flows.js ~/.node-red/flows.json`. [docs/MIGRATION.md](docs/MIGRATION.md)
explains the options (`--in-place`, `--credentials`), Docker, and the report. To migrate a flow by hand instead:

1. Install this palette next to the old one and write down the old settings. Credentials are not exported with a
   flow, so have the SASL username and password at hand.
2. Create a **kafka broker** node. Copy the broker list (one `host:port` list) and the client ID. Upstream
   *Auth: SASL* becomes *Mechanism* PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, plus *Use TLS* if upstream had *Use SSL*
   ticked. Upstream *Auth: TLS* becomes *Use TLS* with a *TLS configuration* node that holds the CA, the client
   certificate and the key. Instead of upstream's *Self Sign* box, add the broker's CA to the TLS configuration (or,
   on test systems only, untick *Verify server certificate* there).
3. Replace each `kafkajs-producer` with a **kafka producer**: Topic, Key, Partition and the headers move to the
   *Message* tab; *Acks* ALL / LEADER / NONE becomes *Acknowledgement* all / leader / none.
4. Replace each `kafkajs-consumer` with a **kafka consumer**: the topic goes to *Topics*. Set *Group ID* to the old
   group ID if you used one. *From beginning*, the commit interval and threshold and the session, heartbeat and
   rebalance timeouts have direct counterparts.
5. Change the function nodes that read the consumer output (`msg.payload.value` → `msg.payload`,
   `msg.payload.key` → `msg.key`, `msg.payload.headers` → `msg.headers`, `msg.payload.offset` → `msg.offset`).
6. Deploy, check that every Kafka node turns green, then remove the old nodes and uninstall the old palette.

More detail, with a property-by-property mapping, is in [docs/MIGRATION.md](docs/MIGRATION.md).

## Troubleshooting FAQ

More cases and diagnostics are in [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

**The node shows `ECONNREFUSED host:9092 · retry …`.** Nothing is listening at that address. Check host, port and
firewall. In Docker, `localhost` is the container itself.

**It connects, then fails with a different host name or with `localhost`.** Kafka answers the first connection with
the addresses in its `advertised.listeners` setting, and all further connections go there. Those addresses must be
reachable from Node-RED.

**`auth failed · retry 60s`.** Wrong user name or password, or the wrong SCRAM variant (SHA-256 and SHA-512 are
separate credentials on the broker).

**`TLS handshake failed`, or `SASL not expected by listener`.** The security settings do not match the listener: TLS
switched on for a plain listener, or SASL selected for a `PLAINTEXT` port. Check which port belongs to which listener.

**The producer shows `connected`, but messages are `queued` and the status soon shows `· retry`.** The broker
accepted the connection but refused the first real request. This typically happens with a client without SASL on a
SASL listener. Check the mechanism.

**`unknown topic/partition`.** The topic does not exist and the broker does not create it. Create it (for example
with the admin node), or enable automatic topic creation.

**The consumer receives nothing although the topic has data.** The group probably has a committed position at the
end already. *Read from the beginning* only applies to new groups. Use `seek` while the consumer runs, or stop it and
use the admin action `resetOffsets`.

**Some messages arrive twice.** That is at-least-once delivery after a connection problem (see
[Delivery guarantees](#ordering-and-delivery-guarantees)). De-duplicate on the consuming side, for example with an id
in a header.

**`invalid Kafka topic name "sensors/temp"`.** Kafka topic names cannot contain `/`. Set the producer's Topic, or
change `msg.topic` (for example replace `/` with `.`).

**`commit: msg.kafka is not a message of this consumer (it has no groupId)`.** A producer node between the consumer
and the commit replaced `msg.kafka` with its delivery report. Keep a copy of the consumer's `msg.kafka` before the
producer and put it back before the commit (see [Commit](#commit)).

**`group coordinator not found · retry …`.** Usually temporary: the brokers are starting or recovering. If it stays,
the user is probably not allowed to use the consumer group (kafkajs reports a missing group ACL the same way): grant
`READ` on the group.

## Development

```sh
npm install
npm test             # unit and node tests against a fake kafkajs, no broker needed
npm run test:unit    # lib/, the example flows, the migration script and the docs snippets
npm run test:nodes   # the four nodes inside node-red-node-test-helper
npm run lint
```

`npm test` uses the Node-RED version from `devDependencies` (4.1). CI also runs the suite against other majors, which
you can do locally with `npm install --no-save node-red@3` (or `@5`).

**End-to-end tests against a real Kafka broker.** The scripts in [`scripts/kafka/`](scripts/kafka) download Apache
Kafka 3.9.1 (checksum verified) and run a single-node KRaft broker with a `PLAINTEXT` listener on `127.0.0.1:19092`
and a `SASL_PLAINTEXT` listener (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; users `admin`/`admin-secret` and
`alice`/`alice-secret`) on `127.0.0.1:19093`. You need bash, curl and a Java runtime (CI uses Java 21).

```sh
export KAFKA_HOME="$(bash scripts/kafka/kafka-download.sh | tail -n1)"
npm run kafka:start
KAFKA_E2E_BROKERS=127.0.0.1:19092 KAFKA_E2E_SASL_BROKERS=127.0.0.1:19093 KAFKA_E2E_CONTROL=1 npm run test:e2e
npm run kafka:stop
```

Without `KAFKA_E2E_BROKERS`, the e2e tests are skipped. `KAFKA_E2E_CONTROL=1` allows the outage tests to stop, kill
(`SIGKILL`) and freeze (`SIGSTOP`) the broker. Other helpers in `scripts/kafka/`: `kafka-status.sh`,
`kafka-create-topics.sh`, `kafka-add-scram-user.sh`, `kafka-pause.sh pause|resume`, `kafka-stop.sh --kill` and
`kafka-reset.sh`. Each script documents its environment variables at the top.

| Folder | Content |
|---|---|
| [`nodes/`](nodes) | The four nodes: runtime (`.js`) and editor + help text (`.html`) |
| [`lib/`](lib) | Shared modules: configuration parsing, error classification, disk and memory queues, serialisation, logging, safety net |
| [`test/unit/`](test/unit), [`test/nodes/`](test/nodes), [`test/e2e/`](test/e2e) | Unit, node (fake kafkajs) and end-to-end (real broker) tests. A root hook fails any test that causes an unhandled rejection. |
| [`examples/`](examples) | Importable example flows |
| [`docs/`](docs) | Architecture, offline queue, migration, troubleshooting and release guides |

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
for how the nodes are built and why. Changes are listed in [CHANGELOG.md](CHANGELOG.md); maintainers publish new
versions as described in [docs/RELEASING.md](docs/RELEASING.md).

## Security

Please report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md), not in public issues. In short:

- Keep secrets in the broker node's credentials or in environment variables (`${KAFKA_PASSWORD}`). Set a
  `credentialSecret` in `settings.js` so Node-RED encrypts them with your own key.
- Use TLS for anything that leaves the machine. SASL/PLAIN without TLS sends the password in clear text, and the node
  warns about it.
- The disk queue stores message contents unencrypted under the Node-RED user directory. Protect it, or use the memory
  queue.

## License & credits

[MIT](LICENSE) © Deepanshu Patel

- Built on [KafkaJS](https://github.com/tulios/kafkajs) (MIT), pinned at 2.2.4.
- Snappy compression through [snappyjs](https://github.com/zhipeng-jia/snappyjs) (MIT); the xerial framing is read
  the same way as in [kafkajs-snappy](https://github.com/tulios/kafkajs-snappy) (MIT). LZ4 compression through
  [lz4js](https://github.com/Benzinga/lz4js) (ISC).
- Inspired by [node-red-contrib-kafkajs](https://github.com/emrebekar/node-red-contrib-kafkajs) by Emre Bekar. This
  project is an independent rewrite and shares no code with it. Thank you for the original palette.
- Not affiliated with the Apache Software Foundation. Apache Kafka is a trademark of the Apache Software Foundation.
