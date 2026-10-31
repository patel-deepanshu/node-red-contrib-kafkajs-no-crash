# Migrating from node-red-contrib-kafkajs

This guide moves flows from [node-red-contrib-kafkajs](https://github.com/emrebekar/node-red-contrib-kafkajs) (by
Emre Bekar, the `kafkajs-client`, `kafkajs-producer` and `kafkajs-consumer` nodes) to this palette. A script
converts a `flows.json` file for you; the tables below show exactly what it does, so you can also migrate by hand.

- [Before you start](#before-you-start)
- [What behaves differently](#what-behaves-differently)
- [Using the migration script](#using-the-migration-script)
- [Property mapping](#property-mapping)
- [Migrating by hand](#migrating-by-hand)
- [Rolling back](#rolling-back)

## Before you start

- **Node-RED 3.1 or newer** is needed (and Node.js 18.5 or newer). Upgrade Node-RED first if you are on 3.0 or
  older; node-red-contrib-kafkajs itself works with the newer versions too.
- **Both palettes can be installed at the same time.** The node types are different (`kafkajs-nc-*` here), so
  nothing conflicts. Install this palette first, migrate, check, then uninstall the old one.
- **Do not run the old and the new consumer with the same group ID at the same time.** They would join the same
  consumer group and split the partitions between them, so each one sees only part of the messages.
- **Remove the old palette when you are done.** The version the palette manager installs (0.0.7) stops the whole
  Node-RED process whenever a producer cannot connect.
- **Back up** your Node-RED user directory (usually `~/.node-red`, `/data` in Docker): at least `flows*.json`,
  `flows*_cred.json` and `.config.runtime.json`.

## What behaves differently

Most differences are fixes. Some of them change what your flows receive, so check these after migrating:

| Topic | node-red-contrib-kafkajs | This palette | What to do |
|---|---|---|---|
| **Consumer output** | `msg.payload` is the whole kafkajs record: `msg.payload.value`, `msg.payload.key`, `msg.payload.headers`, `msg.payload.offset`, ... | `msg.payload` **is** the value. `msg.key`, `msg.headers`, `msg.offset`, `msg.timestamp`, `msg.partition`, `msg.topic` and `msg.kafka` carry the rest. | Change nodes after the consumer that read `msg.payload.value` and friends. The script lists the ones it finds. |
| **Empty group ID** | A new random group (`kafka_js_<uuid>`) on every deploy and restart: messages produced while Node-RED was down were skipped, and old groups piled up on the broker. | `node-red-<node id>`, the same across restarts and deploys. | Nothing, but note that the offsets of the old random groups do not carry over: the first start reads only new messages unless *Read from the beginning* is ticked. |
| **Clear offsets** | Reset the group to the **latest** offset on every deploy (skipping unread messages), and could crash Node-RED. | Removed. | Use the admin node's `resetOffsets` action, or `msg.action = "seek"`, when you really want to move a group. |
| **Advanced options (consumer)** | The checkbox decided whether *From beginning*, the commit settings and the timeouts were used at all. | Always used. | The script copies them only if the box was ticked, and uses the new defaults otherwise, which matches what the old node did. |
| **Producer output** | No output. | A delivery report per message (`msg.kafka.status` `sent` or `queued`). | Optional: wire it to something, or leave it unconnected. |
| **Kafka unreachable** | Messages dropped silently (or Node-RED crashed). | Stored in the offline queue on disk and sent in order later. | Check the *Offline queue* tab if you prefer *In memory* or *Off*. |
| **Payloads** | `0`, `""` and `false` were dropped silently; objects and numbers failed with a vague error. | Sent: objects as JSON, numbers and booleans as text, `null` as a tombstone. | Nothing. |
| **Static headers** | When static headers were set, `msg.headers` was ignored completely; otherwise `msg.headers` was sent as it was. | Static headers and `msg.headers` are merged when *Also send msg.headers* is ticked. | The script unticks it for producers that had static headers, which keeps the old behaviour. |
| **`msg.partition = 0`** | Ignored (0 is "falsy"). | Used. | Nothing. |
| **Acknowledgement "none"** | Broken: waited for answers that never came and wrote each message several times. | Real fire-and-forget (acks=0). | Consider *Leader* or *All* if you need to know that Kafka has the message. |
| **Topic auto-creation (producer)** | The setting was ignored: topics were always created (if the broker allows it). | The setting works. | The script turns it on, which keeps the old behaviour. |
| **Key → partition mapping** | 0.0.7 ran on kafkajs 1.x and its default partitioner. | *Partitioner: Default* (the Java client's mapping) or *Legacy* (the kafkajs 1.x mapping). | The script sets *Legacy*, so messages with the same key keep going to the same partition as before. New producers default to *Default*. |
| **TLS verification** | Inverted: with *Self Sign* unticked (the default) the broker certificate was **not** verified; ticking it turned verification on. | A standard *TLS configuration* node with *Verify server certificate*. | The script uses what *Self Sign* was meant to do ("allow untrusted certificates"): unticked → verify. If the status then shows `TLS: …`, fix the CA file or the server name. |
| **TLS files and passphrase** | File paths in the client node; all three (CA, certificate, key) were required; the passphrase was stored in plain text in `flows.json`. | A *TLS configuration* node; any combination; the passphrase is a credential. | The script creates the TLS configuration node. Enter the passphrase again unless you used `--credentials`. |
| **Errors** | `Kafka Producer Error` without the cause; Catch received an empty message. | The cause in the status, the log and `msg.error`; Catch receives the original message. | Nothing. |
| **Log level** | Ignored; kafkajs printed raw JSON to stdout. | Applied; kafkajs lines go to the Node-RED log. | Nothing. |

## Using the migration script

[`scripts/migrate-flows.js`](../scripts/migrate-flows.js) converts the three node types in a flows file, keeps every
other node as it is, and prints a report of everything that needs your attention. It only uses Node.js itself and
never changes the input file unless you ask for `--in-place`. The npm package installs it as the command
`kafkajs-nc-migrate-flows`:

```
kafkajs-nc-migrate-flows <flows.json> [--out <file>] [--credentials <flows_cred.json>] [--in-place]
```

| Option | Meaning |
|---|---|
| `<flows.json>` | The flows file: Node-RED's own (`flows.json`, or `flows_<hostname>.json` when *flowFile* is not set in `settings.js`), or an export (*Menu → Export → Download*). The admin API format `{ "rev": ..., "flows": [...] }` works too. |
| `--out <file>` | Where to write the result. Default: `<name>.migrated.json` next to the input. |
| `--credentials <file>` | The **unencrypted** credentials file. SASL usernames/passwords and the TLS passphrase are then converted too and written to `<out>_cred.json` (Node-RED's naming rule: `flows.migrated.json` → `flows.migrated_cred.json`), readable by the owner only. |
| `--in-place` | Replace the input (and the credentials file) instead, after copying it to `<file>.bak` (or `.bak.1`, `.bak.2`, ... if that exists). Stop Node-RED first. |
| `-h`, `--help` | Show the usage. |

Exit codes: `0` done (also when there was nothing to migrate; then nothing is written), `1` a file could not be read
or written, or the output would overwrite the input, `2` wrong options.

### Getting the script

The script ships with the npm package. Any of these works; it needs Node.js 18 or newer, which every Node-RED
installation that runs this palette already has.

- **With npx, without installing anything** (npx downloads the package into its cache):

  ```sh
  npx -p node-red-contrib-kafkajs-no-crash kafkajs-nc-migrate-flows ~/.node-red/flows.json
  ```

- **After installing the palette** in the Node-RED user directory (step 1 below), npm has put the command into
  `node_modules/.bin`:

  ```sh
  cd ~/.node-red
  npx kafkajs-nc-migrate-flows flows.json
  # the same: node node_modules/node-red-contrib-kafkajs-no-crash/scripts/migrate-flows.js flows.json
  ```

- **From a git checkout** of the repository:

  ```sh
  git clone https://github.com/patel-deepanshu/node-red-kafkajs-no-crash.git
  node node-red-kafkajs-no-crash/scripts/migrate-flows.js ~/.node-red/flows.json
  ```

- **As a single file**: the script has no dependencies, so a copy works on its own (`node migrate-flows.js ...`):

  ```sh
  curl -fsSLO https://raw.githubusercontent.com/patel-deepanshu/node-red-kafkajs-no-crash/main/scripts/migrate-flows.js
  node migrate-flows.js ~/.node-red/flows.json
  ```

With the official Docker image (user directory `/data` on the volume `node_red_data`) and the palette installed
there, either convert into a new file while the container runs and [import it](#importing-instead):

```sh
docker exec mynodered node /data/node_modules/node-red-contrib-kafkajs-no-crash/scripts/migrate-flows.js \
    /data/flows.json --out /data/flows-new.json
docker cp mynodered:/data/flows-new.json .    # the editor imports files from your computer
```

or stop the container and convert in place with a throw-away container on the same volume:

```sh
docker stop mynodered
docker run --rm -v node_red_data:/data --entrypoint node nodered/node-red \
    /data/node_modules/node-red-contrib-kafkajs-no-crash/scripts/migrate-flows.js /data/flows.json --in-place
docker start mynodered
```

### Step by step

1. **Install this palette** (*Menu → Manage palette → Install*, `node-red-contrib-kafkajs-no-crash`). Keep the old
   one installed for now.
2. **Stop Node-RED**, so that it does not overwrite the files while you work on them, and **back up** the user
   directory.
3. **Run the script.** In place (recommended, it keeps `.bak` copies):

   ```sh
   cd ~/.node-red
   npx kafkajs-nc-migrate-flows flows.json --in-place
   ```

   or into a new file that you import later (see [Importing instead](#importing-instead)):

   ```sh
   npx kafkajs-nc-migrate-flows flows.json --out flows-new.json
   ```

4. **Read the report.** It has three sections (see [The report](#the-report)). Fix the *Needs your attention* items
   in the editor after the next start.
5. **Start Node-RED.** Open each converted broker node, enter credentials and passphrases where the report asked for
   them, and deploy.
6. **Check** that every Kafka node turns green, and go through the *Check after deploying* items: the nodes after
   each consumer, TLS verification, the certificate files.
7. **Uninstall the old palette** (*Manage palette → Nodes → node-red-contrib-kafkajs → remove*) and restart
   Node-RED.

### Credentials

Flows files never contain passwords; Node-RED keeps them in `flows_cred.json`. In a default installation that
file is **encrypted** (with `credentialSecret` from `settings.js`, or a key Node-RED generated and stored in
`.config.runtime.json`). An encrypted file looks like `{"$":"6e1f..."}`.

- **Encrypted** (the usual case): the script recognises it, does not touch it, and asks you to enter the SASL
  username and password (and the TLS passphrase) again in each converted broker / TLS configuration node.
  Decrypting the file is out of scope for this script. You can also switch to environment variables at this point:
  Username `${KAFKA_USER}`, Password `${KAFKA_PASSWORD}`.
- **Unencrypted** (`credentialSecret: false` in `settings.js`): pass it with `--credentials`. The script renames
  `saslusername` / `saslpassword` to `username` / `password` for each converted broker, adds the TLS passphrase
  (which the old node kept in `flows.json`) to the new TLS configuration node, and leaves every other entry
  unchanged. The output is unencrypted like the input; Node-RED encrypts it on the next deploy if you configure a
  `credentialSecret`.

Until a SASL broker has its credentials, it shows `config error` (`SASL username is required`) and does not
connect; producers keep queueing messages meanwhile. Nothing crashes.

### The report

An example (long lines shortened with `…`), for a flow with three clients (none, SASL, TLS files), two producers
and two consumers, converted in place with an encrypted credentials file:

```
Converted 3 kafkajs-clients → kafkajs-nc-broker, 2 kafkajs-producers → kafkajs-nc-producer, 2 kafkajs-consumers → kafkajs-nc-consumer, and created 1 tls-config node.
Backup of the original flows:  flows.json.bak
Converted flows written to:     flows.json
Credentials: the file is encrypted and was not changed; enter the credentials again in the editor.
Producers use Partitioner "Legacy" (the key → partition mapping of node-red-contrib-kafkajs 0.0.7, which ran on kafkajs 1.x) and the offline queue on disk.

Needs your attention (5):
  - kafkajs-nc-broker "Cloud" (c_sasl): Enter the SASL username and password again (Security tab), or use ${KAFKA_USER} / ${KAFKA_PASSWORD}. …
  - tls-config "mTLS TLS" (c5c4ca56dbd5d727): Enter the private key passphrase again in the TLS configuration node …
  - kafkajs-nc-consumer "orders in" (k1): "Clear offsets" (…) does not exist any more. Use a kafka admin node with action resetOffsets, …
  - kafkajs-nc-consumer "orders in" (k1): The output changed: msg.payload is now the record value … Found: function "parse" (fn1) uses msg.payload.key, msg.payload.value.
  - all brokers: The credentials file is encrypted (credentialSecret), so this script cannot convert it: …

Check after deploying (3):
  - kafkajs-nc-broker "mTLS" (c_tls): TLS: the new TLS configuration verifies the broker certificate against the CA file …
  - tls-config "mTLS TLS" (c5c4ca56dbd5d727): Certificate files are read from /etc/kafka/ca.pem, /etc/kafka/client.pem, /etc/kafka/client.key …
  - kafkajs-nc-consumer (k2): The output changed: msg.payload is now the record value …

Good to know (6):
  - kafkajs-nc-broker "Cloud" (c_sasl): Retry factor 0.3 / multiplier 2 are not configurable any more (fixed 0.2 / 2). …
  - kafkajs-nc-producer "to orders" (p1): "Also send msg.headers" is off, as before: …
  …
```

| Section | Meaning |
|---|---|
| **Needs your attention** | Something you must do, or the node will not work as before: missing credentials or brokers, an invalid topic, removed options, nodes that read the old consumer output. |
| **Check after deploying** | Probably fine, but verify it: TLS verification, certificate files, the nodes after each consumer. |
| **Good to know** | Behaviour differences and adjusted values (clamped numbers, dropped settings). |

To find the nodes that read the old consumer output, the script follows the wires from each consumer (also
through link nodes and into subflows) and looks for `msg.payload.value`, `.key`, `.headers`, `.offset`,
`.timestamp` (and the other record fields) in the settings of every node it reaches: function code, change and
switch rules, templates. It cannot understand code, so it may miss usages (for example `const p = msg.payload;
p.value`) or list a usage that is fine. Look at the nodes after each consumer yourself as well.

### Importing instead

If you prefer not to edit files in the user directory, write the result to a new file (`--out`) and import it with
*Menu → Import*. The converted nodes keep their ids, so Node-RED 4 reports that some of the nodes already exist and
lets you choose to replace them (pick *replace* for all of them). The converted file contains no credentials: enter
them again in each broker node and TLS configuration node. Then deploy. Editing the files in place, with Node-RED
stopped, is the more predictable route.

### What the script does not do

- Change the code of function nodes, or any other node.
- Decrypt credentials, or create files: the certificate files named in the old client must exist on the machine
  that runs Node-RED.
- Follow references across files: convert the file that contains the client nodes together with the producers and
  consumers that use them.
- Carry consumer offsets over from the old random group IDs.

## Property mapping

The script keeps each node's `id`, `z` (flow), `g` (group), `x`, `y`, `wires`, `d` (disabled), `l` and `info`. Numbers
the old editor saved as text become numbers and are clamped to the new ranges (with a note in the report);
`${VAR}` references are kept as they are.

### `kafkajs-client` → `kafkajs-nc-broker` (+ `tls-config`)

| Old property | New property | Conversion |
|---|---|---|
| `name` | `name` | as is |
| `brokers` | `brokers` | a string or an array → one list `host:port, host:port`; spaces and empty entries removed; a `${VAR}` is kept. Empty → reported. |
| `clientid` | `clientId` | empty → `node-red` |
| `connectiontimeout` | `connectionTimeout` | number, 100 – 300000 ms |
| `requesttimeout` | `requestTimeout` | number, 1000 – 600000 ms |
| `advancedretry` | – | decides whether the three retry values below are taken |
| `retries`, `initialretrytime`, `maxretrytime` | `retries`, `initialRetryTime`, `maxRetryTime` | numbers (0 – 50, 50 – 60000, 100 – 600000), only if *Advanced retry* was ticked; otherwise 5 / 300 / 30000, the kafkajs defaults the old node used |
| `factor`, `multiplier` | – | not configurable (fixed 0.2 / 2); reported when they differed |
| `auth` = `none` | `authMechanism` = `none`, `tls` = `false` | |
| `auth` = `sasl` | `authMechanism` = `saslmechanism` (`plain`, `scram-sha-256`, `scram-sha-512`; empty → `plain`), `tls` = `saslssl` | a missing `saslssl` means TLS off, as in the old runtime |
| `auth` = `tls` | `tls` = `true`, `tlsConfig` = the id of a new `tls-config` node, `authMechanism` = `none` | no TLS configuration node when no file was set (then the system CAs are used, and it is reported) |
| `tlscacert`, `tlsclientcert`, `tlsprivatekey` | `ca`, `cert`, `key` of the `tls-config` node | file paths, trimmed. Only a certificate or only a key is reported. |
| `tlsselfsign` | `verifyservercert` of the `tls-config` node | **inverted**: unticked → `true` (verify), ticked → `false` |
| `tlspassphrase` | credential `passphrase` of the `tls-config` node | removed from the flow; with `--credentials` moved, otherwise reported |
| `loglevel` | `logLevel` | `error`, `warn`, `info`, `debug` as is; `nothing` → `off`; anything else → `warn` |
| credentials `saslusername`, `saslpassword` | credentials `username`, `password` | with `--credentials` (unencrypted file), otherwise reported |
| – | `authenticationTimeout`, `reconnectMin`, `reconnectMax`, `authRetryMin`, `safetyNet` | new settings with their defaults (10000, 1000, 60000, 60000 ms, on) |

The new `tls-config` node is named `<client name> TLS`, is placed right after the broker in the file, and gets the
same flow (`z`) as the client.

### `kafkajs-producer` → `kafkajs-nc-producer`

| Old property | New property | Conversion |
|---|---|---|
| `name` | `name` | as is |
| `client` | `broker` | same id. Empty, missing from the file, or not a client → reported. |
| `topic` | `topic` | trimmed; a name the new node would reject (e.g. `sensors/temp`) is reported |
| `key` | `key` | `null` → empty (use `msg.key`) |
| `partition` | `partition` | `null`/empty → empty (use `msg.partition` or the partitioner); a whole number → text; anything else → empty, reported |
| `headeritems` | `headers` | same `{ name: value }` object; rows without a name are dropped |
| – | `useMsgHeaders` | `false` when there were static headers (the old node then ignored `msg.headers`), otherwise `true` |
| `acknowledge` | `acks` | `all`, `leader`, `none` as is; missing or unknown → `all` |
| `responsetimeout` | `ackTimeout` | number, 100 – 600000 ms |
| `allowautotopiccreation` | `allowAutoTopicCreation` | always `true`: the old node ignored the setting and topics were auto-created |
| `advancedoptions`, `transactiontimeout`, `metadatamaxage` | – | dropped (they had no effect) |
| – | `partitioner` | `legacy` (the kafkajs 1.x mapping of 0.0.7) |
| – | `compression`, `batchSize`, `queueMode`, `queueDir`, `queueName`, `queueMaxMB`, `queueMaxMessages`, `queueOverflow`, `queueMaxAgeHours`, `queueFsync` | new settings with their defaults (none, 100, disk, empty, empty, 100, 0, drop-oldest, 0, on) |
| `wires: []` | `wires: [[]]` | the new node has an output (the delivery report), unconnected |

### `kafkajs-consumer` → `kafkajs-nc-consumer`

| Old property | New property | Conversion |
|---|---|---|
| `name` | `name` | as is |
| `client` | `broker` | same id, checked as for the producer |
| `topic` | `topics` | trimmed; missing or invalid → reported |
| `groupid` | `groupId` | trimmed; empty stays empty (the new node then uses `node-red-<node id>`) |
| `advancedoptions` | – | decides whether the values below are taken; if it was off, the new defaults are used (the old node ignored the values too) |
| `frombeginning` | `fromBeginning` | |
| `autocommitinterval` | `autoCommitInterval` | number, 0 – 86400000 ms |
| `autocommitthreshold` | `autoCommitThreshold` | number |
| `sessiontimeout`, `rebalancetimeout`, `heartbeatinterval` | `sessionTimeout`, `rebalanceTimeout`, `heartbeatInterval` | numbers; a heartbeat interval not below the session timeout is reported (the node then uses a third of the session timeout) |
| `maxbytesperpartition`, `maxbytes`, `maxwaittimeinms` | `maxBytesPerPartition`, `maxBytes`, `maxWaitTimeInMs` | numbers |
| `allowautotopiccreation` | `allowAutoTopicCreation` | |
| `metadatamaxage`, `minbytes` | – | not configurable; reported if they differed from the kafkajs defaults |
| `clearoffsets` | – | removed; reported when it was ticked |
| – | `topicIsRegex`, `autoStart`, `payloadFormat`, `keyFormat`, `headersFormat`, `commitMode`, `partitionsConsumedConcurrently` | new settings with their defaults (off, on, string, string, string, auto, 1) |

## Migrating by hand

If you have only a few nodes, you can also rebuild them in the editor: the [README](../README.md#migrating-from-node-red-contrib-kafkajs)
lists the steps. Use the tables above for the values, and remember the three things the script would otherwise do
for you: invert *Self Sign* into *Verify server certificate*, choose *Partitioner: Legacy* if keys must keep their
partitions, and untick *Also send msg.headers* on producers that have static headers.

## Rolling back

- With `--in-place`, the originals are in `flows.json.bak` and `flows_cred.json.bak` (or `.bak.1`, ...). Stop
  Node-RED, copy them back, and start it again.
- Otherwise the input files were never changed: delete the `.migrated` files.
- The disk queues of converted producers are in `<user directory>/kafkajs-nc-queue/<node id>/`. If you roll back
  while they still hold messages, those messages are not sent by the old palette.
