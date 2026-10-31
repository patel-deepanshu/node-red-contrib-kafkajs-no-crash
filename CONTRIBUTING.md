# Contributing

Thank you for helping with node-red-contrib-kafkajs-no-crash. Bug reports, documentation fixes, example flows, test
results from your own Kafka setup and code are all welcome.

Everyone taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

- [Ways to contribute](#ways-to-contribute)
- [Development setup](#development-setup)
- [Project layout](#project-layout)
- [Running the tests](#running-the-tests)
- [The prime directive: never crash Node-RED](#the-prime-directive-never-crash-node-red)
- [Code style](#code-style)
- [Adding a new option: checklist](#adding-a-new-option-checklist)
- [Commits and pull requests](#commits-and-pull-requests)
- [Release process](#release-process)
- [Reporting bugs](#reporting-bugs)

## Ways to contribute

- **Report a bug** or **suggest a feature** with the issue templates on GitHub.
- **Improve the documentation**: the README, the guides in [`docs/`](docs), and the help text of each node (the
  Markdown block at the end of `nodes/*.html`, shown in Node-RED's help sidebar).
- **Add or improve an example flow** in [`examples/`](examples).
- **Test against your setup** (a cloud service, an unusual network, a Raspberry Pi) and tell us what worked and what
  did not.
- **Fix a bug or build a feature.** For anything bigger than a small fix, open an issue first so that we can agree on
  the approach before you spend time on it.

## Development setup

You need Node.js 18.5 or newer (Node.js 22 or 24 recommended; Node-RED 5 needs 22.9 or newer), npm and git. For the
end-to-end tests you also need bash, curl and a Java runtime (CI uses Java 21). The palette supports Node-RED 3.1 or
newer (`node-red.version` in `package.json`): 3.0 gives nodes inside subflows a new id on every start, which the
default queue directory and consumer group depend on.

```sh
git clone https://github.com/patel-deepanshu/node-red-kafkajs-no-crash.git
cd node-red-kafkajs-no-crash
npm install
npm test
```

To try your working copy in a real Node-RED, install it into your Node-RED user directory and restart Node-RED:

```sh
cd ~/.node-red
npm install /path/to/node-red-kafkajs-no-crash
```

After changing a `.js` file, restart Node-RED; after changing a `.html` file, restart Node-RED and reload the editor.

## Project layout

| Path | Content |
|---|---|
| [`nodes/`](nodes) | The four nodes: runtime (`*.js`) and editor definition plus help text (`*.html`) |
| [`lib/`](lib) | Shared modules: configuration, error classification, queues, serialisation, logging, safety net |
| [`test/unit/`](test/unit) | Tests of `lib/`, the example flows, the migration script and the documentation snippets |
| [`test/nodes/`](test/nodes) | The nodes in node-red-node-test-helper, with the fake kafkajs |
| [`test/e2e/`](test/e2e) | The nodes against a real Kafka broker |
| [`test/helpers/`](test/helpers) | The crash trap (`setup.js`), the fake kafkajs, small helpers |
| [`examples/`](examples) | Example flows (Node-RED shows them under *Import → Examples*) |
| [`docs/`](docs) | [Architecture](docs/ARCHITECTURE.md), [offline queue](docs/OFFLINE-QUEUE.md), [migration](docs/MIGRATION.md), [troubleshooting](docs/TROUBLESHOOTING.md) |
| [`scripts/kafka/`](scripts/kafka) | A local Kafka broker for the end-to-end tests |
| [`scripts/migrate-flows.js`](scripts/migrate-flows.js) | Converts flows from node-red-contrib-kafkajs |

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before changing a node: it explains the structure every node
follows and the kafkajs behaviours the code works around.

## Running the tests

### Unit and node tests (no Kafka needed)

```sh
npm test                 # everything in test/**/*.spec.js
npm run test:unit        # test/unit only
npm run test:nodes       # test/nodes only
npm run lint             # ESLint
npx mocha test/unit/disk-queue.spec.js            # one file
npx mocha test/nodes/producer.spec.js -g "queue"  # tests whose title matches
```

`.mocharc.json` loads [`test/helpers/setup.js`](test/helpers/setup.js) for every run. It fails any test during
which an unhandled rejection or an uncaught exception happens, because in production each of them would stop
Node-RED. If you see a `CrashTrapError`, you have found a crash bug, not a flaky test.

`npm test` uses the Node-RED version from `devDependencies` (4.1). CI also runs the suite with Node-RED 3.1 (the
oldest supported version) and 5. To do that locally:

```sh
npm install --no-save node-red@3.1   # or node-red@5 (needs Node.js 22.9+)
npm test
npm ci                             # back to the locked versions
```

### End-to-end tests (real Kafka)

The scripts in [`scripts/kafka/`](scripts/kafka) download Apache Kafka (default 3.9.1, SHA-512 checked) and run a
single-node KRaft broker with two listeners: `PLAINTEXT` on `127.0.0.1:19092`, and `SASL_PLAINTEXT` on
`127.0.0.1:19093` with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 and the users `admin`/`admin-secret` and
`alice`/`alice-secret`.

```sh
export KAFKA_HOME="$(bash scripts/kafka/kafka-download.sh | tail -n1)"
npm run kafka:start
KAFKA_E2E_BROKERS=127.0.0.1:19092 KAFKA_E2E_SASL_BROKERS=127.0.0.1:19093 KAFKA_E2E_CONTROL=1 npm run test:e2e
npm run kafka:stop
```

Environment variables of the tests (documented in [`test/e2e/e2e-helpers.js`](test/e2e/e2e-helpers.js)):

| Variable | Meaning |
|---|---|
| `KAFKA_E2E_BROKERS` | `PLAINTEXT` bootstrap list, e.g. `127.0.0.1:19092`. Without it, every e2e test is skipped. |
| `KAFKA_E2E_SASL_BROKERS` | A `SASL_PLAINTEXT` listener with PLAIN and SCRAM-SHA-256/512, e.g. `127.0.0.1:19093` (for `sasl.e2e.js`). |
| `KAFKA_E2E_SASL_USER`, `KAFKA_E2E_SASL_PASSWORD` | A user known to every mechanism (default `alice` / `alice-secret`, as created by `kafka-start.sh`). |
| `KAFKA_E2E_CONTROL=1` | Allows the outage tests to stop, kill (`SIGKILL`), freeze (`SIGSTOP`) and restart the broker with `scripts/kafka/*.sh`. Needs `KAFKA_HOME` and a broker started by `npm run kafka:start`. |

Environment variables of the scripts (each script documents its own at the top; the shared ones are in
[`kafka-env.sh`](scripts/kafka/kafka-env.sh)):

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_HOME` | (required) | An extracted Kafka distribution; `kafka-download.sh` prints it as its last line |
| `KAFKA_VERSION`, `KAFKA_SCALA` | `3.9.1`, `2.13` | What `kafka-download.sh` downloads |
| `KAFKA_CACHE_DIR` | `~/.cache/kafka-e2e` | Where downloads are kept |
| `KAFKA_MIRROR` | `https://archive.apache.org/dist/kafka` | Download mirror (any URL curl understands, including `file://`) |
| `KAFKA_BASE_DIR` | `<repo>/.kafka-local` | Configuration, data, logs and pid file of the broker (git-ignored) |
| `KAFKA_HOST`, `KAFKA_PLAINTEXT_PORT`, `KAFKA_SASL_PORT`, `KAFKA_CONTROLLER_PORT` | `127.0.0.1`, `19092`, `19093`, `19094` | Listeners |
| `KAFKA_ADMIN_USER` / `KAFKA_ADMIN_PASSWORD`, `KAFKA_USER` / `KAFKA_PASSWORD` | `admin` / `admin-secret`, `alice` / `alice-secret` | SASL users |
| `KAFKA_AUTO_CREATE_TOPICS`, `KAFKA_NUM_PARTITIONS` | `true`, `1` | Broker topic defaults |
| `KAFKA_HEAP_OPTS`, `KAFKA_READY_TIMEOUT` | `-Xms256m -Xmx512m`, `90` | JVM heap; seconds to wait for readiness |

| Script | Purpose |
|---|---|
| `kafka-download.sh` | Download, verify and extract Kafka; print `KAFKA_HOME` |
| `kafka-start.sh` (`npm run kafka:start`) | Start the broker (formats the storage once), wait until both listeners answer |
| `kafka-stop.sh [--kill]` (`npm run kafka:stop`) | Controlled shutdown, or `SIGKILL` with `--kill` |
| `kafka-pause.sh pause\|resume` | Freeze and unfreeze the broker (`SIGSTOP`/`SIGCONT`): sockets stay open, nothing answers |
| `kafka-status.sh` | Check the process and every listener and mechanism |
| `kafka-create-topics.sh name[:partitions] ...` | Create topics |
| `kafka-add-scram-user.sh <user> <password>` | Add SCRAM credentials |
| `kafka-reset.sh` | Stop the broker and delete all its state |

The e2e tests use unique topic and group names per run, so they can run repeatedly against the same broker. You can
point `KAFKA_E2E_BROKERS` at another test cluster (the tests create and delete topics there); leave
`KAFKA_E2E_CONTROL` unset unless the tests may stop that broker. Broker logs are in `.kafka-local/logs/`.

## The prime directive: never crash Node-RED

Node-RED exits the whole process on an uncaught exception and, on Node.js 15+, on an unhandled promise rejection.
Every flow on the instance stops. The rules below exist because each of them was a real crash in the palette this
project replaces, or in kafkajs. The reasons are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

1. **No `async` input or close handlers.** Write synchronous handlers that call async work with an explicit
   `.catch()`:

   ```js
   node.on('input', function (msg, send, done) {
       try {
           runtime.input(msg, send, done);          // never throws, calls done exactly once
       } catch (err) {
           done(err);
       }
   });
   ```

2. **Every promise gets a rejection handler**, including fire-and-forget ones. `withTimeout()` from `lib/util.js`
   never rejects and is the usual way to wait for something that may hang.
3. **Register kafkajs listeners only with `safeOn()`** (`lib/util.js`). kafkajs only catches asynchronous rejections
   of listeners; a synchronous throw crashes the process.
4. **Wrap timer callbacks** in `try/catch` (a throw in a timer is an uncaught exception), `unref()` timers that must
   not keep the process alive, and clear every timer in the close handler.
5. **`done()` rules.** Every input message ends in exactly one `done()` or `done(err)` (use `once()`). Message
   problems go to `done(err)`, so that a *catch* node gets the original message. `node.error(text, msg)` only reaches
   *catch* nodes when `msg` is a message object, never a string or an Error. `done(err)` can itself throw when the
   message cannot be cloned: call it through the existing safe wrappers.
6. **Coerce numbers.** The editor saves numbers as strings, and kafkajs breaks on them. Every value passed to kafkajs
   goes through `toInt()` / `toNumber()` / `toBool()` with a default and a range, and the editor validates the same
   range.
7. **Never throw from `eachMessage`** (kafkajs then re-delivers the message forever or stops the consumer), from a
   kafkajs `logCreator`, or from `restartOnFailure` (which must be an `async` function).
8. **Wrap synchronous kafkajs calls** that can throw: `new Kafka()`, `kafka.producer()` / `consumer()` / `admin()`,
   `pause()`, `resume()`, `seek()`, `on()`.
9. **Constructors:** register the close handler first, create resources last, start the real work in
   `setImmediate()`, and never let the constructor throw.
10. **Generations:** after every `await` in connection code, check whether the node was closed or the attempt was
    superseded (`stale(gen)`), and disconnect a client that finished connecting too late.
11. **Bounded close:** the close handler calls `done()` within 10 s whatever happens; every `disconnect()` is bounded;
    `status.dispose()` stops later status updates.
12. **Do not rely on the safety net.** It is a second line of defence; the tests fail on any rejection it would
    have to catch.
13. **`lib/` functions do not throw** on bad input unless their JSDoc says so.

A test that proves a new failure path is safe usually needs only the fake kafkajs: make it fail
(`fake.behaviour.connectError = fake.errors.connectionRefused()`, `sendDelayMs: Infinity`, ...) and assert on
status, log and `done(err)`. The crash trap does the rest.

## Code style

- CommonJS, `'use strict';` at the top of every file, 4-space indentation, single quotes, semicolons, `const` and
  `let`. No TypeScript, no build step.
- JSDoc on exported functions and classes. Comments explain **why**, not what.
- `npm run lint` must pass (ESLint 9, [`eslint.config.js`](eslint.config.js)); `console` is not allowed outside the
  tests (use `node.log` / `node.warn` / `node.error`, or `process.stdout.write` in scripts).
- kafkajs is only `require`d in [`lib/kafka-factory.js`](lib/kafka-factory.js).
- Runtime dependencies must be pure JavaScript (the palette installs on Raspberry Pis and Alpine containers without a
  compiler). Discuss a new dependency in an issue first.
- Node type names (`kafkajs-nc-*`) and property names in the flow JSON are a public interface: changing them breaks
  users' flows.
- User-facing texts (status, log, errors, help) are short, plain English and say what to do.

## Adding a new option: checklist

1. **Editor** (`nodes/<node>.html`): add the property to `defaults` with its default value and a `validate`
   function, add the form row, give older flows without the property a sensible value in `oneditprepare` if needed,
   and document it in the help text.
2. **Runtime** (`nodes/<node>.js`, or [`lib/client-config.js`](lib/client-config.js) `DEFAULTS`/`LIMITS` for the
   broker): parse it with `toInt` / `toNumber` / `toBool`, a default and a range; never pass the raw value to kafkajs.
3. **Tests:** parsing (number, numeric string, blank, out of range, `${ENV}`) and behaviour in `test/nodes/`; an e2e
   test if it changes what goes over the network.
4. **Examples:** every node of the palette in [`examples/`](examples) must contain every property
   (`test/unit/examples.spec.js` checks this), so add the property with its default to each example node of that type.
5. **Migration script:** [`scripts/migrate-flows.js`](scripts/migrate-flows.js) keeps a copy of the editor defaults
   (`test/unit/migrate-flows.spec.js` compares them). Add the default there, and a mapping if the old palette had an
   equivalent setting.
6. **Documentation:** the README settings table (and the status or FAQ sections if relevant), the guides in
   [`docs/`](docs) if behaviour changes, and the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md).

**Removing an option** works the other way round: remove it from the editor `defaults`, the help, the examples, the
migration script's defaults and the documentation, but keep the runtime accepting flows that still contain it.
Ignore the old value and log one warning at deploy that says what happens instead (see how the producer handles the
former `idempotent` property), and test that such a flow still loads.

## Commits and pull requests

- Fork, create a branch from `main`, and keep each pull request to one topic.
- Write commit subjects in the imperative, describing the effect (`Retry missing topics with normal backoff`), and
  explain the reason in the body when it is not obvious.
- Add tests for every fix and feature. A bug fix should come with a test that fails without it.
- Run `npm run lint` and `npm test` before pushing; run the e2e tests too when you change networking, the queue or
  close handling.
- Fill in the pull request template, link the issue (`Fixes #12`), and update the documentation and the changelog.
- Never commit secrets. Tests and examples use placeholders or `${ENV}` references.
- CI (lint, the test matrix and the e2e job) must be green before a pull request is merged.

## Release process

For maintainers: [docs/RELEASING.md](docs/RELEASING.md) has the complete steps. In short:

1. `main` is green in CI (including the e2e job) and `npm run audit:runtime` reports 0 vulnerabilities.
2. `npm version patch|minor|major --no-git-tag-version`, and the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md)
   becomes the new version with today's date. Merge that into `main`.
3. Publish a GitHub release with the tag `vX.Y.Z` (matching `package.json`). The
   [release workflow](.github/workflows/release.yml) runs the checks and the tests and publishes to npm with
   provenance (trusted publishing, no token). The very first release is published by hand.
4. Use *request refresh* on the package's page in the Node-RED flow library.

## Reporting bugs

Open an issue with the bug report template at
<https://github.com/patel-deepanshu/node-red-kafkajs-no-crash/issues>. Please include the versions of this palette,
Node-RED and Node.js, the platform, the broker setup (Kafka version or cloud service, listener, SASL mechanism, TLS),
a flow export with **all secrets removed**, and the log around the problem. [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md#enabling-debug-logging)
explains how to turn on debug logging.

Security problems must not go into public issues: follow [SECURITY.md](SECURITY.md).
