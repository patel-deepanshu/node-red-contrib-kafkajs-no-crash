---
name: Bug report
about: Something does not work as documented (crash, lost message, wrong output…)
title: "[bug] "
labels: bug
---

**What happened**
A clear description of the problem. If Node-RED crashed, paste the full log output around the crash.

**What you expected**

**How to reproduce**
1. Flow (export the relevant nodes with *Export → Selected nodes* and paste the JSON — **remove any passwords/keys first**)
2. Steps / input messages
3. Broker setup (Kafka version, SASL mechanism, TLS yes/no, cloud provider…)

**Environment**
- node-red-contrib-kafkajs-no-crash version:
- Node-RED version (3.1 or newer is supported):
- Node.js version (`node -v`):
- OS / hardware (e.g. Raspberry Pi 4, Docker image, Windows 11):
- How Node-RED is started, if not with the defaults (e.g. `NODE_OPTIONS=--unhandled-rejections=...`):

**Logs**
Turn on debug logging as described in
[docs/TROUBLESHOOTING.md](https://github.com/patel-deepanshu/node-red-contrib-kafkajs-no-crash/blob/main/docs/TROUBLESHOOTING.md#enabling-debug-logging):
set the broker node's *kafkajs log level* (Advanced tab) to `debug`, **and** set `logging.console.level` to `'debug'`
in Node-RED's `settings.js` (restart Node-RED), otherwise debug lines are not printed. Reproduce, and paste the
relevant part of the Node-RED log, **with secrets removed**. For the producer, the output of
`msg.action = "queue-status"` helps too.

```
paste logs here
```
