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
- node-red-kafkajs-no-crash version:
- Node-RED version:
- Node.js version (`node -v`):
- OS / hardware (e.g. Raspberry Pi 4, Docker image, Windows 11):

**Logs**
Set the broker node's *Log level* to `debug`, reproduce, and paste the relevant part of the Node-RED log.

```
paste logs here
```
