# Kafka reference workloads for Antithesis

Two self-contained Antithesis workloads for Apache Kafka 4.3.1 (KRaft, three
brokers). Each workload is one long-running Go process that drives Kafka and
checks a short list of properties with the Antithesis SDK:

- **`at-least-once/`** retries failed sends; messages may land more than once.
- **`at-most-once/`** never retries a failed send; messages may be lost, never
  duplicated.

Pick the one that matches the delivery guarantee you want to exercise. The two
share the same Kafka image and the same shape; the differences are listed below.

## 1. Properties

| Property | Safety / liveness |
|---|---|
| No phantom messages — every consumed message was sent by the producer | safety |
| Message ordering — messages are consumed in the order they were produced | safety |
| No data corruption — every consumed message matches its checksum | safety |
| No data loss (safety side) — the consumer never skips past an acknowledged message | safety |
| Delivery guarantee (`n` ≤ `m`) — consumed no more times than sent | safety |
| Delivery guarantee (`p` ≤ `n`) — every acknowledged message is consumed | liveness (checked once per fault-free window) |
| Producer / consumer progress — both make progress during every fault-free window | liveness |

For `at-least-once`, "every acknowledged message is consumed" means "at least
once". For `at-most-once`, it means exactly "is consumed" (and the ordering
check is strict: a counter cannot repeat).

## 2. Pick the workload that matches your delivery guarantee

Run **one** workload at a time. They share Kafka container names and will
collide if you try to run both locally in parallel.

| If you want to test... | Run |
|---|---|
| A producer that retries on failure and tolerates duplicates on the consumer side | `at-least-once/` |
| A producer that never retries, where any failure is a lost message | `at-most-once/` |

## 3. Layout and images

```
kafka/
  Dockerfile                  shared Kafka image, cataloged for instrumentation
at-least-once/
  config/
    docker-compose.yaml       3 brokers + 1 workload + 4 volumes
    Dockerfile                config image (compose file packaged as /docker-compose.yaml)
  workload/
    Dockerfile                multi-stage Go build with antithesis-go-toolexec
    go.mod go.sum
    main.go producer.go consumer.go checker.go helpers.go
at-most-once/                 same layout, same files
```

| Image | Role |
|---|---|
| `kafka-workload-kafka:latest` | Kafka 4.3.1 broker, with instrumented jars under `/opt/antithesis/catalog/kafka/` |
| `kafka-at-least-once-workload:latest` | at-least-once workload binary |
| `kafka-at-most-once-workload:latest` | at-most-once workload binary |
| `kafka-at-least-once-config:latest` | at-least-once config image |
| `kafka-at-most-once-config:latest` | at-most-once config image |

## 4. Instrumentation

**Kafka (Java).** Antithesis instruments the jars it finds under
`/opt/antithesis/catalog/`. The shared Kafka image moves Kafka's own jars
(`kafka_2.13-*.jar`, `kafka-clients-*.jar`, and the eight other
`kafka-*-*.jar` modules) into `/opt/kafka/instrumented-libs/` and symlinks the
catalog entry there, so only Kafka's code is cataloged. Scala, Jackson, Netty,
zstd and the other transitive jars remain uninstrumented. The image sets
`CLASSPATH="/opt/kafka/instrumented-libs/*"` so `kafka-run-class.sh` loads the
instrumented copies first.

**Workload (Go).** The workload image installs
`antithesis-go-toolexec@v0.8.0` and builds the Go binary with
`go build -toolexec=antithesis-go-toolexec`. Symbol output lands in `/symbols/`
and is copied into the runtime image.

## 5. System under test

Each workload drives a three-broker Kafka cluster running Apache Kafka 4.3.1 in
KRaft mode:

- `KAFKA_PROCESS_ROLES=broker,controller`
- `unclean.leader.election.enable=false`
- `min.insync.replicas=2`
- 3× replication for user topics and for the offsets / transaction state logs
- `auto.create.topics.enable=false` (the workload creates the topic itself)

The workload talks to the brokers over PLAINTEXT on port 9092.

## 6. How a workload works

**Messages.** The producer sends one message at a time, keyed
`producer-1-<N>` where N starts at 1 and increments. Each record carries a
100–200-char alphanumeric body (length and characters picked via the
Antithesis SDK's `random.GetRandom` and `random.RandomChoice`) and a
`checksum` header containing the lowercase hex SHA-256 of the body.

**Producer.** Runs in its own goroutine. For every counter it builds the uuid,
body and checksum, writes `<uuid>, attempt` to `/state/producer-1.log`,
calls `producer.attempted` on the checker, then `ProduceSync` to partition 0.
On success it writes `<uuid>, ack` and calls `checker.acked`. On failure
it fires `Reachable("Producer send attempt failed")` and sleeps 500 ms.
`at-least-once` repeats up to five times for a given counter; `at-most-once`
moves on after a single failure. `acks=all`, idempotence on by default,
delivery timeout 10 s.

**Consumer.** Direct partition consumption (no consumer group). For every
record it computes the SHA-256 of the body, asserts
`header == hex(sha256(body))`, calls `checker.checkConsumed`, then writes
`<uuid>, <offset>` to `/state/consumer-1.log`. The check comes before the
log line so a crash between them leaves the state file lagging the assertion,
never ahead of it.

**State files.** Both logs are append-only with `fsync` after every line. On
startup the workload truncates any torn trailing line so later appends can't
fuse onto a partial record.

**Checker.** Three maps keyed by uuid — `attempts` (`m`), `acks` (`p`),
`consumed` (`n`) — plus `lastConsumed`, `highestAcked`, `totalAcks` and
`totalReads`. Guarded by a single mutex. See §7 for the checks it runs.

**Surviving restarts.** On startup, `main.go`:
1. opens both state files (torn trailing line truncated);
2. replays the producer log first, calling `attempted` and `acked` on the
   checker so its maps match reality before any consumer line is replayed;
3. replays the consumer log, calling `checkConsumed` on each line (safe
   because the producer log has already been replayed);
4. computes `nextCounter` and `nextOffset` from the last complete lines;
5. fires `Reachable` for the two "resumed from state file" markers if the
   logs weren't empty;
6. logs `starting: next uuid producer-1-<N>, next offset <M>`;
7. fires `Reachable("Workload started …")`, calls `lifecycle.SetupComplete`,
   logs `setup complete`;
8. starts the producer and consumer goroutines, then runs the fault-free
   window loop in the main goroutine.

## 7. How each property is checked

| Property | Check | When |
|---|---|---|
| No phantom messages | `attempts[uuid] > 0` in `checkConsumed` | on every consumed record |
| Message ordering | `counter >= lastConsumed` (`>` for at-most-once) | on every consumed record |
| No data corruption | `header == hex(sha256(body))` in `handle` | on every consumed record |
| No data loss (safety) | no acked counter sits in `(lastConsumed, counter)` | on every consumed record |
| `n` ≤ `m` | `AlwaysLessThanOrEqualTo(consumed[uuid], attempts[uuid])` | on every consumed record |
| `p` ≤ `n` | every acked counter ≤ `highestAckedBefore` has `consumed[uuid] ≥ acks[uuid]` | once per fault-free window |
| Producer / consumer progress | `totalAcks`, `totalReads`, `lastConsumed` all advanced during the window | once per fault-free window |

The fault-free window loop sleeps for `FAULT_FREE_INTERVAL`, invokes
`$ANTITHESIS_STOP_FAULTS <duration-seconds>` if set (Antithesis sets it; locally
nothing does), fires `Reachable("A fault-free window started")`, then polls
once a second for the three progress indicators to advance (bounded by
`duration - 1s`). It then runs the three `Always` progress checks and the
`p ≤ n` check.

## 8. Differences between the two workloads

Starting from the same five-file package, only the following lines differ:

| File | at-least-once | at-most-once |
|---|---|---|
| `go.mod` | `module .../at-least-once` | `module .../at-most-once` |
| `main.go` | `topic = "at-least-once"`, has `maxRetries = 5` | `topic = "at-most-once"`, no `maxRetries` |
| `producer.go` | retry loop `attempt := 1 .. 1+maxRetries`, Reachables for "retried" and "gave up", D3 "acked after failures" debug line, rebuild-after-timeout per D2 is off | one send, no retry loop, no "retried" / "gave up" Reachables |
| `checker.go` ordering | `counter >= lastConsumed` (equal allowed for duplicates) | `counter > lastConsumed` (strict) |
| `checker.go` `n ≤ m` | message says "at least once" | message says "at most once" |
| `checker.go` duplicate | `Reachable("… a retried message was consumed more than once")` | removed |
| `checker.go` `p ≤ n` | "every acknowledged message is consumed at least once" | "every acknowledged message is consumed" |
| `config/docker-compose.yaml` | `kafka-at-least-once*` names, `../workload` build context | `kafka-at-most-once*` names, `../workload` build context |

Everything else — the Kafka image, state file formats, replay logic, and the
other six properties — is identical between the two.

## 9. Running locally

```sh
# Pick ONE workload. Both use the same Kafka container names, so running them
# simultaneously will collide.
cd /path/to/mq-workload

docker compose -f at-least-once/config/docker-compose.yaml up -d --build
# or
docker compose -f at-most-once/config/docker-compose.yaml up -d --build

docker logs -f at-least-once-workload
# or
docker logs -f at-most-once-workload

# Stop & remove volumes:
docker compose -f at-least-once/config/docker-compose.yaml down -v
```

Env knobs:

- `FAULT_FREE_INTERVAL` (default `5m`) — gap between fault-free windows.
- `FAULT_FREE_DURATION` (default `60s`) — length of each window.

Locally, nothing pauses fault injection (there is none to pause). The
fault-free window checks still run — the workload just logs a line saying it is
checking liveness without pausing faults.

If you stop brokers by hand (`docker stop kafka-2 kafka-3`), expect the
liveness assertions to fail while they're down. That is not a bug in the
workload; it is the system under test being unavailable.

## 10. Running in Antithesis

```sh
# Build everything (both config and workload images per workload):
docker compose -f at-least-once/config/docker-compose.yaml build

# Validate the harness locally:
snouty validate at-least-once/config

# Submit the run (replace tenant/duration metadata as needed; do NOT run this
# as part of CI unless you intend to spend run budget):
#   snouty launch at-least-once/config --duration <minutes> ...
```

Repeat for `at-most-once/config` to validate or launch the other workload.
