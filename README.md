# Test message queues

This repository provides a set of property-based tests for Apache Kafka using Antithesis. It checks for seven properties most, if not all, message queues should exhibit:

- No phantom messages (Safety)
- Messages are consumed in the order they were produced (Safety)
- No data corruption (Safety)
- No data loss (Safety)
- Messages are consumed no more times than they were sent (Safety)
- Every acknowledged message gets consumed (Liveness)
- Producers and consumers make progress (Liveness)

There are two workloads, one for each delivery guarantee. Pick the one you want to test:

- `at-least-once/`: the producer retries failed sends, so messages may show up more than once, but should never be lost.
- `at-most-once/`: the producer never retries, so messages may be lost, but should never show up twice.

## The test environment

The test environment consists of:

* Kafka cluster: 3-broker Apache Kafka 4.3.1 cluster running in KRaft mode, with 3x replication and `min.insync.replicas=2`.
* Workload: A workload that continuously produces and consumes messages, and contains assertions that define the desired properties for Antithesis to check.

## Workload

The workload creates a topic, starts a producer and a consumer, and loops forever.

1. The producer sends messages one at a time. Each message has a random body and a checksum of that body.
2. When a send fails, the at-least-once producer retries it (up to 5 times). The at-most-once producer gives up and moves on.
3. The consumer reads every message, checks the checksum, and checks the message against what the producer actually sent.
4. Every few minutes, Antithesis pauses faults for a bit, and the workload checks that the producer and consumer are still making progress and that every acknowledged message was consumed.

The producer and consumer write what they've done to state files on disk, so if the workload gets killed it picks up where it left off.

## Running it

Locally:

```sh
docker compose -f at-least-once/config/docker-compose.yaml up -d --build
docker logs -f at-least-once-workload
```

In Antithesis:

```sh
docker compose -f at-least-once/config/docker-compose.yaml build
snouty validate at-least-once/config
```

Swap in `at-most-once` for the other one. Don't run both at the same time locally, they use the same container names.