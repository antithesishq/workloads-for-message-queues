# Test message queues

This repository provides a set of property-based tests for Apache Kafka using Antithesis. It checks for the properties most, if not all, message queues should exhibit:

* No data corruption (Safety)  
* No phantom messages (Safety)  
* Message ordering (Safety)  
* No data loss (Safety)  
* Producer progress (Liveness)  
* Consumer progress (Liveness)  
* Message delivery guarantee (Safety)

The workload implemented here tests at-most-once delivery semantics. The producer never retries a failed send, so an unacknowledged message may be lost, but no message should ever appear twice. If you want to test at-least-once semantics instead, see [Testing at-least-once delivery](https://file+.vscode-resource.vscode-cdn.net/home/vidhi/src/workloads-for-message-queues/README.md#testing-at-least-once-delivery).

## The test environment

The test environment consists of:

* Kafka cluster: 3-broker Apache Kafka 4.3.1 cluster running in KRaft mode, with 3x replication and `min.insync.replicas=2`.  
* Workload: A workload that continuously produces and consumes messages and contains assertions that define the desired properties to check.

## Workload

The workload creates a topic with one partition, then runs a producer, a consumer, and a checker concurrently in the same container. The producer sends message attempts, and acks updates to the checker, which then validates the reliability properties when the consumer reads the message.

### Producing messages

Each message is identified by a monotonic counter that increments by one per message: its UUID is `producer-1-<counter>`, `producer-1` is the producer ID, and since the reference implementation only has one producer, it's hardcoded. The producer sends one message at a time to partition 0, and each message has:

* Key: the UUID.  
* Value: a random body of 100 to 1099 alphanumeric characters.  
* Header: `checksum`, the SHA-256 of the body.

Before sending the message, the producer writes `<uuid>, attempt` to its state file and tells the checker the message was attempted. When the broker acks the message, the producer writes `<uuid>, ack` and tells the checker the message was acked. If the send fails or times out, the producer gives up on that message and moves on to the next counter. This is what makes it an at-most-once client; a failed message is never resent.

It's important to record the attempt *before* sending the message because the phantom check relies on the producer's state file as the list of everything that could be in Kafka. Imagine the producer sends the message first and writes the `attempt` line afterwards. Antithesis could kill the workload just before the `attempt` or the `ack` line is written. The message is now in Kafka, but the state file has no record of it. When the workload restarts, it rebuilds the checker from the state file, so the checker doesn't know that UUID was ever sent. When the consumer reads that message, the phantom check fails, even though the producer really did send it. It's a false positive.

Writing the attempt first rules this out. Every message that could have reached Kafka has an `attempt` line, fsynced to disk before the send. If the producer crashes after writing the `attempt` line but before sending the message, the state file lists a message that never reached Kafka. The consumer never sees that message, so it can't fail the phantom check. This doesn't trigger a data loss failure either because that check only flags messages that were acked.

This also prevents the counters from being reused. On restart, the producer picks up at the counter after the last one in its state file. If a message could reach Kafka without an `attempt` line, the restarted producer would send a new message with the same UUID. The consumer would then see that UUID twice, which fails both the ordering and delivery guarantee checks.

### Consuming and checking messages

The consumer reads partition 0 directly, from a given offset. For every record it reads, it runs these checks in order.

No data corruption

The consumer recomputes the SHA-256 of the record's value and compares it to the `checksum` header. A mismatch means the body was corrupted somewhere between the producer and the consumer.

The checker validates the remaining properties as they depend on each other.

No phantom messages

The checker parses the counter out of the UUID and checks that the producer attempted that UUID. If the producer never attempted it, the message came from nowhere. A phantom stops the checks there: the remaining checks compare the message to what the producer did, which means nothing for a message the producer never sent.

Message ordering

The checker remembers the highest counter consumed so far. Because one producer is sending one message at a time to one partition, counters should come out of the partition in strictly increasing order. Each consumed counter must be greater than the last one. A counter equal to the last one fails this check too, because it's a duplicate.

No data loss

Ordering is what makes this check possible. Since counters arrive in increasing order, every counter between the last consumed one and the current one is a message the consumer skipped over and will never see. Skipping a message the producer only attempted is fine, because that send failed and at most once allows it to be lost. Skipping a message that was acked is data loss, so the checker looks up each skipped counter and fails if any of them was acked.

Delivery guarantee

The checker counts how many times each UUID was consumed and validates it's never more than the number of times the producer attempted it. The at-most-once producer attempts every UUID exactly once, so this means every message is consumed at most once.

After the checks, the checker records the message as consumed and moves the last consumed counter forward, and the consumer writes `<uuid>, <offset>` to its own state file.

### Fault-free windows

Every 5 minutes, the workload asks Antithesis to stop injecting faults for 60 seconds and records how many acks and reads have happened so far. It then waits until both have gone up, or until the window ends, and checks:

* Producer progress: the producer got at least one ack during the window.  
* Consumer progress: the consumer read at least one message during the window.

### Crash recovery

Antithesis may kill the workload at any point, so both state files are written with an fsync after every line. When the workload container restarts, it drops any partial last line from each file and reads them to build the state in-memory. It records the attempts and acks per UUID from the producer state and goes through each consumed message from the consumer state and runs the same checks as a live message, thus rebuilding the last consumed counter and the consume counts. The producer then resumes at the counter after the last one in its file, and the consumer resumes at the offset after the last one in its file.

## Testing at-least-once delivery

For at-least-once delivery, the producer retries a send, generally until the broker acknowledges it or it reaches a max-retry limit. In this configuration, it's okay for a message to be delivered more than once, but an acknowledged message must never be lost. To add an at-least-once client, start from the at-most-once client and make the following changes:

1. The producer retries sending a message in a loop that runs up to `1 + maxRetries` times. It writes an `attempt`line and calls `checker.attempted` every try before, just like in the at-most-once client. It returns as soon as a send is acked. You can add `assert.Reachable` calls for "producer retried a failed send" and "producer gave up after the maximum number of retries" so you can see both paths get exercised.  
2. A message can be in Kafka even though the ack was lost, so the retry writes the same UUID again at the next offset. When checking message ordering, relax the assertion from `counter > c.lastConsumed` to `counter >= c.lastConsumed`.  
3. Track consumption of duplicates: The `consumed <= attempts` assertion (a message can't be delivered more times than it was sent) still works correctly, so no need to update the logic, but you might want to update the property name. Add an `assert.Reachable` for when `consumed[uuid] > 1` so you know duplicates of retried messages actually happened.

## Running it

```
docker compose -f config/docker-compose.yaml up -d --build

docker logs -f workload
```