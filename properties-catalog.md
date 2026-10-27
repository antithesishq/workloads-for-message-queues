A message queue is an asynchronous communication abstraction that decouples producers and consumers by buffering messages. Producers put data in the queue, which can accumulate, and consumers consume from it. Some popular message queue implementations include Kafka, RabbitMQ, Amazon SQS, and NATS JetStream.

These implementations accept different trade-offs about publishing, message delivery, message persistence, and retention policy. For example, a message queue may choose to be a point-to-point service or use a pub/sub protocol. A queue may guarantee message ordering at different levels (e.g., globally or per-partition). The properties you want to test in your message queue depend on the guarantees your implementation promises to its users.

In this catalog, we’ll discuss some of these variations but assume that the queue generally supports message persistence and replication for fault-tolerant, cluster-based configuration.

This is not an exhaustive list of properties you should be testing your message queue for; there are many other internal and implementation-specific properties that you can and should test for.

Each property below comes with a description of how to test it. Check out the [reference implementation](https://github.com/antithesishq/workloads-for-message-queues) that tests Apache Kafka with Antithesis. It tests at-most-once delivery semantics, but the catalog also explains how to turn the at-most-once client into an at-least-once client.

## Property catalog

1. No data corruption (Safety)  
2. No phantom messages (Safety)  
3. Message ordering (Safety)  
4. No data loss (Safety)  
5. Producer progress (Liveness)  
6. Consumer progress (Liveness)  
7. Message delivery guarantee (Safety)

## The reference workload

All seven properties are tested by one workload, described in full in the [README](https://github.com/antithesishq/workloads-for-message-queues/README.md). The properties share the same setup, so it helps to know it before reading how each one is tested.

* To keep the setup and the workload simple, the producer and consumer run concurrently in a single container against a single-partition topic. Kafka only promises ordering within a partition, and one partition with one producer makes it easy to demonstrate how to check the properties.  
* Every message's UUID is `producer-1-<counter>`, where `producer-1` is the producer ID (if you choose to expand the number of producers, you'll have more than one producer ID), and the `counter` goes up by one for every message. The UUID is the message key. Because the counter is monotonic, the consumer can tell from a UUID alone where the message belongs in the order.  
* The producer maintains a state file to track its own work. Before sending a message, the producer appends `<uuid>, attempt` to its state file. When the broker acks the message, it appends `<uuid>, ack`. This file is the source of truth for what was produced.  
* The producer and consumer report everything they do to the checker. It tracks per-UUID attempt, ack, and consume counts, plus the highest counter consumed so far, and validates most of the properties.  
* Antithesis kills the workload at arbitrary points. On restart, the workload replays the producer state file into the checker, then replays the consumer's own state file (`<uuid>, <offset>` per consumed record) through the same checks a live record goes through. The producer resumes at the next counter and the consumer at the next offset.  
* Every 5 minutes, the workload requests Antithesis to stop injecting faults for 60 seconds, and the liveness properties are checked during that window.

### No data corruption

Messages are not corrupted or truncated when published or delivered.

How to test this

* The producer generates a random-length body (100 to 1099 characters in the reference).  
* The producer computes the SHA-256 of the body and attaches it to the message as a `checksum` header. The body is the message value.  
* For every record it reads, the consumer recomputes the SHA-256 of the value and compares it to the `checksum` header.  
* If there’s a mismatch, it's a data corruption violation.

Since this check is independent of the others, the consumer can check this property without the checker.

### No phantom messages

Messages that were never produced cannot be consumed.

How to test this

* For every consumed record, the checker parses the counter from the UUID and checks that the producer attempted to send that UUID.  
* When checking for phantom messages, we don’t care about acks; we only care about the attempts. A message that timed out or whose ack was lost may still be in the topic, and it's not a phantom.  
* If the checker finds a phantom message, it skips the remaining safety checks because the producer never sent it to check anything else.

In the reference workload, the producer writes the `attempt` line *before* the send, which keeps this check free of false positives. If it sent first and wrote the line afterwards, Antithesis could kill the workload after the broker accepted the message but before the line was written. On restart, the checker is rebuilt from the state file, has no record of that UUID, and flags the message as a phantom when the consumer reads it, even though the producer really sent it.

Writing the attempt first has the opposite failure mode, and it's harmless: a crash after the `attempt` line but before the send leaves a line for a message that never reached the queue. The consumer never sees it, and data loss ignores it because it was never acked.

Writing the attempt first also stops counters from being reused. The producer resumes at the counter after the last one in its state file, so a message that reached the queue without an `attempt` line would have its counter reused by the restarted producer, producing two different messages with the same UUID.

### Message ordering

The publish order of messages is maintained in the queue. If message A is published strictly before message B, then message A appears in the queue before message B. Depending on the queue, this ordering may be global or per-partition.

Kafka promises per-partition strict message ordering. So, in the reference workload, we test the message ordering in a single partition.

How to test this

* The producer sends one message at a time, waiting for each send to succeed or fail before starting the next, and every message gets the next counter. So the order the producer sends in is the counter order.  
* The checker remembers the highest counter the consumer has seen. Each newly consumed counter must be strictly greater than it.  
* A counter equal to the last one also fails. Under at most once, the producer never sends a UUID twice, so seeing the same UUID again is both a duplicate and an ordering violation.  
* Gaps are allowed. A counter can jump ahead because a send failed or because messages were lost; the data loss check decides which of those it was.

### No data loss

Messages the queue acknowledged are not lost.

How to test this

This check is built on top of message ordering. Because we check message ordering counters are consumed in increasing order, every counter between the last consumed one and the current one is a message the consumer has skipped over and will never see.

* When the consumer reads message N, the checker checks every counter from the last consumed counter \+ 1 up to N − 1\.  
* For each counter the consumer didn’t read, the checker looks up whether the producer recorded an ack for it.  
* It’s okay to skip a message that was only attempted. Maybe the request timed out or the producer crashed before the ack came back, and an at-most-once client accepts that it can be lost.  
* But skipping an acked message is data loss.

### Producer progress

The producer should be able to make progress and keep publishing data to the queue.

How to test this

* Every 5 minutes, the workload calls the Antithesis stop-faults API to pause fault injection for 60 seconds.  
* At the start of the window, it records the total number of acks the producer has received.  
* It then waits until the ack count goes up, or until the window ends.  
* The ack count must go up during the window.

We’ve chosen 60 seconds as the window for how long the reference system takes to recover, but you should configure it as per your system.

### Consumer progress

Messages in the queue waiting to be delivered should eventually be delivered.

How to test this

* This runs in the same fault-free windows as producer progress.  
* At the start of the window, the workload records the total number of records the consumer has read.  
* The producer keeps sending messages, so the consumer always has something to read.  
* The read count must go up during the window.

### Message delivery guarantee

Depending on the queue and the client, messages will be delivered either at least once or at most once.

#### At most once

Each message is delivered once or not at all. The consumer never sees a message twice, but it might never see some messages.

##### How to test this

The reference workload tests at-most-once delivery.

* The producer never retries. If a send fails or times out, it gives up on that message and moves on to the next counter. So every UUID is attempted exactly once.  
* The checker counts how many times each UUID has been consumed and checks that it's never more than the number of times it was attempted. With one attempt per UUID, that means every message is consumed at most once.  
* The consumer records each consumed record in its own state file after the checks. If it crashes between the checks and writing the line, the restarted consumer reads that record again from Kafka. The replay didn't count it, so it's checked as a first read, not flagged as a duplicate.  
* Losing a message that failed to send is expected under at-most-once, and the data loss check already allows it. Only losing an *acked* message is a failure.

#### At least once

The client keeps retrying a message until the queue acknowledges it, so every acknowledged message is delivered at least once. The consumer may see some messages more than once.

##### How to test this

The reference workload doesn't implement an at-least-once client. To test at-least-once delivery, start from the at-most-once workload and turn its client into an at-least-once client:

* In the producer, instead of giving up when a send fails, retry it (up to a maximum number of retries) until the broker acks it, and stop as soon as it's acked. Record an `attempt` in the state file before every try, for the same reason the at-most-once producer does, and record the `ack` once a try succeeds.  
* A send can land in the log even though its ack was lost, so the retry attempt(s) can produce duplicates in the queue. Relax the ordering check from "each consumed counter is strictly greater than the previous one" to "greater than or equal to".  
* Keep checking that a UUID is consumed no more times than it was attempted (`consumed <= attempts`), but rename it to the at-least-once guarantee. A message sent once but consumed twice still fails it, but a message sent more than once can be consumed more than once now. Add a reachability assertion for that case so you can confirm duplicates actually happen.  
* Assert that retries happen. Add reachability assertions for "the producer retried a failed send" and "the producer gave up after the maximum number of retries" so you can tell both paths were exercised.