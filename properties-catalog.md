A message queue is an asynchronous communication abstraction that decouples producers and consumers by buffering messages between them. Producers put data in the queue, which can accumulate, and consumers consume the data from it. Some popular message queue implementations include Kafka, RabbitMQ, Amazon SQS, NATS Jetstream. 

These implementations make different decisions about the tradeoffs related to publishing, message delivery, message persistence, and retention policy. For example, a message queue may choose to be a point-to-point service or use a pub/sub protocol. A queue may guarantee message ordering at different levels (e.g. globally or per-partition). The properties you will want to test in your message queue will depend on what the guarantees your implementation promises to its users.

For this catalog, we’ll discuss some of these variations but assume that the queue generally supports message persistence, and replication for fault-tolerance cluster-based configuration.

This is not an exhaustive list of properties you should be testing your message queue for, there are many other internal and implementation-specific properties that you can and should test for. 

## Property catalog

1. No data corruption (Safety)  
2. Producer progress (Liveness)  
3. Consumer progress (Liveness)  
4. No data loss (Safety)  
5. No phantom messages (Safety)  
6. Message ordering (Safety)
7. Message delivery guarantee (Safety)

### No data corruption

Messages are not corrupted or truncated when published or delivered

#### How to test this

* Use one producer and one consumer running concurrently in a single container.
* The producer generates a UUID, random data, computes a SHA-256 checksum, and attaches the checksum as a message header.
* The producer publishes the message with the UUID as the key, the random data as the value, and the checksum as a header.
* The consumer recomputes the checksum of each consumed message's value and compares it with the checksum in the header.
* Any checksum mismatch is a data corruption failure.

### Producer progress

The producer should be able to progress and keep publishing data to the queue.

#### How to test this

* Use one producer that continuously publishes messages in a loop.
* On each iteration, roll a dice (1 in N chance) to trigger a liveness check, but only if one isn't already active.
* When triggered, call the Antithesis stop-faults API with a 2-minute duration. Faults resume automatically after the duration.
* During the no-faults window, if a produce gets acknowledged, assert the liveness property. If no produce is ever acknowledged during any no-faults window, the assertion is never triggered, and it's a property failure.
* When the deadline passes, reset and resume normal operation.

### Consumer progress

Messages in the queue waiting to be delivered should be eventually delivered.

#### How to test this 

* Use one consumer that continuously polls for messages in a loop.
* A separate producer thread continuously publishes messages to ensure there is always data available for the consumer.
* On each iteration, roll a dice (1 in N chance) to trigger a liveness check, but only if one isn't already active.
* When triggered, call the Antithesis stop-faults API with a 2-minute duration. Faults resume automatically after the duration.
* During the no-faults window, if a poll returns at least one record, assert the liveness property. If no records are ever consumed during any no-faults window, the assertion is never triggered, and it's a property failure.
* When the deadline passes, reset and resume normal operation.

### No data loss

Committed messages are not lost 

#### How to test this

* Use one producer and one consumer running concurrently in a single container.
* The producer writes each message as `UUID,0` to a shared state file before publishing. After receiving an ack, it flips the `0` to `1` with a single-byte write at a known offset.
* The consumer uses a consumer group with manual offset commits and tracks the last consumed UUID in a persisted state file for crash recovery.
* When the consumer receives a message with UUID N, it reads the state file sequentially from the previous consumed UUID+1 through N. Any UUID in that range marked as acked (`1`) that was not delivered is a data loss.
* Consumer state is persisted (atomic write via temp file + fsync + rename) before committing the offset, so the worst case on crash is a single redelivery.

### No phantom messages

Messages that were never produced cannot be consumed

#### How to test this

* Tested in the same workload as no data loss and message ordering.
* The consumer reads the shared state file to verify each consumed UUID. The file is the source of truth for what was actually produced.
* When the consumer receives UUID N, it reads the state file from prevConsumed+1 through N. If a UUID is not found in the file or doesn’t match the expected sequence, the message is a phantom — it was never produced.
* The state file reader tails the file, waiting for the producer to write more data, with an overall timeout to detect genuine phantoms.

### Delivery guarantee

Depending on the queue, the messages in the queue will be delivered either at least once or at most once.

#### At most once

The queue will deliver the message once and not redeliver it. This ensures that the consumer receives the message at most once. In this configuration, a consumer might never get the message.

##### How to test this

* Use one producer and one consumer running concurrently in a single container.
* The producer does not retry on failure (fire and forget). It records only the UUIDs that were acknowledged by the broker in a shared state file.
* The consumer commits the offset before processing each message. This ensures that after a crash, the consumer resumes from the next message and never reprocesses one.
* The consumer tracks consumed UUIDs in a persistent set. If a UUID appears twice, it is a duplicate delivery — a violation of the at-most-once guarantee.
* **Note:** Because the consumer commits before processing, a crash between committing and recording the UUID means that message is effectively skipped. This means "all acked UUIDs consumed" cannot be guaranteed under fault injection, so we only assert the no-duplicates property.

#### At least once

The queue will deliver the message once and keep retrying until it receives an ack from the consumer. This ensures that the consumer receives the message at least once.

##### How to test this

* Use one producer and one consumer running concurrently in a single container.
* The producer retries each message until it receives an ack. For each UUID, it records the number of send attempts and the number of acks in a shared state file.
* The consumer tracks how many times it has consumed each UUID in its own persistent state.
* On each consume, the consumer checks that its count for that UUID is within bounds: `acks <= consumed <= attempts`. A consumed count below acks means a message was lost; above attempts means the broker fabricated a delivery.

### Message ordering

The publish order of messages is maintained in the queue. If message A is published strictly before message B, then message A appears in the queue before message B. Depending on the queue, this ordering may be global or per-partition. 

Kafka promises per-partition strict message ordering. So, in the reference workload, we’ll test the message ordering in a single partition.

#### How to test this

* Tested in the same workload as no data loss and no phantom messages.
* The producer assigns monotonically increasing UUIDs to each message.
* The consumer tracks the last consumed UUID. Each newly consumed UUID must be strictly greater than the previous one.
* If the consumer receives the exact same UUID as the last consumed (redelivery after a crash), it is safely skipped. Any other out-of-order UUID is an ordering violation.
