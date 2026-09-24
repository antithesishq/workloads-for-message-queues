A message queue is an asynchronous communication abstraction that decouples producers and consumers by buffering messages between them. Producers put data in the queue, which can accumulate, and consumers consume the data from it. Some popular message queue implementations include Kafka, RabbitMQ, Amazon SQS, NATS Jetstream. 

These implementations make different decisions about the tradeoffs related to publishing, message delivery, message persistence, and retention policy. For example, a message queue may choose to be a point-to-point service or use a pub/sub protocol. A queue may guarantee message ordering at different levels (e.g. globally or per-partition). The properties you will want to test in your message queue will depend on what the guarantees your implementation promises to its users.

For this catalog, we’ll discuss some of these variations but assume that the queue generally supports message persistence, and replication for fault-tolerance cluster-based configuration.

This is not an exhaustive list of properties you should be testing your message queue for, there are many other internal and implementation-specific properties that you can and should test for. 

## Property catalog

1. No data corruption (Safety)  
2. Producer progress (Liveness)  
3. Consumer progress (Liveness)  
4. No data loss (Safety)  
5. Publish ordering (Safety)  
6. Message delivery guarantee (Safety)

### No data corruption

Messages are not corrupted or truncated when published or delivered

#### How to test this

* Use one producer and one consumer.
* For each message, generate random data, and compute a checksum of the data.  
* Publish the message.  
* Only after the publish is acknowledged, record the checksum in local memory.  
* Consume messages until the queue is empty or no new messages arrive within a poll timeout.  
* For each consumed message, recompute the checksum and compare it with the recorded checksum.  
* Any checksum mismatch is a data corruption failure.

### Producer progress

The producer should be able to progress and keep publishing data to the queue.

#### How to test this

* Use one producer.  
* Keep attempting to publish in a loop.  
* On every successful acknowledged publish, update a `last_successful_publish` timestamp.  
* If the producer is still trying to publish and the time since the last successful publish exceeds a configurable liveness timeout, fail the test.

### Consumer progress

Messages in the queue waiting to be delivered should be eventually delivered.

#### How to test this 

* Producer publishes N messages  
* Producer monitors state of the queue and tries to keep the number of messages to N  
  * E.g. N=100, after some time there are 90 messages \=\> publish 10 more.  
* Consumer keeps consuming, if too long since the last consumed message, consumer may be stuck

### No data loss

Committed messages are not lost 

#### How to test this

* Use one producer and one consumer.  
* Publish a burst of messages and record each acknowledged message UUID.  
* Consume messages until the queue is empty or no new messages arrive within a poll timeout.  
* Track the set of acknowledged message UUIDs that have been consumed.  
* If any acknowledged message remains unconsumed after the timeout, fail the test.

### No phantom messages

Messages that were never produced cannot be consumed

#### How to test this

* One producer, one consumer.  
* Publish a chunk of messages and record each acknowledged message UUID.  
* Consume messages until queue is empty or timeout.  
* Track the set of acknowledged message UUIDs that are consumed.  
* If a UUID consumed doesn’t exist in the producer’s list of acknowledged messages, fail the test.

### Delivery guarantee

Depending on the queue, the messages in the queue will be delivered either at least once or at most once.

#### At most once

The queue will deliver the message once and not redeliver it. This ensures that the consumer receives the message at most once. In this configuration, a consumer might never get the message.

##### How to test this

* Two containers \-\> 1 producer, 1 consumer (maybe multiple producers)  
* No producer retries  
* Producer loop  
  * Generate a long uuid, random data  
  * Publish to queue  
    * No retry on failure  
* Consumer loop  
  * Init set for consumed messages  
  * Consume message, push uuid to set  
    * If duplicate in set \-\> violation

#### At least once

The queue will deliver the message once and keep retrying until it receives an ack from the consumer. This ensures that the consumer receives the message at least once.

##### How to test this

* Multi container \-\> 1 producer, n consumer groups (one consumer each)  
* Producer retries  
* Producer loop  
  * Generate a random data, header sequence num/counter  
  * Publish to queue  
  * Track the ack’d messages uuid  
* Consumer loop  
  * Track sequence num/counter of consumed messages  
  * Consume message  
  * Message should be either a duplicate or the next sequence number  
  * Mark off uuid from the producer set, no missing uuid 

### Message ordering

The publish order of messages is maintained in the queue. If message A is published strictly before message B, then message A appears in the queue before message B. Depending on the queue, this ordering may be global or per-partition. 

Kafka promises per-partition strict message ordering. So, in the reference workload, we’ll test the message ordering in a single partition.

#### How to test this

* Multi container \-\> 1 producer, n consumer groups (one consumer each)  
* Producer retries  
* Producer loop  
  * Generate a random data, header sequence no/counter  
  * Publish to queue  
* Consumer loop  
  * Track sequence num/counter of consumed messages  
  * Consume message  
  * Message should be either a duplicate or the next sequence number
