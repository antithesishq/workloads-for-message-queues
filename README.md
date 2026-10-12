# Test message queues
This repository provides a set of property-based tests for kafka using Antithesis. It's a reference implementation of the tests outlined in our [catalog of properties for message queues](https://antithesis.com/resources/mq_property_catalog/), which check for safety and liveness properties most, if not all message queues should exhibit:

* No data corruption (Safety)
* Producer progress (Liveness)
* Consumer progress (Liveness)
* No data loss (Safety)
* Delivery guarantee
    * At least once delivery (Safety)
    * At most once delivery (Safety)
* Message ordering (Safety)

## The test environment
The test environment consists of:

* Kafka Cluster: 3-node cluster running KRaft.
* Workload: A workload that continuously exercises kafka's core queue functions, and contains assertions that define the desired properties for Antithesis to check.



combine 
data corruption
phantom messages
message ordering
data loss

into a single workload.
1 producer -> write state to a file
1 consumer -> consumes messages and reads state from the file

this runs continuosly (not finite number of msgs)

combine liveness into a single workload
1 producer -> writes to queue -> no need to track state
1 consumer -> reads from queue -> no need to track state

1 global process that periodically pauses faults and waits for recovery.

In a fixed duration, the producer should be able to get at least acked write, and the consumer should be able to read at least one message

at least once
at most once