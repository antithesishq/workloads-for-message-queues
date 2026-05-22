# Test message queues
This repository provides a set of property-based tests for kafka using Antithesis. It's a reference implementation of the tests outlined in our catalog of properties for message queues, which check for safety and liveness properties most, if not all message queues should exhibit:

* No data corruption (Safety)
* Producer can make progress (Liveness)
* Consumer can make progress (Liveness)
* No data loss (Safety)
* Delivery guarantee
    * At least once delivery (Safety)
    * At most once delivery (Safety)
* Data ordering (Safety)

## The test environment
The test environment consists of:

* Kafka Cluster: 3-node cluster running KRaft.
* Workload: A workload that continuously exercises kafka's core queue functions, and contains assertions that define the desired properties for Antithesis to check.