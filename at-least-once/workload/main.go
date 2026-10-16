package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic      = "at-least-once"
	producerID = "producer-1"
	maxRetries = 5
	interval = 5 * time.Minute
	faultFreeDuration = 60 * time.Second
)

func main() {

	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ",")
	createTopic(brokers)
	log.Printf("topic %s ready", topic)

	// setup complete
	lifecycle.SetupComplete(map[string]any{"message": "Kafka cluster is up!"})
	log.Print("setup complete")

	stateDir := getenv("STATE_DIR", "/state")

	// Crash recovery - replay the state files, so a restarted workload picks up exactly where it left off
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		log.Fatalf("create state dir: %v", err)
	}
	producerLines, producerLog := openStateFile(stateDir + "/producer-1.log")
	consumerLines, consumerLog := openStateFile(stateDir + "/consumer-1.log")

	c := newChecker()

	// producer log first
	nextCounter := int64(1)
	for _, line := range producerLines {
		uuid, value, _ := strings.Cut(line, ", ")
		counter, err := strconv.ParseInt(strings.TrimPrefix(uuid, producerID+"-"), 10, 64)
		if err != nil {
			log.Fatalf("producer state file: bad line %q", line)
		}
		if value == "attempt" {
			c.attempted(uuid)
		} else {
			c.acked(uuid)
		}
		nextCounter = counter + 1
	}

	// consumer log now
	nextOffset := int64(0)
	for _, line := range consumerLines {
		uuid, value, _ := strings.Cut(line, ", ")
		offset, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Fatalf("consumer state file: bad line %q", line)
		}
		c.checkConsumed(uuid, offset)
		nextOffset = offset + 1
	}

	if len(producerLines) > 0 {
		assert.Reachable("Producer resumed from its state file after a restart", map[string]any{"next_counter": nextCounter})
	}
	if len(consumerLines) > 0 {
		assert.Reachable("Consumer resumed from its state file after a restart", map[string]any{"next_offset": nextOffset})
	}

	log.Printf("starting: next uuid %s-%d, next offset %d", producerID, nextCounter, nextOffset)

	assert.Reachable("Workload started and the Kafka topic is ready", nil)

	p := &producer{
		client:   newProducerClient(brokers),
		stateLog: producerLog,
		checker:  c,
		nextSeq:  nextCounter,
	}
	cons := &consumer{
		client:   newConsumerClient(brokers, nextOffset),
		stateLog: consumerLog,
		checker:  c,
	}

	go p.run()
	go cons.run()
	c.runFaultFreeWindows(interval, faultFreeDuration)
}

func createTopic(brokers []string) {

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatal("failed to dial for topic setup:", err)
	}
	defer client.Close()

	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr("2"),
	}

	_, err = kadm.NewClient(client).CreateTopics(context.Background(), 1, 3, configs, topic)
	if err != nil {
		log.Printf("[createTopic] %v (continuing; topic may already exist)\n", err)
	}
}