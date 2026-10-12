package main

import (
	"context"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"os"
	"os/exec"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	livenessTopic         = "liveness-test"
	livenessConsumerGroup = "liveness-group"
	livenessProbeChance   = 10              // 1 in N iterations triggers a liveness probe
	livenessProbeWindow   = 2 * time.Minute // how long faults stay off per probe
)

func runLiveness(ctx context.Context) {
	createTopic(ctx, livenessTopic, 1, 3, "2")

	producer := newClient("liveness-producer",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(livenessTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	defer producer.Close()

	consumer := newClient("liveness-consumer",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(livenessTopic),
		kgo.ConsumerGroup(livenessConsumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	go livenessProducer(ctx, producer)
	livenessConsumer(ctx, consumer)
}

func livenessProducer(ctx context.Context, client *kgo.Client) {
	var probeDeadline time.Time

	for {
		// Reset probe when window expires
		if !probeDeadline.IsZero() && time.Now().After(probeDeadline) {
			probeDeadline = time.Time{}
			log.Printf("[liveness] producer probe window expired")
		}

		// Roll dice to start a new probe (only if not already probing)
		if probeDeadline.IsZero() && mathrand.IntN(livenessProbeChance) == 0 {
			stopFaults(livenessProbeWindow)
			probeDeadline = time.Now().Add(livenessProbeWindow)
			log.Printf("[liveness] producer probe started")
		}

		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		err := client.ProduceSync(opCtx, &kgo.Record{
			Value: []byte(randomString(dataLength)),
		}).FirstErr()
		cancel()

		if err != nil {
			log.Printf("[liveness] produce error: %v", err)
		} else if !probeDeadline.IsZero() {
			assert.Always(true, "Producer makes progress", nil)
			log.Printf("[liveness] producer probe: ack received")
		}
	}
}

func livenessConsumer(ctx context.Context, client *kgo.Client) {
	var probeDeadline time.Time

	for {
		// Reset probe when window expires
		if !probeDeadline.IsZero() && time.Now().After(probeDeadline) {
			probeDeadline = time.Time{}
			log.Printf("[liveness] consumer probe window expired")
		}

		// Roll dice to start a new probe (only if not already probing)
		if probeDeadline.IsZero() && mathrand.IntN(livenessProbeChance) == 0 {
			stopFaults(livenessProbeWindow)
			probeDeadline = time.Now().Add(livenessProbeWindow)
			log.Printf("[liveness] consumer probe started")
		}

		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.NumRecords() > 0 && !probeDeadline.IsZero() {
			assert.Always(true, "Consumer makes progress", nil)
			log.Printf("[liveness] consumer probe: %d records received", fetches.NumRecords())
		}
	}
}

func stopFaults(duration time.Duration) {
	bin := os.Getenv("ANTITHESIS_STOP_FAULTS")
	if bin == "" {
		log.Printf("[liveness] ANTITHESIS_STOP_FAULTS not set, skipping")
		return
	}
	secs := fmt.Sprintf("%d", int(duration.Seconds()))
	out, err := exec.Command(bin, secs).CombinedOutput()
	if err != nil {
		log.Printf("[liveness] stop faults failed: %v: %s", err, out)
		return
	}
	log.Printf("[liveness] faults stopped for %ss", secs)
}
